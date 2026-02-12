"""
Telegram бот для отслеживания менструального цикла
"""
import json
import logging
import os
from datetime import date, datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    filters
)
from database import (
    User,
    CyclePhase,
    init_db,
    SessionLocal,
    save_cycle_record,
    get_last_cycle_record,
    update_cycle_record_actual_end,
    get_effective_cycle_length,
)
from cycle_calculator import (
    CycleCalculator,
    calculate_menstrual_cycle,
    get_phase_and_stage_for_date,
    get_phase_subphase_starts_on_date,
)
import config
import pytz
import re
import locale

# Устанавливаем русскую локаль для дат
try:
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'Russian_Russia.1251')
    except locale.Error:
        # Если не удалось установить локаль, используем ручной формат
        pass

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
(COLLECTING_NAME, COLLECTING_GIRLFRIEND_NAME, COLLECTING_CYCLE_LENGTH,
 COLLECTING_PERIOD_LENGTH, COLLECTING_LAST_PERIOD, COLLECTING_TIMEZONE,
 COLLECTING_NOTIFICATION_TIME, CHANGING_NOTIFICATION_TIME, UPDATING_NEW_CYCLE_DATE,
 COLLECTING_CYCLE_END_DATE) = range(10)


def get_timezone_offset(user: User) -> int:
    """Получить числовое смещение часового пояса пользователя"""
    if isinstance(user.timezone, int):
        return user.timezone
    elif isinstance(user.timezone, str):
        # Обработка старого формата (строка) - конвертируем в число
        try:
            # Пытаемся распарсить как число
            if user.timezone.startswith('+'):
                return int(user.timezone[1:])
            else:
                return int(user.timezone)
        except (ValueError, AttributeError):
            # Если не удалось распарсить, возвращаем 0 (МСК)
            logger.warning(f"Не удалось распарсить часовой пояс пользователя {user.id}: {user.timezone}, используем 0")
            return 0
    else:
        # Если тип неизвестен, возвращаем 0
        logger.warning(f"Неизвестный тип часового пояса для пользователя {user.id}: {type(user.timezone)}, используем 0")
        return 0


def format_timezone_display(timezone_offset: int) -> str:
    """Форматировать часовой пояс для отображения"""
    return f"+{timezone_offset}" if timezone_offset >= 0 else str(timezone_offset)


# Справочник фаз и подфаз (phase_name на русском, stage для подфаз)
PHASE_REFERENCE = None
# Маппинг рассчитанных фаз (англ.) на phase_name в справочнике (рус.)
PHASE_NAME_TO_REF = {
    "Menstrual Phase": "Менструальная фаза (общая)",
    "Follicular Phase": "Фолликулярная фаза (общая информация)",
    "Ovulation": "Овуляция",
    "Luteal Phase": "Лютеиновая фаза (общая информация)",
}
PHASE_CALLBACK_TO_EN = {
    "menstrual": "Menstrual Phase",
    "follicular": "Follicular Phase",
    "ovulation": "Ovulation",
    "luteal": "Luteal Phase",
}


def _load_phase_reference():
    global PHASE_REFERENCE
    if PHASE_REFERENCE is not None:
        return PHASE_REFERENCE
    path = os.path.join(os.path.dirname(__file__), "data", "phase_reference.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            PHASE_REFERENCE = json.load(f)
    except Exception as e:
        logger.warning(f"Не удалось загрузить справочник фаз: {e}")
        PHASE_REFERENCE = {"phases": []}
    return PHASE_REFERENCE


def get_reference_phase(phase_name_en: str, stage: str = None) -> dict:
    """
    phase_name_en: Menstrual Phase | Follicular Phase | Ovulation | Luteal Phase
    stage: early | mid | late (для подфазы) или None (фаза целиком / Овуляция)
    Возвращает dict с keys: symptoms, behavior, male_recommendations, (subphase_name для подфазы).
    """
    ref = _load_phase_reference()
    ref_name = PHASE_NAME_TO_REF.get(phase_name_en)
    if not ref_name:
        return {}
    for p in ref.get("phases", []):
        if p.get("phase_name") != ref_name:
            continue
        if stage and p.get("subphases"):
            for sub in p["subphases"]:
                if sub.get("stage") == stage:
                    return {
                        "symptoms": sub.get("symptoms", []),
                        "behavior": sub.get("behavior", []),
                        "male_recommendations": sub.get("male_recommendations", []),
                        "subphase_name": sub.get("subphase_name", ""),
                    }
        return {
            "symptoms": p.get("symptoms", []),
            "behavior": p.get("behavior", []),
            "male_recommendations": p.get("male_recommendations", []),
            "phase_name_ru": p.get("phase_name", ""),
        }
    return {}


# Русские названия месяцев
RUSSIAN_MONTHS = {
    1: 'Января', 2: 'Февраля', 3: 'Марта', 4: 'Апреля',
    5: 'Мая', 6: 'Июня', 7: 'Июля', 8: 'Августа',
    9: 'Сентября', 10: 'Октября', 11: 'Ноября', 12: 'Декабря'
}


def format_date_russian(d: date) -> str:
    """Форматировать дату на русском языке (30 Декабря)"""
    return f"{d.day} {RUSSIAN_MONTHS[d.month]}"


ADMIN_USER_ID = 774988626

# Текст кнопок постоянной клавиатуры (горячие клавиши в интерфейсе)
KEYBOARD_MAIN_MENU = "🏠 Главное меню"
KEYBOARD_RESTART = "🔄 Перезапуск"


def effective_cycle_length_for_user(user: User) -> int:
    """Длительность цикла: по среднему из последних 1–3 циклов в БД или user.cycle_length."""
    return get_effective_cycle_length(user.id, user.cycle_length or 28)


def get_persistent_reply_keyboard() -> ReplyKeyboardMarkup:
    """Постоянная клавиатура: Главное меню и Перезапуск (не в сообщении, а в интерфейсе)."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton(KEYBOARD_MAIN_MENU), KeyboardButton(KEYBOARD_RESTART)]],
        resize_keyboard=True,
        is_persistent=True,
    )


def get_main_menu(user: User) -> InlineKeyboardMarkup:
    """Получить главное меню в зависимости от того, заполнены ли данные"""
    if user.last_period_start is None:
        # Первое использование - только кнопка "Приступить к работе"
        keyboard = [
            [InlineKeyboardButton("🚀 Приступить к работе", callback_data="start_data_collection")]
        ]
    else:
        # Порядок: 1. Мой профиль, 2. Настройка уведомлений, 3. Обновить дату / Цикл закончился раньше, 4. Объяснение фаз, 5. Заполнить заново
        keyboard = [
            [InlineKeyboardButton("👤 Мой профиль", callback_data="profile")],
            [InlineKeyboardButton("🔔 Настройка уведомлений", callback_data="notification_settings")],
            [
                InlineKeyboardButton("📆 Обновить дату начала цикла", callback_data="update_cycle_date"),
                InlineKeyboardButton("⏪ Цикл закончился раньше", callback_data="cycle_ended_earlier"),
            ],
            [InlineKeyboardButton("📚 Объяснение фаз цикла", callback_data="cycle_info")],
            [InlineKeyboardButton("🔄 Заполнить данные заново", callback_data="start_data_collection")],
        ]
        if user.id == ADMIN_USER_ID:
            keyboard.append([
                InlineKeyboardButton("🧪 Тест: отчёт по текущей фазе", callback_data="admin_test_daily"),
                InlineKeyboardButton("🧪 Тест: приближение фазы", callback_data="admin_test_phase"),
            ])
            keyboard.append([InlineKeyboardButton("🧪 Тест: завершение цикла", callback_data="admin_test_cycle")])
    return InlineKeyboardMarkup(keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        # Проверяем, есть ли пользователь в базе
        user = session.query(User).filter(User.id == user_id).first()
        
        if user is None:
            # Создаем нового пользователя
            user = User(
                id=user_id,
                username=update.effective_user.username,
                first_name=update.effective_user.first_name,
                last_name=update.effective_user.last_name
            )
            session.add(user)
            session.commit()
        
        # Формируем приветственное сообщение
        welcome_text = (
            "👋 Привет! Добро пожаловать в бот для отслеживания менструального цикла.\n\n"
            "Этот бот создан специально для мужчин, которые хотят лучше понимать и поддерживать "
            "свою девушку в разные периоды её цикла. 💕\n\n"
            "Бот поможет вам:\n"
            "📊 Отслеживать текущую фазу цикла\n"
            "🔔 Получать отчёты при смене фазы и подфазы\n"
            "💡 Получать рекомендации, как лучше поддержать партнершу\n"
            "📚 Изучать информацию о фазах цикла\n\n"
            "Помните: ваша забота и внимание - это проявление любви и уважения! ❤️"
        )
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=get_main_menu(user)
        )
        # Постоянные кнопки в интерфейсе (горячие клавиши), не в теле сообщения
        await update.message.reply_text(
            "💡 Кнопки ниже доступны всегда для быстрого доступа.",
            reply_markup=get_persistent_reply_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка в start: {e}")
        await update.message.reply_text("Произошла ошибка. Попробуйте позже.")
    finally:
        session.close()


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    session = SessionLocal()
    
    try:
        user = session.query(User).filter(User.id == user_id).first()
        
        if query.data == "start_data_collection":
            await start_data_collection(query, user, session)
        elif query.data == "fill_later":
            # Обработка кнопки "Заполнить позже" - не через ConversationHandler
            await query.answer()
            await query.edit_message_text(
                "✅ Отлично, возвращайтесь скорее! 💕\n\n"
                "Когда будете готовы, просто нажмите кнопку '🔄 Заполнить данные заново' в главном меню.",
                reply_markup=get_main_menu(user)
            )
        elif query.data == "cycle_info":
            await show_cycle_info(query)
        elif query.data == "notification_settings":
            await notification_settings(query, user, session)
        elif query.data == "profile":
            await show_profile(query, user)
        elif query.data == "toggle_daily":
            await toggle_daily_notifications(query, user, session)
        elif query.data == "toggle_phase_start":
            await toggle_phase_start_notifications(query, user, session)
        elif query.data == "back_to_main":
            await query.edit_message_text(
                "👋 Привет! Добро пожаловать в бот для отслеживания менструального цикла.\n\n"
            "Этот бот создан специально для мужчин, которые хотят лучше понимать и поддерживать "
            "свою девушку в разные периоды её цикла. 💕\n\n"
            "Бот поможет вам:\n"
            "📊 Отслеживать текущую фазу цикла\n"
            "🔔 Получать отчёты при смене фазы и подфазы\n"
            "💡 Получать рекомендации, как лучше поддержать партнершу\n"
            "📚 Изучать информацию о фазах цикла\n\n"
            "Помните: ваша забота и внимание - это проявление любви и уважения! ❤️",
                reply_markup=get_main_menu(user)
            )
        elif query.data.startswith("phase_info_"):
            phase_name = query.data.replace("phase_info_", "")
            await show_phase_details(query, phase_name, stage=None)
        elif query.data.startswith("phase_subphase_"):
            # phase_subphase_menstrual_early
            rest = query.data.replace("phase_subphase_", "")
            if "_" in rest:
                phase_name, stage = rest.rsplit("_", 1)
                if stage in ("early", "mid", "late"):
                    await show_phase_details(query, phase_name, stage=stage)
                    return
        elif query.data.startswith("term_info_"):
            term = query.data.replace("term_info_", "")
            await show_term_info(query, term)
        elif query.data == "update_cycle_date":
            # Обрабатывается ConversationHandler (start_update_cycle_date_handler)
            pass
        elif query.data == "cycle_ended_earlier":
            # Обрабатывается ConversationHandler (cycle_ended_earlier)
            pass
        elif query.data == "cycle_not_ended_on_time":
            await query.answer()
            user = session.query(User).filter(User.id == user_id).first()
            if not user:
                await query.answer("Ошибка: пользователь не найден.")
                return
            extended = getattr(user, 'cycle_extended_days', 0) or 0
            user.cycle_extended_days = extended + 1
            session.commit()
            await query.message.reply_text(
                "⏳ Цикл продлён на 1 день. Завтра снова придёт напоминание об обновлении даты начала нового цикла."
            )
            return
        elif query.data == "admin_test_daily":
            if query.from_user.id != ADMIN_USER_ID:
                await query.answer("Нет доступа")
                return
            await query.answer()
            user = session.query(User).filter(User.id == query.from_user.id).first()
            if not user or not user.last_period_start:
                await query.message.reply_text("Заполните данные профиля для теста.")
                return
            text = generate_daily_notification(user)
            await query.message.reply_text(text, parse_mode='Markdown')
        elif query.data == "admin_test_phase":
            if query.from_user.id != ADMIN_USER_ID:
                await query.answer("Нет доступа")
                return
            await query.answer()
            user = session.query(User).filter(User.id == query.from_user.id).first()
            if not user or not user.last_period_start:
                await query.message.reply_text("Заполните данные профиля для теста.")
                return
            calculator = CycleCalculator(
                user.last_period_start, effective_cycle_length_for_user(user), user.period_length
            )
            next_phase_info = calculator.get_next_phase()
            if next_phase_info:
                phase = next_phase_info['phase']
                phase_start_date = next_phase_info['start_date']
                recommendations = get_detailed_recommendations(phase.name, False)
                phase_advance_text = (
                    f"🔔 **Приближается новая фаза**\n\n"
                    f"👩 Для: {user.girlfriend_name}\n\n"
                    f"🌙 Через 2 дня начнется фаза: **{phase.name_ru}**\n"
                    f"📅 Дата начала: {format_date_russian(phase_start_date)}\n\n"
                    f"📝 **Что это значит:**\n{phase.description}\n\n"
                    f"{recommendations}"
                )
                await query.message.reply_text(phase_advance_text, parse_mode='Markdown')
            else:
                await query.message.reply_text("Не удалось определить следующую фазу.")
        elif query.data == "admin_test_cycle":
            if query.from_user.id != ADMIN_USER_ID:
                await query.answer("Нет доступа")
                return
            await query.answer()
            user = session.query(User).filter(User.id == query.from_user.id).first()
            if not user or not user.girlfriend_name:
                await query.message.reply_text("Заполните данные профиля для теста.")
                return
            cycle_end_text = (
                f"🔄 **Цикл завершен!**\n\n"
                f"👩 Для: {user.girlfriend_name}\n\n"
                f"📅 Текущий цикл завершился. Необходимо обновить дату начала нового цикла.\n\n"
                f"💡 **Важно:** Обязательно уточните у своей девушки, началась ли у неё новый цикл.\n\n"
                f"Нажмите кнопку ниже, чтобы обновить дату начала нового цикла:"
            )
            keyboard = [
                [InlineKeyboardButton("📆 Обновить дату начала цикла", callback_data="update_cycle_date")],
                [InlineKeyboardButton("⏪ Цикл закончился раньше", callback_data="cycle_ended_earlier")],
                [InlineKeyboardButton("⏳ Цикл не завершился вовремя", callback_data="cycle_not_ended_on_time")],
                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
            ]
            await query.message.reply_text(
                cycle_end_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )

    except Exception as e:
        logger.error(f"Ошибка в button_handler: {e}")
        await query.edit_message_text("Произошла ошибка. Попробуйте позже.")
    finally:
        session.close()


async def start_data_collection(query, user: User, session):
    """Начать процесс сбора данных"""
    text = (
        "📝 Для работы бота необходимо собрать некоторые данные.\n\n"
        "💡 **Важно:** Не бойтесь спрашивать у своей девушки! "
        "Её это только порадует, что вы настолько вовлечены в отношения и заботитесь о ней. "
        "Это показывает вашу зрелость и внимание к её состоянию. ❤️\n\n"
        "📋 **Необходимые данные:**\n\n"
        "1️⃣ Ваше имя\n"
        "2️⃣ Имя вашей девушки\n"
        "3️⃣ Длительность цикла (обычно 21-35 дней, среднее 28)\n"
        "4️⃣ Длительность менструации (обычно 3-7 дней)\n"
        "5️⃣ Дата начала последней менструации (формат: ДД.ММ.ГГГГ)\n"
        "6️⃣ Ваш часовой пояс (например: +3, -1, 0 относительно МСК)\n"
        "7️⃣ Время для уведомлений (формат: ЧЧ:ММ, например 09:00)\n\n"
        "Вы можете заполнить все данные сейчас или взять паузу, чтобы собрать информацию.\n\n"
        "Начнем?"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Начать заполнение", callback_data="start_filling")],
        [InlineKeyboardButton("⏸️ Заполнить позже", callback_data="fill_later")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def start_update_cycle_date(query, user: User, session):
    """Начать процесс обновления даты начала нового цикла"""
    await query.answer()
    text = (
        "📆 **Обновление даты начала нового цикла**\n\n"
        "💡 **ВАЖНО:** Обязательно уточните у своей девушки, началась ли у неё менструация. "
        "Не обновляйте дату, если менструация еще не началась!\n\n"
        "Введите дату начала нового цикла (формат: ДД.ММ.ГГГГ, например: 25.01.2026):"
    )
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return UPDATING_NEW_CYCLE_DATE


async def update_cycle_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновление даты начала нового цикла (новая запись в БД, уведомление об успехе)."""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    # Горячие клавиши: выход в главное меню
    if text in (KEYBOARD_MAIN_MENU, KEYBOARD_RESTART):
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            await update.message.reply_text(
                "👋 Главное меню",
                reply_markup=get_main_menu(user)
            )
        finally:
            session.close()
        return ConversationHandler.END

    session = SessionLocal()
    try:
        # Парсим дату в формате ДД.ММ.ГГГГ
        try:
            new_period_date = datetime.strptime(text, "%d.%m.%Y").date()
        except ValueError:
            await update.message.reply_text(
                "⚠️ Неверный формат даты. Используйте формат ДД.ММ.ГГГГ "
                "(например: 25.01.2026):"
            )
            return UPDATING_NEW_CYCLE_DATE

        # Проверяем, что дата не в будущем
        if new_period_date > date.today():
            await update.message.reply_text(
                "⚠️ Дата не может быть в будущем. Введите корректную дату:"
            )
            return UPDATING_NEW_CYCLE_DATE

        # При обновлении цикла разрешаем дату до 14 дней назад (пользователь мог обновить с задержкой)
        days_diff = (date.today() - new_period_date).days
        if days_diff > 14:
            await update.message.reply_text(
                "⚠️ Дата слишком старая. Укажите дату начала менструации не более чем 14 дней назад. "
                "Проверьте дату и введите корректное значение:"
            )
            return UPDATING_NEW_CYCLE_DATE

        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            await update.message.reply_text("❌ Пользователь не найден. Отправьте /start")
            return ConversationHandler.END
        user.last_period_start = new_period_date
        user.cycle_extended_days = 0  # сброс продления при обновлении даты нового цикла
        session.commit()

        effective_len = effective_cycle_length_for_user(user)
        cycle_data = calculate_menstrual_cycle(
            effective_len, user.period_length, new_period_date
        )
        save_cycle_record(user_id, new_period_date, cycle_data)

        logger.info(f"Пользователь {user_id} обновил дату начала цикла на {new_period_date}")

        await update.message.reply_text(
            f"✅ **Дата начала нового цикла успешно установлена.**\n\n"
            f"📅 Новая дата: {format_date_russian(new_period_date)}\n\n"
            f"Запись в историю циклов добавлена. Бот продолжит отслеживание с новой даты.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]]),
            parse_mode="Markdown"
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Ошибка при обновлении даты цикла: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка. Попробуйте еще раз или отправьте /cancel"
        )
        return UPDATING_NEW_CYCLE_DATE
    finally:
        session.close()


async def start_cycle_ended_earlier(query, user: User, session):
    """Начать процесс «Цикл закончился раньше»: запрос даты окончания текущего цикла."""
    await query.answer()
    text = (
        "⏪ **Цикл закончился раньше**\n\n"
        "Введите дату окончания текущего цикла (формат ДД.ММ.ГГГГ, например: 10.02.2026).\n\n"
        "Эта дата будет записана в текущий цикл в истории."
    )
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return COLLECTING_CYCLE_END_DATE


async def handle_cycle_end_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принять дату окончания цикла, записать в БД, затем запросить дату начала нового цикла."""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if text in (KEYBOARD_MAIN_MENU, KEYBOARD_RESTART):
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            await update.message.reply_text(
                "👋 Главное меню",
                reply_markup=get_main_menu(user)
            )
        finally:
            session.close()
        return ConversationHandler.END

    session = SessionLocal()
    try:
        try:
            end_date = datetime.strptime(text, "%d.%m.%Y").date()
        except ValueError:
            await update.message.reply_text(
                "⚠️ Неверный формат даты. Используйте ДД.ММ.ГГГГ (например: 10.02.2026):"
            )
            return COLLECTING_CYCLE_END_DATE

        if end_date > date.today():
            await update.message.reply_text(
                "⚠️ Дата окончания не может быть в будущем. Введите корректную дату:"
            )
            return COLLECTING_CYCLE_END_DATE

        user = session.query(User).filter(User.id == user_id).first()
        if not user or not user.last_period_start:
            await update.message.reply_text("❌ Сначала заполните данные профиля.")
            return ConversationHandler.END
        if end_date < user.last_period_start:
            await update.message.reply_text(
                "⚠️ Дата окончания цикла не может быть раньше даты начала цикла "
                f"(начало: {format_date_russian(user.last_period_start)}). Введите корректную дату:"
            )
            return COLLECTING_CYCLE_END_DATE

        ok = update_cycle_record_actual_end(user_id, end_date)
        if not ok:
            last_record = get_last_cycle_record(user_id)
            if not last_record:
                await update.message.reply_text(
                    "❌ В истории нет записи текущего цикла. Сначала обновите дату начала цикла через «Обновить дату начала цикла»."
                )
                return ConversationHandler.END
            await update.message.reply_text("❌ Не удалось сохранить дату окончания. Попробуйте позже.")
            return COLLECTING_CYCLE_END_DATE

        await update.message.reply_text(
            f"✅ Дата окончания текущего цикла сохранена: {format_date_russian(end_date)}.\n\n"
            "Теперь введите дату начала нового цикла (формат ДД.ММ.ГГГГ, например: 15.02.2026):"
        )
        return UPDATING_NEW_CYCLE_DATE
    finally:
        session.close()


async def show_main_menu_from_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать главное меню по нажатию постоянной кнопки (Главное меню / Перезапуск)."""
    user_id = update.effective_user.id
    session = SessionLocal()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if user is None:
            user = User(
                id=user_id,
                username=update.effective_user.username,
                first_name=update.effective_user.first_name,
                last_name=update.effective_user.last_name,
            )
            session.add(user)
            session.commit()
        welcome = (
            "👋 Главное меню\n\n"
            "Бот поможет вам отслеживать фазы цикла и поддерживать партнёршу. "
            "Выберите действие ниже."
        )
        await update.message.reply_text(
            welcome,
            reply_markup=get_main_menu(user)
        )
    finally:
        session.close()


async def begin_filling(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начать заполнение данных - обработчик для ConversationHandler"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    session = SessionLocal()
    
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            await query.message.reply_text("❌ Ошибка: пользователь не найден. Отправьте /start")
            return ConversationHandler.END
        
        user.data_collection_state = "name"
        session.commit()
        
        logger.info(f"Начало сбора данных для пользователя {user_id}")
        
        # Отправляем новое сообщение вместо редактирования, чтобы ConversationHandler правильно работал
        await query.message.reply_text(
            "📝 Отлично! Начнем заполнение данных.\n\n"
            "Напишите ваше имя:"
        )
        return COLLECTING_NAME
    except Exception as e:
        logger.error(f"Ошибка в begin_filling: {e}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте позже.")
        return ConversationHandler.END
    finally:
        session.close()


async def fill_later_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Заполнить позже'"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    session = SessionLocal()
    
    try:
        user = session.query(User).filter(User.id == user_id).first()
        await query.edit_message_text(
            "✅ Отлично, возвращайтесь скорее! 💕\n\n"
            "Когда будете готовы, просто нажмите кнопку '🔄 Заполнить данные заново' в главном меню.",
            reply_markup=get_main_menu(user)
        )
        return ConversationHandler.END
    finally:
        session.close()


async def collect_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор имени пользователя"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        logger.info(f"collect_name вызван для пользователя {user_id}, текст: {update.message.text}")
        
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            logger.error(f"Пользователь {user_id} не найден в базе")
            await update.message.reply_text("❌ Ошибка: пользователь не найден. Отправьте /start")
            return ConversationHandler.END
        
        name = update.message.text.strip()
        if not name:
            await update.message.reply_text("⚠️ Пожалуйста, введите ваше имя:")
            return COLLECTING_NAME
        
        # Проверка длины имени
        if len(name) > 50:
            await update.message.reply_text("⚠️ Имя слишком длинное. Пожалуйста, введите имя короче (максимум 50 символов):")
            return COLLECTING_NAME
        
        # Проверка на допустимые символы (буквы, пробелы, дефисы)
        if not re.match(r'^[а-яА-ЯёЁa-zA-Z\s\-]+$', name):
            await update.message.reply_text("⚠️ Имя может содержать только буквы, пробелы и дефисы. Пожалуйста, введите корректное имя:")
            return COLLECTING_NAME
        
        user.name = name
        user.data_collection_state = "girlfriend_name"
        session.commit()
        
        logger.info(f"Пользователь {user_id} ввел имя: {name}")
        
        await update.message.reply_text(
            f"✅ Отлично, {user.name}! Теперь напишите имя вашей девушки: 👩"
        )
        return COLLECTING_GIRLFRIEND_NAME
    except Exception as e:
        logger.error(f"Ошибка в collect_name для пользователя {user_id}: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте еще раз или отправьте /cancel")
        return COLLECTING_NAME
    finally:
        session.close()


async def collect_girlfriend_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор имени девушки"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        girlfriend_name = update.message.text.strip()
        
        if not girlfriend_name:
            await update.message.reply_text("⚠️ Пожалуйста, введите имя вашей девушки:")
            return COLLECTING_GIRLFRIEND_NAME
        
        # Проверка длины имени
        if len(girlfriend_name) > 50:
            await update.message.reply_text("⚠️ Имя слишком длинное. Пожалуйста, введите имя короче (максимум 50 символов):")
            return COLLECTING_GIRLFRIEND_NAME
        
        # Проверка на допустимые символы
        if not re.match(r'^[а-яА-ЯёЁa-zA-Z\s\-]+$', girlfriend_name):
            await update.message.reply_text("⚠️ Имя может содержать только буквы, пробелы и дефисы. Пожалуйста, введите корректное имя:")
            return COLLECTING_GIRLFRIEND_NAME
        
        user = session.query(User).filter(User.id == user_id).first()
        user.girlfriend_name = girlfriend_name
        user.data_collection_state = "cycle_length"
        session.commit()
        
        await update.message.reply_text(
            f"💕 Прекрасно! Теперь укажите длительность цикла в днях "
            f"(обычно 21-35 дней, среднее значение 28): 📅"
        )
        return COLLECTING_CYCLE_LENGTH
    finally:
        session.close()


async def collect_cycle_length(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор длительности цикла"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        cycle_length_str = update.message.text.strip()
        
        # Проверка, что это число
        try:
            cycle_length = int(cycle_length_str)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Пожалуйста, введите целое число (например: 28):"
            )
            return COLLECTING_CYCLE_LENGTH
        
        if cycle_length < 21 or cycle_length > 35:
            await update.message.reply_text(
                "⚠️ Длительность цикла обычно составляет 21-35 дней. "
                "Пожалуйста, введите корректное значение:"
            )
            return COLLECTING_CYCLE_LENGTH
        
        user = session.query(User).filter(User.id == user_id).first()
        user.cycle_length = cycle_length
        user.data_collection_state = "period_length"
        session.commit()
        
        await update.message.reply_text(
            "✅ Принято! Теперь укажите длительность менструации в днях "
            "(обычно 3-7 дней): 🩸"
        )
        return COLLECTING_PERIOD_LENGTH
    except ValueError:
        await update.message.reply_text(
            "⚠️ Пожалуйста, введите число (например: 28):"
        )
        return COLLECTING_CYCLE_LENGTH
    finally:
        session.close()


async def collect_period_length(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор длительности менструации"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        period_length_str = update.message.text.strip()
        
        # Проверка, что это число
        try:
            period_length = int(period_length_str)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Пожалуйста, введите целое число (например: 5):"
            )
            return COLLECTING_PERIOD_LENGTH
        
        if period_length < 1 or period_length > 10:
            await update.message.reply_text(
                "⚠️ Длительность менструации обычно составляет 3-7 дней. "
                "Пожалуйста, введите корректное значение:"
            )
            return COLLECTING_PERIOD_LENGTH
        
        user = session.query(User).filter(User.id == user_id).first()
        user.period_length = period_length
        user.data_collection_state = "last_period"
        session.commit()
        
        await update.message.reply_text(
            "✅ Отлично! Теперь укажите дату начала последней менструации "
            "(формат: ДД.ММ.ГГГГ, например: 15.01.2026): 📆"
        )
        return COLLECTING_LAST_PERIOD
    except ValueError:
        await update.message.reply_text(
            "⚠️ Пожалуйста, введите число (например: 5):"
        )
        return COLLECTING_PERIOD_LENGTH
    finally:
        session.close()


async def collect_last_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор даты последней менструации"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        date_str = update.message.text.strip()
        # Парсим дату в формате ДД.ММ.ГГГГ
        try:
            period_date = datetime.strptime(date_str, "%d.%m.%Y").date()
        except ValueError:
            await update.message.reply_text(
                "⚠️ Неверный формат даты. Используйте формат ДД.ММ.ГГГГ "
                "(например: 15.01.2026):"
            )
            return COLLECTING_LAST_PERIOD
        
        # Проверяем, что дата не в будущем
        if period_date > date.today():
            await update.message.reply_text(
                "⚠️ Дата не может быть в будущем. Введите корректную дату:"
            )
            return COLLECTING_LAST_PERIOD
        
        user = session.query(User).filter(User.id == user_id).first()
        user.last_period_start = period_date
        user.data_collection_state = "timezone"
        session.commit()
        
        await update.message.reply_text(
            "✅ Отлично! Теперь укажите ваш часовой пояс относительно МСК "
            "(например: +3, -1, 0). "
            "Положительное число - восточнее МСК, отрицательное - западнее: 🌍"
        )
        return COLLECTING_TIMEZONE
    except Exception as e:
        logger.error(f"Ошибка при сборе даты: {e}")
        await update.message.reply_text(
            "⚠️ Произошла ошибка. Попробуйте ввести дату еще раз:"
        )
        return COLLECTING_LAST_PERIOD
    finally:
        session.close()


async def collect_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор часового пояса"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        timezone_str = update.message.text.strip()
        
        # Парсим часовой пояс (формат: +3, -1, 0)
        try:
            # Убираем знак + если есть
            if timezone_str.startswith('+'):
                timezone_str = timezone_str[1:]
            timezone_offset = int(timezone_str)
            
            # Проверяем диапазон (обычно от -12 до +14)
            if timezone_offset < -12 or timezone_offset > 14:
                await update.message.reply_text(
                    "⚠️ Часовой пояс должен быть в диапазоне от -12 до +14. "
                    "Введите корректное значение (например: +3, -1, 0):"
                )
                return COLLECTING_TIMEZONE
        except ValueError:
            await update.message.reply_text(
                "⚠️ Неверный формат. Используйте формат числа относительно МСК "
                "(например: +3, -1, 0):"
            )
            return COLLECTING_TIMEZONE
        
        user = session.query(User).filter(User.id == user_id).first()
        # Сохраняем как число (для совместимости с новым форматом)
        user.timezone = timezone_offset
        user.data_collection_state = "notification_time"
        session.commit()
        
        # Логируем для отладки
        logger.info(f"Пользователь {user_id} установил часовой пояс: {timezone_offset}")
        
        timezone_display = f"+{timezone_offset}" if timezone_offset >= 0 else str(timezone_offset)
        await update.message.reply_text(
            f"✅ Отлично! Часовой пояс установлен: {timezone_display} относительно МСК.\n\n"
            f"Укажите время, в которое присылать отчёты (формат: ЧЧ:ММ, например: 09:00). "
            f"Отчёты приходят только в дни начала фазы или подфазы.\n\n"
            f"⏰ **Важно:** время указывается в ВАШЕМ часовом поясе!"
        )
        return COLLECTING_NOTIFICATION_TIME
    finally:
        session.close()


async def collect_notification_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбор времени уведомлений"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        time_str = update.message.text.strip()
        
        # Проверяем формат времени
        if not re.match(r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$', time_str):
            await update.message.reply_text(
                "⚠️ Неверный формат времени. Используйте формат ЧЧ:ММ "
                "(например: 09:00 или 21:30):"
            )
            return COLLECTING_NOTIFICATION_TIME
        
        user = session.query(User).filter(User.id == user_id).first()
        user.notification_time = time_str
        user.data_collection_state = None
        user.notifications_enabled = True
        session.commit()
        
        effective_len = effective_cycle_length_for_user(user)
        cycle_data = calculate_menstrual_cycle(
            effective_len, user.period_length, user.last_period_start
        )
        save_cycle_record(user_id, user.last_period_start, cycle_data)
        
        # Формируем финальное сообщение
        calculator = CycleCalculator(
            user.last_period_start,
            effective_len,
            user.period_length
        )
        phase_info = calculator.get_current_phase()
        
        text = (
            f"🎉 Отлично, {user.name}! Все данные собраны и бот настроен!\n\n"
            f"📊 **Текущая информация:**\n"
            f"👩 Девушка: {user.girlfriend_name}\n"
            f"📅 Длительность цикла: {effective_len} дней\n"
            f"🩸 Длительность менструации: {user.period_length} дней\n"
            f"📆 Последняя менструация: {format_date_russian(user.last_period_start)}\n\n"
        )
        
        timezone_offset = get_timezone_offset(user)
        timezone_display = format_timezone_display(timezone_offset)
        text += (
            f"🔔 Отчёты при смене фазы или подфазы будут приходить в {user.notification_time} "
            f"(часовой пояс: {timezone_display} относительно МСК).\n\n"
            f"⚙️ Настройки уведомлений — в главном меню.\n\n"
            f"💡 **Совет:** Обновляйте дату начала менструации, когда начинается новый цикл!"
        )
        
        keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]]
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return ConversationHandler.END
    finally:
        session.close()


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена сбора данных"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if user:
            user.data_collection_state = None
            session.commit()
        
        await update.message.reply_text(
            "❌ Сбор данных отменен. Вы можете начать заново из главного меню.",
            reply_markup=get_main_menu(user) if user else None
        )
        return ConversationHandler.END
    finally:
        session.close()


async def show_cycle_info(query):
    """Показать информацию о фазах цикла"""
    text = (
        "📚 **Справочник фаз менструального цикла**\n\n"
        "Здесь вы можете узнать подробную информацию о каждой фазе цикла, "
        "симптомах, поведении и рекомендациях по поддержке вашей девушки.\n\n"
        "Выберите, что вас интересует:"
    )
    
    keyboard = [
        [InlineKeyboardButton("🩸 Менструальная фаза", callback_data="phase_info_menstrual")],
        [InlineKeyboardButton("🌱 Фолликулярная фаза", callback_data="phase_info_follicular")],
        [InlineKeyboardButton("💫 Овуляция", callback_data="phase_info_ovulation")],
        [InlineKeyboardButton("🌙 Лютеиновая фаза (ПМС)", callback_data="phase_info_luteal")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


def _format_ref_block(items: list) -> str:
    if not items:
        return ""
    return "\n".join(f"• {s}" for s in items) if isinstance(items[0], str) else "\n".join(items)


async def show_phase_details(query, phase_name: str, stage: str = None):
    """Показать детали фазы или подфазы из справочника (phase_name: menstrual|follicular|ovulation|luteal, stage: early|mid|late или None)."""
    phase_en = PHASE_CALLBACK_TO_EN.get(phase_name)
    if not phase_en:
        await query.answer("Фаза не найдена")
        return
    ref = get_reference_phase(phase_en, stage)
    if not ref:
        await query.answer("Данные не найдены")
        return
    symptoms = ref.get("symptoms", [])
    behavior = ref.get("behavior", [])
    recs = ref.get("male_recommendations", [])
    title = ref.get("subphase_name") or ref.get("phase_name_ru") or phase_en
    text = (
        f"📊 **{title}**\n\n"
        f"😷 **Симптомы:**\n{_format_ref_block(symptoms)}\n\n"
        f"👤 **Поведение:**\n{_format_ref_block(behavior)}\n\n"
        f"💡 **Рекомендации для вас:**\n\n{_format_ref_block(recs)}"
    )
    keyboard = []
    if stage:
        keyboard.append([InlineKeyboardButton("🔙 Назад к фазе", callback_data=f"phase_info_{phase_name}")])
    else:
        # Кнопки подфаз только для фаз с подфазами (не Овуляция)
        if phase_name in ("menstrual", "follicular", "luteal"):
            keyboard.append([
                InlineKeyboardButton("Начало", callback_data=f"phase_subphase_{phase_name}_early"),
                InlineKeyboardButton("Середина", callback_data=f"phase_subphase_{phase_name}_mid"),
                InlineKeyboardButton("Конец", callback_data=f"phase_subphase_{phase_name}_late"),
            ])
    keyboard.append([InlineKeyboardButton("🔙 Назад к фазам", callback_data="cycle_info")])
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


async def show_terms_list(query):
    """Показать список терминов"""
    text = (
        "📖 **Ключевые термины**\n\n"
        "Выберите термин для получения подробной информации:"
    )
    
    keyboard = [
        [InlineKeyboardButton("🩸 Менструация", callback_data="term_info_menstruation")],
        [InlineKeyboardButton("💫 Овуляция", callback_data="term_info_ovulation")],
        [InlineKeyboardButton("🌙 ПМС", callback_data="term_info_pms")],
        [InlineKeyboardButton("📅 Цикл", callback_data="term_info_cycle")],
        [InlineKeyboardButton("🔙 Назад", callback_data="cycle_info")]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


async def show_term_info(query, term: str):
    """Показать информацию о термине"""
    terms = {
        "menstruation": (
            "🩸 **Менструация**\n\n"
            "Менструация - это ежемесячное кровотечение, которое происходит, "
            "когда организм избавляется от неоплодотворенной яйцеклетки и эндометрия "
            "(слизистой оболочки матки). Обычно длится 3-7 дней.\n\n"
            "В этот период женщина может испытывать слабость, боли, усталость."
        ),
        "ovulation": (
            "💫 **Овуляция**\n\n"
            "Овуляция - это процесс выхода зрелой яйцеклетки из фолликула яичника. "
            "Обычно происходит на 14 день цикла (при 28-дневном цикле). "
            "Это период максимальной фертильности.\n\n"
            "Во время овуляции женщина чувствует прилив сил, повышение либидо, "
            "уверенность в себе."
        ),
        "pms": (
            "🌙 **ПМС (Предменструальный синдром)**\n\n"
            "ПМС - это комплекс симптомов, которые возникают за несколько дней "
            "до начала менструации (обычно за 1-2 недели).\n\n"
            "Симптомы включают:\n"
            "• Перепады настроения\n"
            "• Раздражительность\n"
            "• Усталость\n"
            "• Отеки\n"
            "• Изменения аппетита\n"
            "• Вздутие живота\n\n"
            "Это нормальная часть цикла, требующая понимания и поддержки."
        ),
        "cycle": (
            "📅 **Менструальный цикл**\n\n"
            "Менструальный цикл - это регулярные изменения в организме женщины, "
            "подготовка к возможной беременности. Обычно длится 21-35 дней "
            "(в среднем 28 дней).\n\n"
            "Цикл состоит из четырех фаз:\n"
            "1. Менструальная (дни 1-7)\n"
            "2. Фолликулярная (дни 7-14)\n"
            "3. Овуляция (день 14)\n"
            "4. Лютеиновая (дни 15-28)\n\n"
            "Каждая фаза имеет свои особенности и требует разного подхода."
        )
    }
    
    text = terms.get(term, "Термин не найден")
    keyboard = [[InlineKeyboardButton("🔙 Назад к терминам", callback_data="terms_list")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


def _days_in_phase_from_cycle_data(cycle_data: dict, target_date) -> tuple:
    """По cycle_data и дате вернуть (days_in_phase, days_left_in_phase) для текущей фазы/подфазы."""
    if hasattr(target_date, 'strftime'):
        d_str = target_date.strftime("%Y-%m-%d")
    else:
        d_str = str(target_date)
    for ph in cycle_data.get("phases", []):
        if "subphases" in ph:
            for sub in ph["subphases"]:
                start_s, end_s = sub["start_date"], sub["end_date"]
                if start_s <= d_str <= end_s:
                    start_d = datetime.strptime(start_s, "%Y-%m-%d").date()
                    end_d = datetime.strptime(end_s, "%Y-%m-%d").date()
                    today = target_date if isinstance(target_date, date) else datetime.strptime(d_str, "%Y-%m-%d").date()
                    days_in = (today - start_d).days + 1
                    days_left = (end_d - today).days
                    return (days_in, days_left)
        else:
            start_s, end_s = ph["start_date"], ph["end_date"]
            if start_s <= d_str <= end_s:
                start_d = datetime.strptime(start_s, "%Y-%m-%d").date()
                end_d = datetime.strptime(end_s, "%Y-%m-%d").date()
                today = target_date if isinstance(target_date, date) else datetime.strptime(d_str, "%Y-%m-%d").date()
                days_in = (today - start_d).days + 1
                days_left = (end_d - today).days
                return (days_in, days_left)
    return (None, None)


async def show_profile(query, user: User):
    """Показать профиль пользователя (фаза и овуляции — по тем же расчётам, что и в ежедневном отчёте)."""
    effective_len = effective_cycle_length_for_user(user)
    calculator = CycleCalculator(
        user.last_period_start,
        effective_len,
        user.period_length
    )
    cycle_data = calculate_menstrual_cycle(
        effective_len, user.period_length, user.last_period_start
    )
    phase_name_en, stage = get_phase_and_stage_for_date(cycle_data, date.today())
    ref = get_reference_phase(phase_name_en, stage) if phase_name_en else {}
    phase_title = ref.get("subphase_name") or ref.get("phase_name_ru") if ref else None
    
    phase_info = calculator.get_current_phase()
    current_day = phase_info["current_day"]
    if not phase_title and phase_info.get("phase"):
        phase_title = phase_info["phase"].name_ru
    
    days_in_phase, days_left_in_phase = _days_in_phase_from_cycle_data(cycle_data, date.today())
    
    next_period = calculator.get_next_period_date()
    last_ovulation = calculator.get_last_ovulation_date()
    next_ovulation = calculator.get_next_ovulation_date()
    days_until_period = (next_period - date.today()).days
    days_until_ovulation = (next_ovulation - date.today()).days
    
    timezone_offset = get_timezone_offset(user)
    timezone_display = format_timezone_display(timezone_offset)
    
    phase_line = f"🌙 Фаза: {phase_title or '—'}"
    if days_in_phase is not None and days_left_in_phase is not None:
        phase_line += f" — день {days_in_phase}, осталось {days_left_in_phase} дней"
    phase_line += "\n"
    
    text = (
        f"👤 **Мой профиль**\n\n"
        f"👨 Имя: {user.name or 'Не указано'}\n"
        f"👩 Имя девушки: {user.girlfriend_name or 'Не указано'}\n\n"
        f"📊 **Данные цикла:**\n\n"
        f"📅 Длительность цикла: {effective_len} дней\n"
        f"🩸 Длительность менструации: {user.period_length} дней\n"
        f"📆 Последняя менструация: {format_date_russian(user.last_period_start) if user.last_period_start else 'Не указано'}\n\n"
        f"📈 **Текущее состояние:**\n\n"
        f"📅 Текущий день: {current_day} из {effective_len}\n"
        f"{phase_line}"
        f"💫 Овуляция была: {format_date_russian(last_ovulation)}\n"
        f"💫 Следующая овуляция: {format_date_russian(next_ovulation)} (через {days_until_ovulation} {'день' if days_until_ovulation == 1 else 'дня' if days_until_ovulation < 5 else 'дней'})\n"
        f"🩸 Следующая менструация: {format_date_russian(next_period)} (через {days_until_period} {'день' if days_until_period == 1 else 'дня' if days_until_period < 5 else 'дней'})\n\n"
        f"🔔 **Уведомления:**\n\n"
        f"Статус: {'✅ Включены' if user.notifications_enabled else '❌ Выключены'}\n"
        f"⏰ Время: {user.notification_time}\n"
        f"🌍 Часовой пояс: {timezone_display} относительно МСК\n\n"
        f"📊 Дней с нами в режиме отслеживания: {user.days_with_notifications}"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )




async def toggle_daily_notifications(query, user: User, session):
    """Переключить отчёты при начале фазы/подфазы"""
    user.notify_daily = not user.notify_daily
    session.commit()
    
    status = "✅ включены" if user.notify_daily else "❌ выключены"
    await query.answer(f"Отчёты при начале фазы/подфазы {status}")
    
    # Обновляем меню настроек
    await notification_settings(query, user, session)


async def toggle_phase_start_notifications(query, user: User, session):
    """Переключить напоминание за 2 дня до новой фазы"""
    user.notify_phase_start = not user.notify_phase_start
    session.commit()
    
    status = "✅ включено" if user.notify_phase_start else "❌ выключено"
    await query.answer(f"Напоминание за 2 дня до фазы {status}")
    
    # Обновляем меню настроек
    await notification_settings(query, user, session)


async def start_change_notification_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начать процесс изменения времени уведомлений"""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "⏰ Укажите новое время отправки отчётов "
        "(формат: ЧЧ:ММ, например: 09:00 или 21:30):"
    )
    return CHANGING_NOTIFICATION_TIME


async def change_notification_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Изменение времени уведомлений"""
    user_id = update.effective_user.id
    session = SessionLocal()
    
    try:
        time_str = update.message.text.strip()
        
        # Проверяем формат времени
        if not re.match(r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$', time_str):
            await update.message.reply_text(
                "⚠️ Неверный формат времени. Используйте формат ЧЧ:ММ "
                "(например: 09:00 или 21:30):"
            )
            return CHANGING_NOTIFICATION_TIME
        
        user = session.query(User).filter(User.id == user_id).first()
        user.notification_time = time_str
        session.commit()
        
        logger.info(f"Пользователь {user_id} изменил время уведомлений на {time_str}")
        
        user_id = update.effective_user.id
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            await update.message.reply_text(
                f"✅ Время отправки изменено на {time_str}!\n\n"
                f"Отчёты при начале фазы или подфазы будут приходить в это время.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в настройки", callback_data="notification_settings")]])
            )
        finally:
            session.close()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Ошибка при изменении времени уведомлений: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка. Попробуйте еще раз или отправьте /cancel"
        )
        return CHANGING_NOTIFICATION_TIME
    finally:
        session.close()


async def notification_settings(query, user: User, session):
    """Настройки уведомлений"""
    timezone_offset = get_timezone_offset(user)
    timezone_display = format_timezone_display(timezone_offset)
    
    text = (
        f"🔔 **Настройка уведомлений**\n\n"
        f"⏰ Время отправки: {user.notification_time}\n"
        f"🌍 Часовой пояс: {timezone_display} относительно МСК\n\n"
        f"Отчёты приходят только в дни смены фазы или подфазы (не каждый день).\n\n"
        f"📅 Отчёты при начале фазы/подфазы: {'✅ Включены' if user.notify_daily else '❌ Выключены'}\n"
        f"🔔 Напоминание за 2 дня до новой фазы (в 15:00): {'✅ Включено' if user.notify_phase_start else '❌ Выключено'}\n\n"
        f"💡 Используйте кнопки ниже для изменения настроек:"
    )
    
    keyboard = [
        [
            InlineKeyboardButton(
                f"{'✅' if user.notify_daily else '❌'} Отчёты при начале фазы/подфазы",
                callback_data="toggle_daily"
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅' if user.notify_phase_start else '❌'} Напоминание за 2 дня до фазы",
                callback_data="toggle_phase_start"
            )
        ],
        [InlineKeyboardButton("⏰ Изменить время отправки", callback_data="change_notification_time")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


def get_detailed_recommendations(phase_name: str, is_pms: bool) -> str:
    """Получить подробные рекомендации для мужчин в зависимости от фазы"""
    recommendations = {
        'menstrual': (
            "💡 **Что делать вам как мужчине:**\n\n"
            "• **Будьте терпеливы и понимающими** - сейчас ей особенно нужна ваша поддержка\n"
            "• **Предложите помощь по дому** - возьмите на себя больше обязанностей, не ждите просьб\n"
            "• **Создайте комфортную атмосферу** - приготовьте горячий чай, включите любимый фильм\n"
            "• **Не планируйте активные мероприятия** - лучше провести время дома в спокойной обстановке\n"
            "• **Будьте внимательны к её потребностям** - спросите, что ей нужно, и сделайте это\n"
            "• **Избегайте конфликтов** - сейчас не время для серьезных разговоров или споров\n"
            "• **Проявите заботу** - купите её любимые продукты, сделайте массаж, просто будьте рядом"
        ),
        'follicular': (
            "💡 **Что делать вам как мужчине:**\n\n"
            "• **Планируйте совместные активности** - сейчас отличное время для новых впечатлений\n"
            "• **Поддержите её инициативу** - она полна энергии, помогите реализовать её идеи\n"
            "• **Организуйте романтическое свидание** - она чувствует себя привлекательной и уверенной\n"
            "• **Обсуждайте планы на будущее** - это идеальное время для важных решений\n"
            "• **Будьте активными вместе** - займитесь спортом, прогуляйтесь, сходите в новое место\n"
            "• **Цените её хорошее настроение** - наслаждайтесь этим периодом вместе"
        ),
        'ovulation': (
            "💡 **Что делать вам как мужчине:**\n\n"
            "• **Это идеальное время для романтики** - она чувствует себя особенно привлекательной\n"
            "• **Планируйте интимную близость** - её либидо на пике, это время максимальной близости\n"
            "• **Делайте комплименты** - она особенно чувствительна к вниманию и восхищению\n"
            "• **Организуйте особенное свидание** - ужин при свечах, романтическая прогулка\n"
            "• **Будьте инициативными** - она в настроении для активного общения и близости\n"
            "• **Наслаждайтесь этим временем вместе** - это период максимальной гармонии в отношениях"
        ),
        'luteal': (
            "💡 **Что делать вам как мужчине:**\n\n"
            "• **Максимальная поддержка и терпение** - сейчас ей особенно нужна ваша забота\n"
            "• **Помогайте больше, требуйте меньше** - возьмите на себя больше домашних дел\n"
            "• **Избегайте конфликтов любой ценой** - не спорьте, даже если она не права\n"
            "• **Будьте понимающими** - её эмоции могут быть нестабильными, это нормально\n"
            "• **Создайте спокойную атмосферу** - минимизируйте стресс, будьте предсказуемы\n"
            "• **Проявите заботу** - купите её любимую еду, сделайте что-то приятное без повода\n"
            "• **Слушайте и поддерживайте** - иногда ей просто нужно выговориться\n"
            "• **Не принимайте всё на свой счет** - её раздражительность связана с гормонами, а не с вами"
        )
    }
    
    if is_pms:
        return recommendations.get('luteal', recommendations['luteal'])
    else:
        return recommendations.get(phase_name, recommendations['menstrual'])


def generate_daily_notification(user: User) -> str:
    """Генерация текста ежедневного уведомления по справочнику (phase_name + stage)."""
    effective_len = effective_cycle_length_for_user(user)
    calculator = CycleCalculator(
        user.last_period_start,
        effective_len,
        user.period_length
    )
    cycle_data = calculate_menstrual_cycle(
        effective_len, user.period_length, user.last_period_start
    )
    phase_name_en, stage = get_phase_and_stage_for_date(cycle_data, date.today())
    ref = get_reference_phase(phase_name_en, stage) if phase_name_en else {}
    
    phase_info = calculator.get_current_phase()
    current_day = phase_info['current_day']
    phase = phase_info['phase']
    days_left = phase_info['days_left_in_phase']
    is_pms = phase_info['is_pms']
    
    next_period = calculator.get_next_period_date()
    last_ovulation = calculator.get_last_ovulation_date()
    next_ovulation = calculator.get_next_ovulation_date()
    days_until_period = (next_period - date.today()).days
    days_until_ovulation = (next_ovulation - date.today()).days
    
    phase_title = (
        ref.get("subphase_name") or ref.get("phase_name_ru")
        if ref else phase.name_ru if phase else "—"
    )
    if not phase_title and phase:
        phase_title = phase.name_ru
    
    text = (
        f"📊 **Ежедневный отчет**\n\n"
        f"👩 Для: {user.girlfriend_name}\n"
        f"📅 Текущий день: {current_day} из {effective_len}\n\n"
        f"🌙 **Фаза:** {phase_title}"
    )
    
    if days_left > 0:
        text += f" — день {phase_info['days_in_phase']}, осталось {days_left} дней\n"
    else:
        text += f" — последний день фазы\n"
    
    text += (
        f"\n💫 Овуляция была: {format_date_russian(last_ovulation)}\n"
        f"💫 Следующая овуляция: {format_date_russian(next_ovulation)} (через {days_until_ovulation} {'день' if days_until_ovulation == 1 else 'дня' if days_until_ovulation < 5 else 'дней'})\n"
        f"🩸 Менструация: {format_date_russian(next_period)} (через {days_until_period} {'день' if days_until_period == 1 else 'дня' if days_until_period < 5 else 'дней'})\n"
    )
    
    if ref:
        symptoms = ref.get("symptoms", [])
        behavior = ref.get("behavior", [])
        recs = ref.get("male_recommendations", [])
        if is_pms and not symptoms:
            text += f"\n⚠️ **ПМС: АКТИВЕН!**\n"
        if symptoms:
            text += f"\n📝 **Симптомы:**\n{_format_ref_block(symptoms)}\n\n"
        if behavior:
            text += f"👤 **Поведение:**\n{_format_ref_block(behavior)}\n\n"
        text += f"💡 **Рекомендации для вас:**\n\n{_format_ref_block(recs)}"
    else:
        if is_pms:
            text += f"\n⚠️ **ПМС: АКТИВЕН!**\n📝 Симптомы: {phase.symptoms}\n\n"
        else:
            text += f"\n📝 **Симптомы:** {phase.symptoms}\n👤 **Поведение:** {phase.behavior}\n\n"
        text += get_detailed_recommendations(phase.name, is_pms)
    
    return text


def generate_notification_for_phase_stage(user: User, phase_name_en: str, stage: str = None) -> str:
    """Текст отчёта для начала конкретной фазы/подфазы (для уведомлений при старте фазы/подфазы)."""
    effective_len = effective_cycle_length_for_user(user)
    calculator = CycleCalculator(
        user.last_period_start,
        effective_len,
        user.period_length
    )
    ref = get_reference_phase(phase_name_en, stage)
    phase_info = calculator.get_current_phase()
    current_day = phase_info["current_day"]
    next_period = calculator.get_next_period_date()
    last_ovulation = calculator.get_last_ovulation_date()
    next_ovulation = calculator.get_next_ovulation_date()
    days_until_period = (next_period - date.today()).days
    days_until_ovulation = (next_ovulation - date.today()).days
    phase_title = ref.get("subphase_name") or ref.get("phase_name_ru") or phase_name_en
    symptoms = ref.get("symptoms", [])
    behavior = ref.get("behavior", [])
    recs = ref.get("male_recommendations", [])
    text = (
        f"📊 **Отчёт: начало фазы/подфазы**\n\n"
        f"👩 Для: {user.girlfriend_name}\n"
        f"📅 Текущий день: {current_day} из {effective_len}\n\n"
        f"🌙 **Началась:** {phase_title}\n\n"
        f"💫 Овуляция была: {format_date_russian(last_ovulation)}\n"
        f"💫 Следующая овуляция: {format_date_russian(next_ovulation)} (через {days_until_ovulation} {'день' if days_until_ovulation == 1 else 'дня' if days_until_ovulation < 5 else 'дней'})\n"
        f"🩸 Менструация: {format_date_russian(next_period)} (через {days_until_period} {'день' if days_until_period == 1 else 'дня' if days_until_period < 5 else 'дней'})\n\n"
    )
    if symptoms:
        text += f"📝 **Симптомы:**\n{_format_ref_block(symptoms)}\n\n"
    if behavior:
        text += f"👤 **Поведение:**\n{_format_ref_block(behavior)}\n\n"
    text += f"💡 **Рекомендации для вас:**\n\n{_format_ref_block(recs)}"
    return text


async def send_daily_notifications(context: ContextTypes.DEFAULT_TYPE):
    """Отправка уведомлений только при начале фазы или подфазы. В один день может быть несколько отчётов — закрепляется последнее."""
    session = SessionLocal()
    try:
        users = session.query(User).filter(
            User.notifications_enabled == True,
            User.last_period_start.isnot(None)
        ).all()
        
        for user in users:
            try:
                timezone_offset = get_timezone_offset(user)
                msk_tz = pytz.timezone('Europe/Moscow')
                msk_time = datetime.now(msk_tz)
                user_time = msk_time + timedelta(hours=timezone_offset)
                current_time = user_time.strftime('%H:%M')
                user_date = user_time.date()
                
                if current_time == user.notification_time:
                    effective_len = effective_cycle_length_for_user(user)
                    cycle_data = calculate_menstrual_cycle(
                        effective_len, user.period_length, user.last_period_start
                    )
                    starts_today = get_phase_subphase_starts_on_date(cycle_data, user_date)
                    
                    if not starts_today:
                        continue
                    if user.last_notification_date == user_date:
                        continue
                    
                    # Открепляем предыдущее закреплённое сообщение
                    if user.pinned_message_id:
                        try:
                            await context.bot.unpin_chat_message(
                                chat_id=user.id,
                                message_id=user.pinned_message_id
                            )
                        except Exception as e:
                            logger.warning(f"Не удалось открепить сообщение для пользователя {user.id}: {e}")
                        user.pinned_message_id = None
                    
                    sent_messages = []
                    for phase_name_en, stage in starts_today:
                        notification_text = generate_notification_for_phase_stage(user, phase_name_en, stage)
                        msg = await context.bot.send_message(
                            chat_id=user.id,
                            text=notification_text,
                            parse_mode='Markdown'
                        )
                        sent_messages.append(msg)
                    
                    if sent_messages:
                        last_msg = sent_messages[-1]
                        try:
                            await context.bot.pin_chat_message(
                                chat_id=user.id,
                                message_id=last_msg.message_id,
                                disable_notification=True
                            )
                            user.pinned_message_id = last_msg.message_id
                        except Exception as e:
                            logger.warning(f"Не удалось закрепить сообщение для пользователя {user.id}: {e}")
                    
                    user.last_notification_date = user_date
                    user.days_with_notifications += 1
                    session.commit()
                
                # Проверяем уведомления о приближении фазы (в 15:00)
                # Отправляем отдельно от ежедневных уведомлений, только один раз в день
                if current_time == "15:00" and user.notify_phase_start:
                    # Проверяем, не отправляли ли уже уведомление о приближении фазы сегодня
                    if not user.last_phase_advance_date or user.last_phase_advance_date != date.today():
                        calculator = CycleCalculator(
                            user.last_period_start,
                            effective_cycle_length_for_user(user),
                            user.period_length
                        )
                        next_phase_info = calculator.get_next_phase()
                        
                        if next_phase_info and next_phase_info['days_until'] == 2:
                            phase = next_phase_info['phase']
                            phase_start_date = next_phase_info['start_date']
                            recommendations = get_detailed_recommendations(phase.name, False)
                            
                            phase_advance_text = (
                                f"🔔 **Приближается новая фаза**\n\n"
                                f"👩 Для: {user.girlfriend_name}\n\n"
                                f"🌙 Через 2 дня начнется фаза: **{phase.name_ru}**\n"
                                f"📅 Дата начала: {format_date_russian(phase_start_date)}\n\n"
                                f"📝 **Что это значит:**\n{phase.description}\n\n"
                                f"{recommendations}"
                            )
                            
                            try:
                                await context.bot.send_message(
                                    chat_id=user.id,
                                    text=phase_advance_text,
                                    parse_mode='Markdown'
                                )
                                # Помечаем, что уведомление отправлено
                                user.last_phase_advance_date = date.today()
                                session.commit()
                            except Exception as e:
                                logger.error(f"Ошибка отправки уведомления о приближении фазы пользователю {user.id}: {e}")
                
                # Проверяем, завершился ли цикл (нужно обновить дату)
                # Делаем это только если не отправляли ежедневное уведомление (чтобы не дублировать)
                if current_time != user.notification_time:
                    effective_len = effective_cycle_length_for_user(user)
                    extended = getattr(user, 'cycle_extended_days', 0) or 0
                    days_since_start = (user_date - user.last_period_start).days + 1
                    # Цикл считается завершённым, когда прошло >= (длина + продление) дней
                    if days_since_start >= effective_len + extended:
                        if user.last_notification_date != user_date:
                            cycle_end_text = (
                                f"🔄 **Цикл завершен!**\n\n"
                                f"👩 Для: {user.girlfriend_name}\n\n"
                                f"📅 Текущий цикл завершился. Необходимо обновить дату начала нового цикла.\n\n"
                                f"💡 **Важно:** Обязательно уточните у своей девушки, начался ли у неё новый цикл "
                                f"(началась ли менструация). Не обновляйте дату, если менструация еще не началась!\n\n"
                                f"Нажмите кнопку ниже, чтобы обновить дату начала нового цикла:"
                            )
                            keyboard = [
                                [InlineKeyboardButton("📆 Обновить дату начала цикла", callback_data="update_cycle_date")],
                                [InlineKeyboardButton("⏪ Цикл закончился раньше", callback_data="cycle_ended_earlier")],
                                [InlineKeyboardButton("⏳ Цикл не завершился вовремя", callback_data="cycle_not_ended_on_time")],
                                [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
                            ]
                            
                            try:
                                await context.bot.send_message(
                                    chat_id=user.id,
                                    text=cycle_end_text,
                                    reply_markup=InlineKeyboardMarkup(keyboard),
                                    parse_mode='Markdown'
                                )
                                # Помечаем, что уведомление отправлено
                                user.last_notification_date = user_date
                                session.commit()
                            except Exception as e:
                                logger.error(f"Ошибка отправки уведомления о завершении цикла пользователю {user.id}: {e}")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления пользователю {user.id}: {e}")
    finally:
        session.close()


def main():
    """Главная функция запуска бота"""
    # Инициализация базы данных
    init_db()
    
    # Создание приложения
    application = Application.builder().token(config.BOT_TOKEN).build()
    
    # Обработчик команды /start
    application.add_handler(CommandHandler("start", start))
    
    # Обработчик команды /cancel
    application.add_handler(CommandHandler("cancel", cancel))
    
    # Тестовые команды (только для пользователя с id 774988626)
    async def test_daily_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Тестовая команда для ежедневного отчета"""
        if update.effective_user.id != ADMIN_USER_ID:
            await update.message.reply_text("❌ У вас нет доступа к этой команде.")
            return
        
        # Создаем тестового пользователя с рандомными данными
        from random import randint
        test_user = User(
            id=ADMIN_USER_ID,
            name="Тестовый пользователь",
            girlfriend_name="Тестовая девушка",
            cycle_length=randint(25, 32),
            period_length=randint(3, 7),
            last_period_start=date.today() - timedelta(days=randint(5, 20)),
            notification_time="09:00",
            timezone=0
        )
        
        notification_text = generate_daily_notification(test_user)
        await update.message.reply_text(notification_text, parse_mode='Markdown')
    
    async def test_phase_advance(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Тестовая команда для уведомления о приближении фазы"""
        if update.effective_user.id != ADMIN_USER_ID:
            await update.message.reply_text("❌ У вас нет доступа к этой команде.")
            return
        
        from random import randint
        test_user = User(
            id=ADMIN_USER_ID,
            name="Тестовый пользователь",
            girlfriend_name="Тестовая девушка",
            cycle_length=randint(25, 32),
            period_length=randint(3, 7),
            last_period_start=date.today() - timedelta(days=randint(5, 20)),
            notification_time="09:00",
            timezone=0
        )
        
        calculator = CycleCalculator(
            test_user.last_period_start,
            test_user.cycle_length,
            test_user.period_length
        )
        next_phase_info = calculator.get_next_phase()
        
        if next_phase_info:
            phase = next_phase_info['phase']
            phase_start_date = next_phase_info['start_date']
            recommendations = get_detailed_recommendations(phase.name, False)
            
            phase_advance_text = (
                f"🔔 **Приближается новая фаза**\n\n"
                f"👩 Для: {test_user.girlfriend_name}\n\n"
                f"🌙 Через 2 дня начнется фаза: **{phase.name_ru}**\n"
                f"📅 Дата начала: {format_date_russian(phase_start_date)}\n\n"
                f"📝 **Что это значит:**\n{phase.description}\n\n"
                f"{recommendations}"
            )
            await update.message.reply_text(phase_advance_text, parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ Не удалось определить следующую фазу.")
    
    async def test_cycle_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Тестовая команда для уведомления о завершении цикла"""
        if update.effective_user.id != ADMIN_USER_ID:
            await update.message.reply_text("❌ У вас нет доступа к этой команде.")
            return
        
        from random import randint
        test_user = User(
            id=ADMIN_USER_ID,
            name="Тестовый пользователь",
            girlfriend_name="Тестовая девушка",
            cycle_length=randint(25, 32),
            period_length=randint(3, 7),
            last_period_start=date.today() - timedelta(days=randint(25, 35)),
            notification_time="09:00",
            timezone=0
        )
        
        cycle_end_text = (
            f"🔄 **Цикл завершен!**\n\n"
            f"👩 Для: {test_user.girlfriend_name}\n\n"
            f"📅 Текущий цикл завершился. Необходимо обновить дату начала нового цикла.\n\n"
            f"💡 **Важно:** Обязательно уточните у своей девушки, начался ли у неё новый цикл "
            f"(началась ли менструация). Не обновляйте дату, если менструация еще не началась!\n\n"
            f"Нажмите кнопку ниже, чтобы обновить дату начала нового цикла:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📆 Обновить дату начала цикла", callback_data="update_cycle_date")],
            [InlineKeyboardButton("⏪ Цикл закончился раньше", callback_data="cycle_ended_earlier")],
            [InlineKeyboardButton("⏳ Цикл не завершился вовремя", callback_data="cycle_not_ended_on_time")],
            [InlineKeyboardButton("🔙 Главное меню", callback_data="back_to_main")]
        ]
        
        await update.message.reply_text(
            cycle_end_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    
    application.add_handler(CommandHandler("test_daily", test_daily_report))
    application.add_handler(CommandHandler("test_phase", test_phase_advance))
    application.add_handler(CommandHandler("test_cycle", test_cycle_end))
    
    # Выход в главное меню по горячим кнопкам из любого диалога
    async def main_menu_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await show_main_menu_from_keyboard(update, context)
        return ConversationHandler.END

    _keyboard_fallback = MessageHandler(
        filters.Regex(f"^({KEYBOARD_MAIN_MENU}|{KEYBOARD_RESTART})$"),
        main_menu_fallback
    )

    # ConversationHandler для сбора данных
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(begin_filling, pattern="^start_filling$")
        ],
        states={
            COLLECTING_NAME: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_name)],
            COLLECTING_GIRLFRIEND_NAME: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_girlfriend_name)],
            COLLECTING_CYCLE_LENGTH: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_cycle_length)],
            COLLECTING_PERIOD_LENGTH: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_period_length)],
            COLLECTING_LAST_PERIOD: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_last_period)],
            COLLECTING_TIMEZONE: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_timezone)],
            COLLECTING_NOTIFICATION_TIME: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, collect_notification_time)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_chat=True,
        per_user=True,
        per_message=False,
    )
    
    application.add_handler(conv_handler)
    
    # ConversationHandler для изменения времени уведомлений
    time_change_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_change_notification_time, pattern="^change_notification_time$")
        ],
        states={
            CHANGING_NOTIFICATION_TIME: [_keyboard_fallback, MessageHandler(filters.TEXT & ~filters.COMMAND, change_notification_time)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_chat=True,
        per_user=True,
        per_message=False,
    )
    
    application.add_handler(time_change_handler)
    
    # ConversationHandler для обновления даты начала нового цикла и «Цикл закончился раньше»
    async def start_update_cycle_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик для начала обновления даты цикла"""
        query = update.callback_query
        user_id = query.from_user.id
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            await start_update_cycle_date(query, user, session)
        finally:
            session.close()

    async def start_cycle_ended_earlier_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик для «Цикл закончился раньше»"""
        query = update.callback_query
        user_id = query.from_user.id
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            return await start_cycle_ended_earlier(query, user, session)
        finally:
            session.close()

    async def back_to_main_from_update_cycle(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выход из диалога обновления даты и возврат в главное меню"""
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.id == user_id).first()
            await query.edit_message_text(
                "👋 Привет! Добро пожаловать в бот для отслеживания менструального цикла.\n\n"
            "Этот бот создан специально для мужчин, которые хотят лучше понимать и поддерживать "
            "свою девушку в разные периоды её цикла. 💕\n\n"
            "Бот поможет вам:\n"
            "📊 Отслеживать текущую фазу цикла\n"
            "🔔 Получать отчёты при смене фазы и подфазы\n"
            "💡 Получать рекомендации, как лучше поддержать партнершу\n"
            "📚 Изучать информацию о фазах цикла\n\n"
            "Помните: ваша забота и внимание - это проявление любви и уважения! ❤️",
                reply_markup=get_main_menu(user)
            )
        finally:
            session.close()
        return ConversationHandler.END

    cycle_update_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_update_cycle_date_handler, pattern="^update_cycle_date$"),
            CallbackQueryHandler(start_cycle_ended_earlier_handler, pattern="^cycle_ended_earlier$"),
        ],
        states={
            UPDATING_NEW_CYCLE_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, update_cycle_date)],
            COLLECTING_CYCLE_END_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_cycle_end_date)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(back_to_main_from_update_cycle, pattern="^back_to_main$")
        ],
        per_chat=True,
        per_user=True,
        per_message=False,
    )
    
    application.add_handler(cycle_update_handler)
    
    # Постоянные кнопки (Главное меню / Перезапуск) — когда пользователь не в диалоге
    application.add_handler(
        MessageHandler(
            filters.Regex(f"^({KEYBOARD_MAIN_MENU}|{KEYBOARD_RESTART})$"),
            show_main_menu_from_keyboard
        )
    )
    
    # Обработчик кнопок (добавлен после ConversationHandler, поэтому start_filling уже обработан)
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Планировщик для ежедневных уведомлений (проверка каждую минуту)
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(send_daily_notifications, interval=60, first=10)  # Каждую минуту
        logger.info("Планировщик уведомлений запущен")
    else:
        logger.warning("JobQueue не доступен. Уведомления не будут работать. Установите: pip install 'python-telegram-bot[job-queue]'")
    
    # Запуск бота
    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
