"""
Модели базы данных для бота отслеживания менструального цикла
"""
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean, DateTime, Float, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date as date_type
import config
import logging
import json

logger = logging.getLogger(__name__)

Base = declarative_base()


class User(Base):
    """Модель пользователя"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)  # Telegram user ID
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Данные профиля
    name = Column(String, nullable=True)  # Имя пользователя
    girlfriend_name = Column(String, nullable=True)  # Имя девушки
    
    # Данные цикла
    cycle_length = Column(Integer, default=28)  # Длительность цикла в днях (исходная/при отсутствии истории)
    period_length = Column(Integer, default=5)  # Длительность менструации в днях
    last_period_start = Column(Date, nullable=True)  # Дата начала последней менструации
    cycle_extended_days = Column(Integer, default=0)  # Доп. дни продления (цикл не завершился вовремя)
    
    # Настройки уведомлений
    notifications_enabled = Column(Boolean, default=True)
    notification_time = Column(String, default='09:00')  # Время в формате HH:MM
    timezone = Column(Integer, default=0)  # Часовой пояс относительно МСК (например: +3, -1)
    notify_daily = Column(Boolean, default=True)  # Ежедневные уведомления
    notify_phase_start = Column(Boolean, default=True)  # Уведомления о начале фаз
    
    # Статистика
    days_with_notifications = Column(Integer, default=0)  # Дней с включенными уведомлениями
    last_notification_date = Column(Date, nullable=True)  # Дата последнего уведомления
    last_phase_advance_date = Column(Date, nullable=True)  # Дата последнего уведомления о приближении фазы
    pinned_message_id = Column(Integer, nullable=True)  # ID закрепленного сообщения
    
    # Состояние заполнения данных
    data_collection_state = Column(String, nullable=True)  # Текущее состояние сбора данных


class CycleRecord(Base):
    """История циклов: один цикл на запись (cycle_info + phases с subphases)."""
    __tablename__ = 'cycle_records'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    cycle_start_date = Column(Date, nullable=False)  # last_menstruation_start этого цикла
    cycle_data = Column(Text, nullable=False)  # JSON: cycle_info + phases
    cycle_actual_end_date = Column(Date, nullable=True)  # фактическая дата окончания (если цикл закончился раньше)
    created_at = Column(DateTime, default=datetime.utcnow)


class CyclePhase(Base):
    """Справочник фаз цикла"""
    __tablename__ = 'cycle_phases'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)  # Название фазы
    name_ru = Column(String, nullable=False)  # Название на русском
    start_day = Column(Integer, nullable=False)  # День начала (относительно цикла)
    end_day = Column(Integer, nullable=False)  # День окончания
    description = Column(String, nullable=True)  # Описание фазы
    symptoms = Column(String, nullable=True)  # Симптомы (JSON строка)
    behavior = Column(String, nullable=True)  # Поведение
    recommendations = Column(String, nullable=True)  # Рекомендации


# Создание движка базы данных
engine = create_engine(config.DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    """Инициализация базы данных - создание таблиц"""
    Base.metadata.create_all(engine)
    
    # Миграция: добавление нового столбца pinned_message_id, если его нет
    session = SessionLocal()
    try:
        # Проверяем, существует ли столбец pinned_message_id
        from sqlalchemy import inspect, text
        inspector = inspect(engine)
        
        if 'cycle_records' in inspector.get_table_names():
            cycle_columns = [col['name'] for col in inspector.get_columns('cycle_records')]
            if 'cycle_actual_end_date' not in cycle_columns:
                logger.info("Добавление столбца cycle_actual_end_date в таблицу cycle_records...")
                try:
                    session.execute(text('ALTER TABLE cycle_records ADD COLUMN cycle_actual_end_date DATE'))
                    session.commit()
                    logger.info("Столбец cycle_actual_end_date успешно добавлен")
                except Exception as e:
                    logger.error(f"Ошибка при добавлении столбца cycle_actual_end_date: {e}")
                    session.rollback()
        
        if 'users' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            if 'pinned_message_id' not in columns:
                logger.info("Добавление столбца pinned_message_id в таблицу users...")
                try:
                    session.execute(text('ALTER TABLE users ADD COLUMN pinned_message_id INTEGER'))
                    session.commit()
                    logger.info("Столбец pinned_message_id успешно добавлен")
                except Exception as e:
                    logger.error(f"Ошибка при добавлении столбца pinned_message_id: {e}")
                    session.rollback()
            
            if 'cycle_extended_days' not in columns:
                logger.info("Добавление столбца cycle_extended_days в таблицу users...")
                try:
                    session.execute(text('ALTER TABLE users ADD COLUMN cycle_extended_days INTEGER DEFAULT 0'))
                    session.commit()
                    logger.info("Столбец cycle_extended_days успешно добавлен")
                except Exception as e:
                    logger.error(f"Ошибка при добавлении столбца cycle_extended_days: {e}")
                    session.rollback()
            # Миграция: добавление столбца last_phase_advance_date
            if 'last_phase_advance_date' not in columns:
                logger.info("Добавление столбца last_phase_advance_date в таблицу users...")
                try:
                    session.execute(text('ALTER TABLE users ADD COLUMN last_phase_advance_date DATE'))
                    session.commit()
                    logger.info("Столбец last_phase_advance_date успешно добавлен")
                except Exception as e:
                    logger.error(f"Ошибка при добавлении столбца last_phase_advance_date: {e}")
                    session.rollback()
    except Exception as e:
        logger.warning(f"Ошибка при миграции базы данных: {e}")
    finally:
        session.close()
    
    # Заполнение справочника фаз цикла
    session = SessionLocal()
    try:
        # Проверяем, есть ли уже данные
        if session.query(CyclePhase).count() == 0:
            phases = [
                CyclePhase(
                    name='menstrual',
                    name_ru='Менструальная',
                    start_day=1,
                    end_day=7,
                    description='Фаза менструального кровотечения. Организм очищается от неоплодотворенной яйцеклетки и эндометрия.',
                    symptoms='Слабость, апатия, боли в животе и голове, усталость',
                    behavior='Низкая активность, потребность в отдыхе, снижение концентрации',
                    recommendations='Обеспечьте максимальный комфорт, будьте терпеливы, предложите помощь по дому'
                ),
                CyclePhase(
                    name='follicular',
                    name_ru='Фолликулярная',
                    start_day=7,
                    end_day=14,
                    description='Фаза созревания фолликула. Уровень эстрогена растет, организм готовится к овуляции.',
                    symptoms='Прилив сил, улучшение настроения, чистая кожа, повышение энергии',
                    behavior='Повышенная активность, уверенность, инициативность, социальная активность',
                    recommendations='Отличное время для совместных активностей, планирования, новых начинаний'
                ),
                CyclePhase(
                    name='ovulation',
                    name_ru='Овуляция',
                    start_day=14,
                    end_day=15,
                    description='Выход зрелой яйцеклетки из фолликула. Пик фертильности.',
                    symptoms='Повышенное либидо, прилив сил, чувство привлекательности, возможны легкие боли внизу живота',
                    behavior='Максимальная активность, повышенное половое влечение, уверенность в себе',
                    recommendations='Идеальное время для романтики, интимной близости, активного общения'
                ),
                CyclePhase(
                    name='luteal',
                    name_ru='Лютеиновая (ПМС)',
                    start_day=15,
                    end_day=28,
                    description='Фаза после овуляции. Желтое тело вырабатывает прогестерон. Во второй половине - предменструальный синдром.',
                    symptoms='Раздражительность, усталость, отеки, изменения аппетита, перепады настроения, вздутие живота',
                    behavior='Эмоциональная нестабильность, потребность в поддержке, снижение активности',
                    recommendations='Максимальная поддержка! Помогайте больше, требуйте меньше. Избегайте конфликтов. Будьте терпеливы и понимающими'
                )
            ]
            session.add_all(phases)
            session.commit()
    finally:
        session.close()


def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def save_cycle_record(user_id: int, cycle_start_date, cycle_data: dict):
    """
    Сохранить рассчитанный цикл в историю (новая запись, без перезаписи).
    cycle_data — результат calculate_menstrual_cycle (cycle_info + phases).
    """
    session = SessionLocal()
    try:
        start_date = cycle_start_date.date() if isinstance(cycle_start_date, datetime) else cycle_start_date
        if not isinstance(start_date, date_type):
            start_date = cycle_data["cycle_info"]["last_menstruation_start"]
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        record = CycleRecord(
            user_id=user_id,
            cycle_start_date=start_date,
            cycle_data=json.dumps(cycle_data, ensure_ascii=False)
        )
        session.add(record)
        session.commit()
        logger.info(f"Сохранён цикл для user_id={user_id}, start={start_date}")
    except Exception as e:
        logger.error(f"Ошибка сохранения цикла: {e}")
        session.rollback()
    finally:
        session.close()


def get_last_cycle_record(user_id: int):
    """Получить последнюю запись цикла пользователя (по дате начала)."""
    session = SessionLocal()
    try:
        return session.query(CycleRecord).filter(
            CycleRecord.user_id == user_id
        ).order_by(CycleRecord.cycle_start_date.desc()).first()
    finally:
        session.close()


def get_last_n_cycle_records(user_id: int, n: int = 4):
    """Последние n записей циклов пользователя (по убыванию даты начала)."""
    session = SessionLocal()
    try:
        return session.query(CycleRecord).filter(
            CycleRecord.user_id == user_id
        ).order_by(CycleRecord.cycle_start_date.desc()).limit(n).all()
    finally:
        session.close()


def get_effective_cycle_length(user_id: int, fallback_cycle_length: int = 28) -> int:
    """
    Длительность цикла по среднему за последние 1–3 завершённых цикла из БД.
    Длина цикла = (дата окончания − дата начала) в днях.
    Если записей меньше двух — возвращается fallback_cycle_length (исходное значение пользователя).
    Результат ограничен диапазоном 21–35.
    """
    records = get_last_n_cycle_records(user_id, n=4)
    if not records:
        return max(21, min(35, fallback_cycle_length))
    lengths = []
    for i in range(1, len(records)):
        r_cur = records[i]
        r_next = records[i - 1]
        if r_cur.cycle_actual_end_date is not None:
            length = (r_cur.cycle_actual_end_date - r_cur.cycle_start_date).days + 1
        else:
            length = (r_next.cycle_start_date - r_cur.cycle_start_date).days
        if length >= 1:
            lengths.append(length)
        if len(lengths) >= 3:
            break
    if not lengths:
        return max(21, min(35, fallback_cycle_length))
    avg = round(sum(lengths) / len(lengths))
    return max(21, min(35, avg))


def update_cycle_record_actual_end(user_id: int, cycle_actual_end_date) -> bool:
    """
    Обновить фактическую дату окончания у последнего цикла пользователя.
    Используется при «Цикл закончился раньше».
    """
    session = SessionLocal()
    try:
        record = session.query(CycleRecord).filter(
            CycleRecord.user_id == user_id
        ).order_by(CycleRecord.cycle_start_date.desc()).first()
        if not record:
            return False
        record.cycle_actual_end_date = cycle_actual_end_date.date() if hasattr(cycle_actual_end_date, 'date') else cycle_actual_end_date
        session.commit()
        logger.info(f"Обновлена дата окончания цикла user_id={user_id}, record_id={record.id}, end={record.cycle_actual_end_date}")
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления даты окончания цикла: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def reset_user_and_cycle_data(session, user_id: int) -> bool:
    """
    Удалить все записи циклов пользователя и сбросить данные профиля (для «Заполнить данные заново»).
    Использует переданную сессию и выполняет commit.
    """
    try:
        session.query(CycleRecord).filter(CycleRecord.user_id == user_id).delete()
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            return False
        user.name = None
        user.girlfriend_name = None
        user.cycle_length = 28
        user.period_length = 5
        user.last_period_start = None
        user.cycle_extended_days = 0
        user.data_collection_state = None
        user.notification_time = "09:00"
        user.timezone = 0
        user.notifications_enabled = True
        user.notify_daily = True
        user.notify_phase_start = True
        user.last_notification_date = None
        user.last_phase_advance_date = None
        user.pinned_message_id = None
        user.days_with_notifications = 0
        session.commit()
        logger.info(f"Данные пользователя и циклов сброшены для user_id={user_id}")
        return True
    except Exception as e:
        logger.error(f"Ошибка сброса данных пользователя: {e}")
        session.rollback()
        return False
