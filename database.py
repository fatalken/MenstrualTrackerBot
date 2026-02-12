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
    cycle_length = Column(Integer, default=28)  # Длительность цикла в днях
    period_length = Column(Integer, default=5)  # Длительность менструации в днях
    last_period_start = Column(Date, nullable=True)  # Дата начала последней менструации
    
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
        
        # Проверяем, существует ли таблица users
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
