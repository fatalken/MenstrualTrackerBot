"""
Конфигурационный файл для бота
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Токен бота Telegram
BOT_TOKEN = os.getenv('BOT_TOKEN', '8234150758:AAESo5iQwGlP7QACGqIc4KJL4wOFmzdjLwE')

# Настройки базы данных
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///menstrual_tracker.db')

# Настройки часового пояса по умолчанию
DEFAULT_TIMEZONE = os.getenv('DEFAULT_TIMEZONE', 'Europe/Moscow')

# Время отправки уведомлений по умолчанию (часы:минуты)
DEFAULT_NOTIFICATION_TIME = os.getenv('DEFAULT_NOTIFICATION_TIME', '09:00')
