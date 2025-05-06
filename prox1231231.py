import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import telebot
import schedule
import time
import datetime
import io
from telebot import types
import logging
import json
import os
import cv2

import numpy as np
from typing import Dict, List, Tuple, Optional
from dateutil.relativedelta import relativedelta
import calendar
import matplotlib
matplotlib.use('Agg')  # Должно быть ДО импорта pyplot
import matplotlib.pyplot as plt

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot_log.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Константы
TOKEN = '8030511411:AAGQSvX8LALdHqblA1eZBpspAOGzzFKoqBE'
CHAT_IDS_FILE = 'chat_ids.json'  # Файл для хранения ID чатов пользователей
DATA_FILE = 'energy_data.xlsx'  # файл с данными
DATA_FILE_1 = 'ex.xlsx'
DATA_FILE_2 = 'ex.xlsx'
THRESHOLD_MULTIPLIER = 1.5  # Множитель для определения аномального потребления

# Инициализация бота
bot = telebot.TeleBot(TOKEN)

# Структура для хранения средних значений потребления по оборудованию
equipment_avg_consumption = {}  # Будет заполнено при запуске

# Словарь для хранения текущего состояния диалога с пользователем
user_states = {}


# Состояния диалога для выбора дат
class UserState:
    IDLE = 0
    WAITING_FOR_START_DATE = 1
    WAITING_FOR_END_DATE = 2
    WAITING_FOR_EQUIPMENT = 3
    WAITING_FOR_DAILY_REPORT_DATE = "waiting_for_daily_report_date"


class EnergyMonitor:
    """Класс для анализа и мониторинга потребления электроэнергии"""

    def __init__(self, data_file: str):
        self.data_file = data_file
        self.equipment_data = self.load_data()  # Загружаем основные данные
        self.equipment_stats = {}
        self.last_data_date = self.equipment_data['date'].max() if self.equipment_data is not None \
    else None


    def get_equipment_list(self, df):
        """Returns list of equipment names from dataframe"""
        if df is None:
            return []
        return [col for col in df.columns
                if col not in ['date', 'time', 'datetime']]

    def load_data(self, start_date=None, end_date=None) -> pd.DataFrame:
        """Загрузка данных из Excel файла с корректной обработкой дат и времени"""
        try:
            # Загружаем данные с проверкой на пустой файл
            df = pd.read_excel(self.data_file, skiprows=1)
            if df.empty:
                logger.error("Файл данных пуст или содержит только заголовки")
                return None

            # Проверяем минимальное требуемое количество столбцов
            if len(df.columns) < 3:
                logger.error(f"Файл содержит недостаточно столбцов: {len(df.columns)}")
                return None

            # Преобразуем дату и время с явной проверкой
            date_col = df.columns[0]
            time_col = df.columns[1]

            df['date'] = pd.to_datetime(df[date_col], format='%d.%m.%Y', errors='coerce').dt.date
            df['time'] = pd.to_datetime(df[time_col], format='%H:%M:%S', errors='coerce').dt.time

            # Проверяем успешность преобразования
            if df['date'].isna().any() or df['time'].isna().any():
                logger.warning("Обнаружены некорректные значения даты/времени")

            # Фильтрация по дате если указаны границы
            if start_date and end_date:
                mask = (df['date'] >= start_date) & (df['date'] <= end_date)
                df = df.loc[mask]

            if df.empty:
                logger.warning(f"Нет данных за указанный период: {start_date} - {end_date}")
                return None

            return df

        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {str(e)}", exc_info=True)
            return None
def check_anomalies():
    """Проверка аномалий на основе данных из ex.xlsx"""
    try:
        # Загружаем данные из ex.xlsx
        df = pd.read_excel('ex.xlsx', engine='openpyxl')
        
        if df.empty:
            logger.warning("Файл ex.xlsx пустой!")
            return None  # Если данные пустые, возвращаем None
        
        # Удаляем пробелы в названиях столбцов
        df.columns = df.columns.str.strip()
        
        # Проверка структуры данных
        required_columns = ['date', 'time']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Отсутствует колонка {col} в данных!")
                return None
            
        # Объединяем 'date' и 'time' в новый столбец 'datetime'
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), errors='coerce')
        
        if df['datetime'].isnull().any():
            logger.error("Некорректные данные в столбцах 'date' или 'time'.")
            return None
        
        # Удаляем столбцы 'date' и 'time', так как они больше не нужны
        df.drop(columns=['date', 'time'], inplace=True)
        
        anomalies = []
        
        # Проходим по каждому столбцу с оборудованием
        for equipment in df.columns:
            if equipment == 'datetime':
                continue  # Пропускаем столбец datetime
            
            # Преобразуем данные о расходе в числовой формат
            df[equipment] = pd.to_numeric(df[equipment], errors='coerce')
            
            # Проверяем наличие аномалий (например, если расход равен 0 или превышает норму)
            mean_consumption = df[equipment].mean()
            std_consumption = df[equipment].std()
            
            # Ищем аномалии (значения, выходящие за пределы mean ± 2 * std)
            lower_bound = mean_consumption - 2 * std_consumption
            upper_bound = mean_consumption + 2 * std_consumption
            
            # Фильтруем аномалии
            anomalous_data = df[(df[equipment] < lower_bound) | (df[equipment] > upper_bound)]
            
            if not anomalous_data.empty:
                anomalies.append({
                    'equipment': equipment,
                    'anomalies': anomalous_data[['datetime', equipment]].to_dict(orient='records')  # Преобразуем в список словарей
                })
                
        return anomalies
    
    except Exception as e:
        logger.error(f"Ошибка при проверке аномалий: {e}", exc_info=True)
        return None
    

    def filter_data_by_date(self, date: datetime.datetime) -> pd.DataFrame:
        """Фильтрует данные по заданной дате"""
        df = self.load_data()
        if df is None or df.empty:
            return pd.DataFrame()

        # Конвертируем дату в формат datetime.date для сравнения
        target_date = date.date() if isinstance(date, datetime.datetime) else date

        # Фильтруем данные по дате
        filtered_df = df[df['date'] == target_date]

        if filtered_df.empty:
            logger.warning(f"Нет данных за {target_date}")

        return filtered_df

    def filter_data_by_date_range(self, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
        """Фильтрует данные по диапазону дат"""
        df = self.load_data()
        if df is None or df.empty:
            return pd.DataFrame()

        # Фильтруем данные по диапазону дат
        filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

        if filtered_df.empty:
            logger.warning(f"Нет данных за период с {start_date} по {end_date}")

        return filtered_df

    def analyze_daily_consumption(self, date: datetime.datetime) -> Dict:
        """Анализ дневного потребления для каждой установки"""
        # Фильтруем данные по указанной дате
        daily_data = self.filter_data_by_date(date)

        if daily_data.empty:
            logger.warning(f"Нет данных за {date.date()}")
            return {}

        # Анализируем данные по каждой установке
        results = {}
        equipment_cols = [col for col in daily_data.columns if col not in [daily_data.columns[0], 'date', 'time']]

        for equipment in equipment_cols:
            # Преобразуем данные по оборудованию в числовой формат
            equipment_data = pd.to_numeric(daily_data[equipment], errors='coerce')

            # Пропускаем, если все значения отсутствуют или NaN
            if equipment_data.isna().all():
                continue

            # Определяем периоды работы и простоя
            # Получаем разницу между последовательными показаниями
            consumption_diff = equipment_data.diff().fillna(0)

            # Определяем статус работы
            # Если разница больше минимального порога, считаем что установка работает
            min_threshold = 0.01  # минимальное изменение считается работой

            # Маска для периодов работы
            working_periods = consumption_diff > min_threshold

            # Рассчитываем время работы и простоя (предполагаем, что измерения каждые 30 минут)
            time_interval = 30  # минут между измерениями
            working_time = working_periods.sum() * time_interval
            idle_time = (~working_periods).sum() * time_interval

            # Общее потребление за день (разница между последним и первым показанием)
            if len(equipment_data) > 1:
                # Берем первое и последнее ненулевые значения
                valid_values = equipment_data.dropna()
                if len(valid_values) >= 2:
                    total_consumption = valid_values.iloc[-1] - valid_values.iloc[0]
                else:
                    total_consumption = 0
            else:
                total_consumption = 0

            # Распределяем потребление на рабочее и холостое
            if working_time + idle_time > 0:
                working_consumption = total_consumption * (working_time / (working_time + idle_time))
                idle_consumption = total_consumption * (idle_time / (working_time + idle_time))
            else:
                working_consumption = 0
                idle_consumption = 0

            # Расчет потенциальной экономии
            monthly_savings = idle_consumption * 30 if idle_time > 0 else 0

            # Получаем среднее потребление из базовой статистики
            avg_stats = self.equipment_stats.get(equipment, {})
            avg_consumption = avg_stats.get('avg_consumption', 0)

            results[equipment] = {
                'working_time': working_time,
                'idle_time': idle_time,
                'total_consumption': total_consumption,
                'working_consumption': working_consumption,
                'idle_consumption': idle_consumption,
                'potential_monthly_savings': monthly_savings,
                'avg_consumption': avg_consumption
            }

        return results

    def analyze_period_consumption(self, start_date: datetime.date, end_date: datetime.date) -> Dict:
        """Анализ потребления за указанный период для каждой установки"""
        # Фильтруем данные по указанному диапазону дат
        period_data = self.filter_data_by_date_range(start_date, end_date)

        if period_data.empty:
            logger.warning(f"Нет данных за период с {start_date} по {end_date}")
            return {}

        # Анализируем данные по каждой установке
        results = {}
        equipment_cols = [col for col in period_data.columns if col not in [period_data.columns[0], 'date', 'time']]

        for equipment in equipment_cols:
            # Группируем данные по датам для этого оборудования
            daily_groups = period_data.groupby('date')

            # Инициализируем значения для накопления по дням
            total_working_time = 0
            total_idle_time = 0
            total_consumption = 0
            total_working_consumption = 0
            total_idle_consumption = 0

            # Обрабатываем каждый день в периоде
            for date, group in daily_groups:
                # Преобразуем данные по оборудованию в числовой формат
                equipment_data = pd.to_numeric(group[equipment], errors='coerce')

                # Пропускаем, если все значения отсутствуют или NaN
                if equipment_data.isna().all():
                    continue

                # Определяем периоды работы и простоя
                consumption_diff = equipment_data.diff().fillna(0)
                min_threshold = 0.01  # минимальное изменение считается работой
                working_periods = consumption_diff > min_threshold

                # Рассчитываем время работы и простоя (предполагаем, что измерения каждые 30 минут)
                time_interval = 30  # минут между измерениями
                working_time = working_periods.sum() * time_interval
                idle_time = (~working_periods).sum() * time_interval

                # Общее потребление за день
                if len(equipment_data) > 1:
                    valid_values = equipment_data.dropna()
                    if len(valid_values) >= 2:
                        day_consumption = valid_values.iloc[-1] - valid_values.iloc[0]
                    else:
                        day_consumption = 0
                else:
                    day_consumption = 0

                # Распределяем потребление на рабочее и холостое
                if working_time + idle_time > 0:
                    day_working_consumption = day_consumption * (working_time / (working_time + idle_time))
                    day_idle_consumption = day_consumption * (idle_time / (working_time + idle_time))
                else:
                    day_working_consumption = 0
                    day_idle_consumption = 0

                # Накапливаем значения
                total_working_time += working_time
                total_idle_time += idle_time
                total_consumption += day_consumption
                total_working_consumption += day_working_consumption
                total_idle_consumption += day_idle_consumption

            # Получаем среднее потребление из базовой статистики
            avg_stats = self.equipment_stats.get(equipment, {})
            avg_consumption = avg_stats.get('avg_consumption', 0)

            # Расчет потенциальной экономии (экстраполяция на месяц)
            days_in_period = (end_date - start_date).days + 1
            monthly_savings = (total_idle_consumption / days_in_period) * 30 if days_in_period > 0 else 0

            results[equipment] = {
                'working_time': total_working_time,
                'idle_time': total_idle_time,
                'total_consumption': total_consumption,
                'working_consumption': total_working_consumption,
                'idle_consumption': total_idle_consumption,
                'potential_monthly_savings': monthly_savings,
                'avg_consumption': avg_consumption,
                'days_in_period': days_in_period
            }

        return results

    def check_anomalies(self) -> List[Dict]:
        """Проверка аномального потребления"""
        latest_data, last_date = self.get_latest_data()
        if latest_data is None or last_date is None:
            return []

        anomalies = []

        for equipment, value in latest_data.items():
            # Получаем среднее потребление из статистики оборудования
            avg_stats = self.equipment_stats.get(equipment, {})
            avg_consumption = avg_stats.get('avg_consumption', 0)

            # Проверяем, что значение - число, а не дата/время и выше порога
            if isinstance(value, (int, float)) and avg_consumption > 0:
                # Если текущее потребление превышает среднее в N раз
                if value > avg_consumption * THRESHOLD_MULTIPLIER:
                    anomalies.append({
                        'equipment': equipment,
                        'current_consumption': float(value),
                        'avg_consumption': float(avg_consumption),
                        'timestamp': last_date
                    })

        return anomalies

    def generate_consumption_chart(self, equipment_name: str, start_date=None, end_date=None,
                                   data_period: int = 7) -> io.BytesIO:
        """Генерация графика потребления за период"""
        # Загружаем данные
        df = self.load_data()
        if df is None or df.empty:
            # Создаем пустой график в случае отсутствия данных
            plt.figure(figsize=(10, 6))
            plt.title(f"Нет данных для {equipment_name}")
            plt.xlabel("Дата")
            plt.ylabel("Потребление, кВт*ч")
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close()
            return buf

        # Если даты не указаны, используем последние data_period дней
        if start_date is None:
            end_date = datetime.datetime.now().date()
            start_date = end_date - datetime.timedelta(days=data_period)

        # Фильтруем данные по датам
        filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

        if filtered_df.empty or equipment_name not in filtered_df.columns:
            plt.figure(figsize=(10, 6))
            plt.title(f"Нет данных по {equipment_name} за выбранный период")
            plt.xlabel("Дата")
            plt.ylabel("Потребление, кВт*ч")
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close()
            return buf

        # Группируем данные по дням для конкретного оборудования
        daily_consumption = filtered_df.groupby('date')[equipment_name].agg(['first', 'last'])
        daily_consumption['consumption'] = daily_consumption['last'] - daily_consumption['first']

        # Создаем график
        plt.figure(figsize=(12, 7))

        # Даты для отображения
        dates = daily_consumption.index
        consumption_values = daily_consumption['consumption'].values

        # Строим график
        plt.bar(dates, consumption_values, alpha=0.6, color='skyblue')
        plt.plot(dates, consumption_values, marker='o', linestyle='-', color='blue')

        # Добавляем значения над точками
        for i, value in enumerate(consumption_values):
            plt.annotate(f"{value:.2f}",
                         (dates[i], value),
                         textcoords="offset points",
                         xytext=(0, 10),
                         ha='center')

        plt.title(f'Потребление {equipment_name} за период {start_date} - {end_date}')
        plt.xlabel('Дата')
        plt.ylabel('кВт·ч')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45)

        # Добавляем среднюю линию
        if len(consumption_values) > 0:
            avg_consumption = np.mean(consumption_values)
            plt.axhline(y=avg_consumption, color='r', linestyle='--', alpha=0.7)
            plt.text(dates[0], avg_consumption, f"Среднее: {avg_consumption:.2f} кВт·ч",
                     color='r', fontsize=10, verticalalignment='bottom')

        plt.tight_layout()

        # Сохраняем график в байтовый поток
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()

        return buf


# Функции для работы с Telegram
def save_chat_id(chat_id: int):
    """Сохранение ID чата для рассылки"""
    try:
        if os.path.exists(CHAT_IDS_FILE):
            with open(CHAT_IDS_FILE, 'r') as f:
                chat_ids = json.load(f)
        else:
            chat_ids = []

        if chat_id not in chat_ids:
            chat_ids.append(chat_id)

            with open(CHAT_IDS_FILE, 'w') as f:
                json.dump(chat_ids, f)

            logger.info(f"Добавлен новый чат ID: {chat_id}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении chat_id: {e}")


def get_chat_ids() -> List[int]:
    """Получение списка ID чатов для рассылки"""
    try:
        if os.path.exists(CHAT_IDS_FILE):
            with open(CHAT_IDS_FILE, 'r') as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Ошибка при получении chat_ids: {e}")
        return []


def format_time_minutes(minutes: int) -> str:
    """Форматирование времени в часы и минуты"""
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} часов {mins} минут"


def generate_daily_report(equipment_name: str, data: Dict) -> str:
    """Генерация ежедневного отчета для конкретной установки"""
    working_time = format_time_minutes(data['working_time'])
    idle_time = format_time_minutes(data['idle_time'])
    savings = round(data['potential_monthly_savings'], 2)

    report = (
        f"📊 Отчет за {datetime.datetime.now().strftime('%d.%m.%Y')}\n"
        f"• Оборудование \"{equipment_name}\"\n"
        f"Общее время полезной работы: {working_time}\n"
        f"Время холостого хода: {idle_time}\n"
    )

    if data['idle_time'] > 60:  # Если простой более часа
        report += (
            f"❗ Рекомендация: Отключайте установку на перерывах\n"
            f"💰 Экономия: До {savings} кВт·ч/мес при соблюдении рационального подхода\n"
        )

    return report


def generate_period_report(equipment_name: str, data: Dict, start_date: datetime.date, end_date: datetime.date) -> str:
    """Генерация отчета за период для конкретной установки"""
    working_time = format_time_minutes(data['working_time'])
    idle_time = format_time_minutes(data['idle_time'])
    savings = round(data['potential_monthly_savings'], 2)

    # Форматируем потребление
    total_consumption = round(data['total_consumption'], 2)
    working_consumption = round(data['working_consumption'], 2)
    idle_consumption = round(data['idle_consumption'], 2)

    # Расчет среднего потребления в день
    days = data.get('days_in_period', 1)
    daily_avg = total_consumption / days if days > 0 else 0

    report = (
        f"📊 Отчет за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}\n"
        f"• Оборудование \"{equipment_name}\"\n"
        f"Общее время полезной работы: {working_time}\n"
        f"Время холостого хода: {idle_time}\n"
        f"Всего потреблено: {total_consumption} кВт·ч\n"
        f"Потребление во время работы: {working_consumption} кВт·ч\n"
        f"Потребление на холостом ходу: {idle_consumption} кВт·ч\n"
        f"Среднесуточное потребление: {round(daily_avg, 2)} кВт·ч/день\n"
    )

    if data['idle_time'] > 60:  # Если простой более часа
        report += (
            f"❗ Рекомендация: Отключайте установку на перерывах\n"
            f"💰 Потенциальная экономия: До {savings} кВт·ч/мес при соблюдении рационального подхода\n"
        )

    return report


def generate_anomaly_alert(anomaly: Dict) -> str:
    """Генерация предупреждения об аномальном потреблении"""
    # Форматируем время из datetime объекта
    if isinstance(anomaly['timestamp'], datetime.datetime):
        timestamp = anomaly['timestamp'].strftime('%H:%M')
    else:
        timestamp = "неизвестное время"  # на случай, если timestamp не datetime

    current = round(anomaly['current_consumption'], 2)
    avg = round(anomaly['avg_consumption'], 2)

    alert = (
        f"🚨 *Срочно!*\n"
        f"Оборудование \"{anomaly['equipment']}\" потребляет {current} кВт·ч ({timestamp}).\n"
        f"Среднее значение: {avg} кВт·ч\n"
        f"Возможна перегрузка сети. Проверьте настройки!"
    )

    return alert


# Вспомогательные функции для работы с календарем и выбором дат
def create_calendar_markup(year=None, month=None):
    """Создание разметки с календарем для выбора даты"""
    now = datetime.datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    markup = types.InlineKeyboardMarkup(row_width=7)

    # Добавляем заголовок с месяцем и годом и кнопки навигации
    month_name = calendar.month_name[month]
    header_buttons = [
        types.InlineKeyboardButton("<<", callback_data=f"calendar_prev_{year}_{month}"),
        types.InlineKeyboardButton(f"{month_name} {year}", callback_data="ignore"),
        types.InlineKeyboardButton(">>", callback_data=f"calendar_next_{year}_{month}")
    ]
    markup.row(*header_buttons)

    # Добавляем названия дней недели
    days_of_week = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    week_buttons = [types.InlineKeyboardButton(day, callback_data="ignore") for day in days_of_week]
    markup.row(*week_buttons)

    # Получаем календарь на выбранный месяц
    month_calendar = calendar.monthcalendar(year, month)

    # Добавляем дни месяца
    for week in month_calendar:
        week_buttons = []
        for day in week:
            if day == 0:
                # Пустая клетка для дней не из текущего месяца
                week_buttons.append(types.InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                # Дата в формате гггг-мм-дд
                date_str = f"{year:04d}-{month:02d}-{day:02d}"
                week_buttons.append(types.InlineKeyboardButton(
                    str(day), callback_data=f"date_{date_str}"
                ))
        markup.row(*week_buttons)

    # Добавляем кнопку отмены
    markup.add(types.InlineKeyboardButton("Отмена", callback_data="cancel_date_selection"))

    return markup


# Обработчики команд Telegram
@bot.message_handler(commands=['start'])
def start_handler(message):
    """Обработчик команды /start"""
    chat_id = message.chat.id
    save_chat_id(chat_id)

    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    item1 = types.KeyboardButton('📊 Дневной отчет')
    item2 = types.KeyboardButton('📈 Графики потребления')
    item3 = types.KeyboardButton('⚠️ Проверить аномалии')
    item4 = types.KeyboardButton('📅 Отчет за период')
    item5 = types.KeyboardButton('📷 Распознать счётчик')  # Новая кнопка
    item6 = types.KeyboardButton('ℹ️ Информация')
    markup.add(item1, item2, item3, item4, item5, item6)

    bot.send_message(
        chat_id,
        "Добро пожаловать в систему мониторинга энергопотребления!\n\n"
        "Я буду отправлять вам ежедневные отчеты о работе оборудования "
        "и предупреждать о потенциальных аномалиях в потреблении энергии.\n\n"
        "Выберите действие из меню ниже:",
        reply_markup=markup
    )


def recognize_meter_reading(image_path: str) -> Optional[str]:
    """Распознает показания счетчика с изображения с помощью tesserocr"""
    try:
        # Загружаем изображение с помощью OpenCV
        img = cv2.imread(image_path)
        if img is None:
            logger.error("Не удалось загрузить изображение")
            return None

        # Преобразуем в grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Применяем thresholding для улучшения контраста
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

        # Сохраняем временный файл для tesseract
        temp_file = "temp_preprocessed.png"
        cv2.imwrite(temp_file, thresh)

        # Распознаем текст с помощью tesserocr
        with PyTessBaseAPI(lang='eng') as api:
            api.SetImageFile(temp_file)
            api.SetVariable("tessedit_char_whitelist", "0123456789")  # Только цифры
            text = api.GetUTF8Text().strip()

        # Удаляем временный файл
        os.remove(temp_file)

        # Очищаем результат (оставляем только цифры)
        digits = ''.join(filter(str.isdigit, text))

        if not digits:
            logger.warning("Не удалось распознать цифры на изображении")
            return None

        return digits

    except Exception as e:
        logger.error(f"Ошибка распознавания: {e}", exc_info=True)
        return None

@bot.message_handler(func=lambda message: message.text == '📷 Распознать счётчик')

def request_meter_photo(message):
    markup = types.ReplyKeyboardRemove()  # Убираем клавиатуру
    bot.send_message(
        message.chat.id,
        "📸 Пожалуйста, отправьте четкое фото счётчика. Убедитесь, что:\n"
        "• Счётчик хорошо освещён\n"
        "• Цифры видны чётко\n"
        "• Весь счётчик в кадре\n\n"
        "Для лучшего результата:\n"
        "1. Подойдите ближе к счетчику\n"
        "2. Убедитесь, что цифры не бликуют\n"
        "3. Держите камеру прямо напротив счетчика",
        reply_markup=markup
    )

    @bot.message_handler(content_types=['photo'])
    def handle_meter_photo(message):
        try:
            chat_id = message.chat.id

            # Получаем файл фото
            file_info = bot.get_file(message.photo[-1].file_id)
            downloaded_file = bot.download_file(file_info.file_path)

            # Сохраняем временный файл
            temp_file = f"temp_{chat_id}.jpg"
            with open(temp_file, 'wb') as new_file:
                new_file.write(downloaded_file)

            # Отправляем сообщение о начале обработки
            processing_msg = bot.send_message(chat_id, "🔄 Обрабатываю изображение...")

            # Распознаем показания
            result = recognize_meter_reading(temp_file)

            # Удаляем временный файл
            os.remove(temp_file)

            if result:
                # Сохраняем результат в состоянии пользователя
                user_states[chat_id] = {
                    'recognized_value': result,
                    'photo_file_id': message.photo[-1].file_id
                }

                # Создаем клавиатуру для подтверждения
                markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
                markup.add('✅ Да', '❌ Нет')
                markup.add('🔄 Повторить попытку')

                bot.edit_message_text(
                    f"🔢 Распознанные показания: *{result}*\n\n"
                    "Показания верны?",
                    chat_id=chat_id,
                    message_id=processing_msg.message_id,
                    parse_mode='Markdown',
                    reply_markup=markup
                )
            else:
                bot.edit_message_text(
                    "Не удалось распознать показания. Попробуйте сделать фото ещё раз.",
                    chat_id=chat_id,
                    message_id=processing_msg.message_id
                )
                # Предлагаем попробовать снова
                request_meter_photo(message)

        except Exception as e:
            logger.error(f"Ошибка обработки фото: {e}", exc_info=True)
            bot.send_message(
                message.chat.id,
                "Произошла ошибка при обработке фото. Попробуйте ещё раз."
            )

            @bot.message_handler(func=lambda m: m.text in ['✅ Да', '❌ Нет', '🔄 Повторить попытку'])
            def handle_meter_reading_confirmation(message):
                chat_id = message.chat.id
                user_state = user_states.get(chat_id, {})

                if message.text == '✅ Да':
                    # Сохраняем подтвержденные данные
                    value = user_state.get('recognized_value')
                    if value:
                        # Здесь можно добавить логику сохранения в базу данных
                        bot.send_message(
                            chat_id,
                            f"Показания {value} успешно сохранены!",
                            reply_markup=types.ReplyKeyboardRemove()
                        )
                    else:
                        bot.send_message(
                            chat_id,
                            "Ошибка: не найдены данные для сохранения",
                            reply_markup=types.ReplyKeyboardRemove()
                        )

                elif message.text == '❌ Нет':
                    bot.send_message(
                        chat_id,
                        "Показания не сохранены.",
                        reply_markup=types.ReplyKeyboardRemove()
                    )

                elif message.text == '🔄 Повторить попытку':
                    # Отправляем оригинальное фото еще раз для повторной обработки
                    file_id = user_state.get('photo_file_id')
                    if file_id:
                        bot.send_photo(chat_id, file_id, caption="Попробуем еще раз...")
                        # Вызываем обработчик фото снова
                        msg = types.Message(message_id=message.message_id,
                                            from_user=message.from_user,
                                            date=message.date,
                                            chat=message.chat,
                                            content_type='photo',
                                            photo=[types.PhotoSize(file_id=file_id, width=0, height=0, file_size=0)])
                        handle_meter_photo(msg)
                    else:
                        request_meter_photo(message)

                # Очищаем состояние
                user_states.pop(chat_id, None)
@bot.message_handler(commands=['report'])
def report_handler(message):
    """Обработчик команды /report - запрос отчета на текущую дату"""
    """Обработчик команды /report или кнопки '📊 Дневной отчет'"""
    chat_id = message.chat.id
    user_states[chat_id] = UserState.WAITING_FOR_DAILY_REPORT_DATE
    
    markup = create_calendar_markup()
    bot.send_message(
        chat_id,
        "📅 Пожалуйста, выберите дату для дневного отчета:",
        reply_markup=markup
    )
    chat_id = message.chat.id

    try:
        monitor = EnergyMonitor(DATA_FILE_1)
        today = datetime.datetime.now()
        results = monitor.analyze_daily_consumption(today)

        if not results:
            bot.send_message(chat_id, "Данные за сегодня отсутствуют")
            return

        # Отправляем отчет по каждой установке
        for equipment, data in results.items():
            report = generate_daily_report(equipment, data)
            bot.send_message(chat_id, report)

            # Добавляем задержку чтобы избежать лимитов API
            time.sleep(0.1)

    except Exception as e:
        logger.error(f"Ошибка при формировании отчета: {e}")
        bot.send_message(chat_id, "Произошла ошибка при формировании отчета")


@bot.message_handler(commands=['check'])
def load_and_filter_data(date: datetime.date):
    """Загружает данные из Excel и фильтрует по выбранной дате"""
    try:
        df = pd.read_excel('ex.xlsx')
        
        # Убедитесь, что названия столбцов совпадают с вашими
        df['date'] = pd.to_datetime(df['date'])  # дата + время
        
        # Фильтрация по дате (без учета времени)
        filtered_data = df[df['date'].dt.date == date]
        
        if filtered_data.empty:
            return None, None
        
        # Группировка по оборудованию
        equipment_consumption = filtered_data.groupby('equipment')['consumption'].sum()
        
        return equipment_consumption, filtered_data
    
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return None, None
    
def check_anomalies_handler(message):
    """Обработчик проверки аномалий"""
    chat_id = message.chat.id
    
    anomalies = check_anomalies()
    
    if anomalies is None or not anomalies:
        bot.send_message(chat_id, "Аномалии не обнаружены.")
        return
    
    for anomaly in anomalies:
        equipment = anomaly['equipment']
        anomalous_data = anomaly['anomalies']
        
        # Преобразуем список аномальных данных в DataFrame
        anomalous_df = pd.DataFrame(anomalous_data)
        
        # Теперь можно использовать .iterrows()
        for _, row in anomalous_df.iterrows():
            datetime_str = row['datetime']
            consumption = row[equipment]
            
            # Формируем сообщение о каждой аномалии
            message_text = (f"⚠️ Аномалия в оборудовании: {equipment}\n"
                            f"Дата и время: {datetime_str}\n"
                            f"Потребление: {consumption}")
            
            bot.send_message(chat_id, message_text)
            
@bot.message_handler(commands=['period'])
def period_report_handler(message):
    """Обработчик команды /period - отчет за период"""
    chat_id = message.chat.id

    # Инициализируем состояние пользователя для выбора диапазона дат
    user_states[chat_id] = UserState.WAITING_FOR_START_DATE

    # Показываем календарь для выбора начальной даты
    markup = create_calendar_markup()
    bot.send_message(
        chat_id,
        "Пожалуйста, выберите *начальную* дату периода:",
        reply_markup=markup,
        parse_mode='Markdown'
    )
@bot.message_handler(func=lambda message: message.reply_to_message and
                      "Распознанные показания" in message.reply_to_message.text)
def save_meter_reading(message):
    chat_id = message.chat.id
    if message.text.lower() in ['да', 'yes', 'сохранить']:
        # Здесь логика сохранения данных
        bot.send_message(
            chat_id,
            "Данные успешно сохранены!",
            reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add('📷 Распознать счётчик')
        )
    else:
        bot.send_message(
            chat_id,
            "Данные не сохранены.",
            reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add('📷 Распознать счётчик')
        )

        @bot.message_handler(content_types=['text'])
        def text_handler(message):
            if message.text == '📷 Распознать счётчик':
                request_meter_photo(message)
            # остальные обработчики...

@bot.callback_query_handler(func=lambda call: call.data.startswith('date_'))
def date_callback_handler(call):
    def date_callback_handler(call):
        chat_id = call.message.chat.id
        date_str = call.data.replace('date_', '')
        
        try:
            selected_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Загружаем данные
            equipment_consumption, filtered_data = load_and_filter_data(selected_date)
            
            if equipment_consumption is None:
                bot.send_message(chat_id, f"❌ Не найдено данных за {selected_date.strftime('%d.%m.%Y')}")
                return
            
            # Отчет текстом
            report = f"📅 Отчет за {selected_date.strftime('%d.%m.%Y')}\n"
            for equipment, consumption in equipment_consumption.items():
                report += f"{equipment}: {consumption} кВт·ч\n"
            bot.send_message(chat_id, report)
            
            # Строим график
            plt.figure(figsize=(10, 6))
            for equipment in filtered_data['equipment'].unique():
                subset = filtered_data[filtered_data['equipment'] == equipment]
                plt.plot(subset['date'], subset['consumption'], label=equipment)
                
            plt.xlabel('Время')
            plt.ylabel('Потребление (кВт·ч)')
            plt.title(f'График потребления за {selected_date.strftime("%d.%m.%Y")}')
            plt.legend()
            plt.grid(True)
            
            # Сохраняем график во временный файл
            plot_path = f"plot_{chat_id}.png"
            plt.tight_layout()
            plt.savefig(plot_path)
            plt.close()
            
            # Отправляем график
            with open(plot_path, 'rb') as photo:
                bot.send_photo(chat_id, photo)
                
            # Удаляем файл
            os.remove(plot_path)
            
        except ValueError:
            bot.answer_callback_query(call.id, "Ошибка обработки даты. Попробуйте снова.")
            
    try:
        chat_id = call.message.chat.id
        date_str = call.data.replace('date_', '')

        try:
            selected_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError as e:
            logger.error(f"Ошибка парсинга даты: {e}")
            bot.answer_callback_query(call.id, "Ошибка обработки даты. Попробуйте снова.")
            return

        if chat_id not in user_states:
            bot.answer_callback_query(call.id, "Сессия истекла. Начните заново.")
            return

        state = user_states[chat_id]

        if state == UserState.WAITING_FOR_START_DATE:
            user_states[chat_id] = {
                'state': UserState.WAITING_FOR_END_DATE,
                'start_date': selected_date
            }

            markup = create_calendar_markup()
            bot.edit_message_text(
                f"Начальная дата: {selected_date.strftime('%d.%m.%Y')}\n"
                "Теперь выберите *конечную* дату периода:",
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='Markdown'
            )

        elif isinstance(state, dict) and state['state'] == UserState.WAITING_FOR_END_DATE:
            start_date = state['start_date']
            end_date = selected_date

            if end_date < start_date:
                bot.answer_callback_query(
                    call.id,
                    "⚠️ Конечная дата не может быть раньше начальной!",
                    show_alert=True
                )
                return

            user_states[chat_id] = {
                'state': UserState.WAITING_FOR_EQUIPMENT,
                'start_date': start_date,
                'end_date': end_date
            }

            try:
                monitor = EnergyMonitor(DATA_FILE)
                stats = monitor.load_data(start_date=start_date, end_date=end_date)

                if stats is None or stats.empty:
                    bot.send_message(chat_id, "❌ Данные об оборудовании отсутствуют")
                    return

                # Get equipment names from DataFrame columns (excluding date/time columns)
                equipment_list = [col for col in stats.columns
                                  if col not in ['date', 'time', 'datetime', 'date', 'time', 'Дата', 'Время']]

                if not equipment_list:
                    bot.send_message(chat_id, "❌ В данных не обнаружено оборудования")
                    return

                # Create keyboard with equipment buttons
                markup = types.InlineKeyboardMarkup(row_width=1)
                for equipment in sorted(equipment_list):
                    button = types.InlineKeyboardButton(
                        text=equipment,
                        callback_data=f"period_equipment_{equipment}"
                    )
                    markup.add(button)

                markup.add(types.InlineKeyboardButton(
                    text="📊 Все оборудование",
                    callback_data="period_equipment_ALL"
                ))

                bot.edit_message_text(
                    f"📅 Выбран период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n"
                    "🔧 Выберите оборудование для анализа:",
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup
                )

            except Exception as e:
                logger.error(f"Ошибка при загрузке оборудования: {e}", exc_info=True)
                bot.send_message(chat_id, "⚠️ Произошла ошибка при загрузке списка оборудования")
                user_states.pop(chat_id, None)

    except Exception as e:
        logger.error(f"Ошибка в date_callback_handler: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "⚠️ Произошла непредвиденная ошибка")


@bot.callback_query_handler(func=lambda call: call.data.startswith('period_equipment_'))
def period_equipment_callback_handler(call):
    """Обработчик выбора оборудования для отчета за период"""
    chat_id = call.message.chat.id
    equipment = call.data.replace('period_equipment_', '')

    # Проверяем, что у пользователя есть сохраненные даты
    if chat_id in user_states and isinstance(user_states[chat_id], dict):
        state = user_states[chat_id]
        start_date = state.get('start_date')
        end_date = state.get('end_date')

        if start_date and end_date:
            try:
                # Создаем и отправляем отчет за период
                monitor = EnergyMonitor(DATA_FILE)
                results = monitor.analyze_period_consumption(start_date, end_date)

                if equipment in results:
                    # Генерируем отчет
                    report = generate_period_report(equipment, results[equipment], start_date, end_date)
                    bot.send_message(chat_id, report)

                    # Генерируем и отправляем график
                    chart_image = monitor.generate_consumption_chart(equipment, start_date, end_date)
                    bot.send_photo(
                        chat_id,
                        chart_image,
                        caption=f"График потребления для {equipment} за выбранный период"
                    )
                else:
                    bot.send_message(
                        chat_id,
                        f"Данные по оборудованию {equipment} за выбранный период отсутствуют"
                    )

                # Очищаем состояние
                user_states.pop(chat_id, None)

            except Exception as e:
                logger.error(f"Ошибка при формировании отчета за период: {e}")
                bot.send_message(chat_id, "Произошла ошибка при формировании отчета")
                user_states.pop(chat_id, None)
        else:
            bot.send_message(chat_id, "Ошибка: период не выбран")
            user_states.pop(chat_id, None)
    else:
        bot.send_message(chat_id, "Сначала выберите период. Используйте команду /period")


@bot.message_handler(commands=['chart'])
def chart_handler(message):
    """Обработчик команды /chart - график потребления"""
    chat_id = message.chat.id

    # Показываем список оборудования для выбора
    try:
        monitor = EnergyMonitor(DATA_FILE)
        if not monitor.equipment_stats:
            bot.send_message(chat_id, "Данные об оборудовании отсутствуют")
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for equipment in monitor.equipment_stats.keys():
            button = types.InlineKeyboardButton(
                text=equipment,
                callback_data=f"chart_{equipment}"
            )
            markup.add(button)

        bot.send_message(
            chat_id,
            "Выберите оборудование для просмотра графика потребления:",
            reply_markup=markup
        )

    except Exception as e:
        logger.error(f"Ошибка при подготовке выбора оборудования: {e}")
        bot.send_message(chat_id, "Произошла ошибка при подготовке графиков")


@bot.callback_query_handler(func=lambda call: call.data.startswith('calendar_'))
def calendar_navigation_handler(call):
    """Обработчик навигации по календарю"""
    chat_id = call.message.chat.id
    action, year, month = call.data.split('_')[1:]
    year, month = int(year), int(month)

    # Определяем следующий или предыдущий месяц
    if action == 'prev':
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == 'next':
        month += 1
        if month > 12:
            month = 1
            year += 1

    # Обновляем календарь
    markup = create_calendar_markup(year, month)

    # Определяем текст сообщения в зависимости от состояния
    if chat_id in user_states:
        state = user_states[chat_id]
        if state == UserState.WAITING_FOR_START_DATE:
            text = "Пожалуйста, выберите *начальную* дату периода:"
        elif isinstance(state, dict) and state['state'] == UserState.WAITING_FOR_END_DATE:
            start_date = state['start_date'].strftime('%d.%m.%Y')
            text = f"Начальная дата: {start_date}\nТеперь выберите *конечную* дату периода:"
        else:
            text = "Выберите дату:"
    else:
        text = "Выберите дату:"

    bot.edit_message_text(
        text,
        chat_id,
        call.message.message_id,
        reply_markup=markup,
        parse_mode='Markdown'
    )


@bot.callback_query_handler(func=lambda call: call.data == 'cancel_date_selection')
def cancel_date_selection_handler(call):
    """Обработчик отмены выбора даты"""
    chat_id = call.message.chat.id

    # Очищаем состояние пользователя
    user_states.pop(chat_id, None)

    bot.edit_message_text(
        "Выбор даты отменен.",
        chat_id,
        call.message.message_id
    )


@bot.callback_query_handler(func=lambda call: call.data == 'ignore')
def ignore_callback_handler(call):
    """Обработчик для 'пустых' кнопок в календаре"""
    bot.answer_callback_query(call.id)


def generate_consumption_chart(equipment_name: str, start_date=None, end_date=None,
                               data_period: int = 7) -> io.BytesIO:
    """Функция-делегат для вызова метода EnergyMonitor"""
    monitor = EnergyMonitor(DATA_FILE)
    return monitor.generate_consumption_chart(equipment_name, start_date, end_date, data_period)


@bot.message_handler(content_types=['text'])
def text_handler(message):
    """Обработчик текстовых сообщений"""
    chat_id = message.chat.id
    text = message.text

    if text == '📊 Дневной отчет':
        report_handler(message)
    elif text == '📈 Графики потребления':
        chart_handler(message)
    elif text == '⚠️ Проверить аномалии':
        check_anomalies_handler(message)
    elif text == '📅 Отчет за период':
        period_report_handler(message)
    elif text == 'ℹ️ Информация':
        bot.send_message(
            chat_id,
            "Система мониторинга энергопотребления\n\n"
            "Команды:\n"
            "/start - Начать работу с ботом\n"
            "/report - Получить ежедневный отчет\n"
            "/check - Проверить наличие аномалий\n"
            "/chart - Построить график потребления\n"
            "/period - Отчет за выбранный период\n\n"
            "Бот автоматически отправляет ежедневные отчеты "
            "и оповещения об аномалиях в режиме реального времени."
        )
    else:
        bot.send_message(
            chat_id,
            "Воспользуйтесь кнопками меню для взаимодействия с ботом"
        )


# Функции для запланированных задач
def send_daily_reports():
    """Отправка ежедневных отчетов всем подписанным пользователям"""
    chat_ids = get_chat_ids()
    if not chat_ids:
        logger.warning("Нет подписанных пользователей для отправки отчетов")
        return

    try:
        monitor = EnergyMonitor(DATA_FILE)
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        results = monitor.analyze_daily_consumption(yesterday)

        if not results:
            logger.warning(f"Данные за {yesterday.date()} отсутствуют")
            return

        # Отправляем отчет по каждой установке всем пользователям
        for chat_id in chat_ids:
            for equipment, data in results.items():
                report = generate_daily_report(equipment, data)
                bot.send_message(chat_id, report)

                # Добавляем задержку чтобы избежать лимитов API
                time.sleep(0.1)

    except Exception as e:
        logger.error(f"Ошибка при отправке ежедневных отчетов: {e}")


def check_realtime_anomalies():
    """Проверка аномалий в реальном времени"""
    chat_ids = get_chat_ids()
    if not chat_ids:
        logger.warning("Нет подписанных пользователей для отправки уведомлений")
        return

    try:
        monitor = EnergyMonitor(DATA_FILE)
        anomalies = monitor.check_anomalies()

        if not anomalies:
            return

        # Отправляем предупреждения о найденных аномалиях всем пользователям
        for chat_id in chat_ids:
            for anomaly in anomalies:
                alert = generate_anomaly_alert(anomaly)
                bot.send_message(chat_id, alert, parse_mode='Markdown')

                # Добавляем задержку чтобы избежать лимитов API
                time.sleep(0.1)

    except Exception as e:
        logger.error(f"Ошибка при проверке аномалий в реальном времени: {e}")


# Планировщик задач
def setup_schedulers():
    """Настройка планировщика задач"""
    # Ежедневный отчет в 8:00
    schedule.every().day.at("08:00").do(send_daily_reports)

    # Проверка аномалий каждые 30 минут
    schedule.every(30).minutes.do(check_realtime_anomalies)

    # Еженедельный отчет в понедельник в 9:00
    schedule.every().monday.at("09:00").do(send_weekly_report)

    logger.info("Планировщик задач настроен")


def send_weekly_report():
    """Отправка еженедельных отчетов всем подписанным пользователям"""
    chat_ids = get_chat_ids()
    if not chat_ids:
        logger.warning("Нет подписанных пользователей для отправки еженедельных отчетов")
        return

    try:
        monitor = EnergyMonitor(DATA_FILE)
        today = datetime.datetime.now().date()
        end_date = today - datetime.timedelta(days=1)  # Вчерашний день
        start_date = end_date - datetime.timedelta(days=6)  # Неделя назад

        results = monitor.analyze_period_consumption(start_date, end_date)

        if not results:
            logger.warning(f"Данные за период с {start_date} по {end_date} отсутствуют")
            return

        # Отправляем отчет по каждой установке всем пользователям
        for chat_id in chat_ids:
            # Сначала отправляем общий заголовок
            bot.send_message(
                chat_id,
                f"📅 *Еженедельный отчет*\n"
                f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n",
                parse_mode='Markdown'
            )

            for equipment, data in results.items():
                report = generate_period_report(equipment, data, start_date, end_date)
                bot.send_message(chat_id, report)

                # Генерируем и отправляем график
                chart_image = monitor.generate_consumption_chart(equipment, start_date, end_date)
                bot.send_photo(
                    chat_id,
                    chart_image,
                    caption=f"График потребления для {equipment} за прошедшую неделю"
                )

                # Добавляем задержку чтобы избежать лимитов API
                time.sleep(0.2)

    except Exception as e:
        logger.error(f"Ошибка при отправке еженедельных отчетов: {e}")


def run_schedulers():
    """Запуск планировщика задач"""
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    # Загружаем данные при запуске
    monitor = EnergyMonitor(DATA_FILE)

    # Настраиваем планировщик
    setup_schedulers()

    # Запускаем планировщик в отдельном потоке
    import threading

    scheduler_thread = threading.Thread(target=run_schedulers)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Запускаем бота
    logger.info("Бот запущен")
    bot.polling(none_stop=True)