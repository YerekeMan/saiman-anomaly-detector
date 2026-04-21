import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
import requests
import os
from dotenv import load_dotenv
load_dotenv()
def send_telegram_alert(message):
    token = os.getenv("TG_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Ошибка отправки в ТГ: {e}")

engine = create_engine(os.getenv("DB_URL"))
from_date=datetime.now().date()-timedelta(days=180)
m_limit=50

list_query = f"""
SELECT ns.device_name, COUNT(*) as readings
FROM network_server_meter ns
JOIN ns_active_energy na ON ns.id=na.network_server_meter_id
WHERE na.fixed_at >= '{from_date}'
GROUP BY ns.device_name
ORDER BY readings DESC
LIMIT {m_limit}
"""

THRESHOLD_PERCENT = 0.10

try:
    meters_df = pd.read_sql_query(list_query, engine)

    for index, row in meters_df.iterrows():
        device = row['device_name']
        data_query = f"""
                SELECT DATE(na.fixed_at) as day, MAX(na.total) as daily_total
                FROM network_server_meter ns
                JOIN ns_active_energy na ON ns.id=na.network_server_meter_id
                WHERE ns.device_name = '{device}' 
                  AND na.fixed_at >= '{from_date}'
                  AND na.type = 'daily-plus'
                GROUP BY day
                ORDER BY day ASC
                """
        df = pd.read_sql_query(data_query, engine)
        df['day'] = pd.to_datetime(df['day'])
        today_date = df['day'].max()
        if today_date.date() < datetime.now().date():
            print(f"[{device}] Сегодня данных еще не было. Пропускаем.")
            continue

        # 1. Отделяем "сегодня" от "прошлого"
        df_today = df[df['day'] == today_date].iloc[0]
        df_history = df[df['day'] < today_date].copy()

        if len(df_history) < 10:
            continue

        # 2. Готовим данные для обучения модели
        df_history['yesterday'] = df_history['daily_total'].shift(1)
        df_history['days_passed'] = df_history['day'].diff().dt.days
        df_train = df_history.dropna()

        X = df_train[['yesterday', 'days_passed']]
        y = df_train['daily_total']

        model = LinearRegression()
        model.fit(X, y)

        # 3. Делаем прогноз на сегодня
        last_history_total = df_history['daily_total'].iloc[-1]
        days_since_last = (today_date - df_history['day'].iloc[-1]).days

        X_today = pd.DataFrame([[last_history_total, days_since_last]],
                               columns=['yesterday', 'days_passed'])
        prediction_today = model.predict(X_today)[0]

        # 4. Сравниваем факт и прогноз
        actual_today = df_today['daily_total']
        diff = abs(actual_today - prediction_today)
        error_margin = prediction_today * THRESHOLD_PERCENT

        print(f"--- Проверка {device} ---")
        if diff > error_margin:
            alert_msg = (f"⚠️ АНОМАЛИЯ: {device}\n"
                         f"Ожидали: {prediction_today:.2f}\n"
                         f"Пришло: {actual_today}\n"
                         f"Разница: {diff:.2f}")
            print(alert_msg)
            send_telegram_alert(alert_msg)
        else:
            print(f"✅ В норме. Прогноз: {prediction_today:.2f}, Факт: {actual_today}")

except Exception as e:
    print(f"Ошибка: {e}")

