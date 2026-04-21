import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
import requests
import os
from dotenv import load_dotenv
import time

# Загружаем переменные
load_dotenv()


def send_telegram_alert(message):
    token = os.getenv("TG_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Ошибка отправки в ТГ: {e}")


def run_anomaly_detection():
    """Основная функция, которую можно вызвать вручную или по расписанию"""
    print(f"\n🚀 [{datetime.now()}] ЗАПУСК ПРОВЕРКИ АНОМАЛИЙ...")

    db_url = os.getenv("DB_URL")
    if not db_url:
        print("Ошибка: DB_URL не найден.")
        return

    engine = create_engine(db_url)
    from_date = datetime.now().date() - timedelta(days=180)
    m_limit = 50
    THRESHOLD_PERCENT = 0.10

    list_query = f"""
    SELECT ns.device_name, COUNT(*) as readings
    FROM network_server_meter ns
    JOIN ns_active_energy na ON ns.id=na.network_server_meter_id
    WHERE na.fixed_at >= '{from_date}'
    GROUP BY ns.device_name
    ORDER BY readings DESC
    LIMIT {m_limit}
    """

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
                      AND na.total > 0
                    GROUP BY day
                    ORDER BY day ASC
                    """
            df = pd.read_sql_query(data_query, engine)
            if df.empty: continue

            df['day'] = pd.to_datetime(df['day'])
            today_date = df['day'].max()

            if today_date.date() < datetime.now().date():
                print(f"[{device}] Данных за сегодня ещё нет.")
                continue

            df_today = df[df['day'] == today_date].iloc[0]
            df_history = df[df['day'] < today_date].copy()

            if len(df_history) < 10: continue

            df_history['yesterday'] = df_history['daily_total'].shift(1)
            df_history['days_passed'] = df_history['day'].diff().dt.days
            df_train = df_history.dropna()

            X = df_train[['yesterday', 'days_passed']]
            y = df_train['daily_total']

            model = LinearRegression().fit(X, y)

            last_total = df_history['daily_total'].iloc[-1]
            days_since = (today_date - df_history['day'].iloc[-1]).days
            X_today = pd.DataFrame([[last_total, days_since]], columns=['yesterday', 'days_passed'])

            prediction = model.predict(X_today)[0]
            actual = df_today['daily_total']
            diff = abs(actual - prediction)

            if diff > (prediction * THRESHOLD_PERCENT):
                msg = f"⚠️ АНОМАЛИЯ: {device}\nОжидали: {prediction:.2f}\nПришло: {actual}\nРазница: {diff:.2f}"
                print(f"!!! {msg}")
                send_telegram_alert(msg)
            else:
                print(f"✅ {device}: OK")

        print(f"🏁 [{datetime.now()}] Проверка завершена успешно.")

    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    TARGET_HOUR = 21  # Время запуска (9 вечера)
    print(f"Бот Saiman запущен. Плановая проверка в {TARGET_HOUR}:00")

    while True:
        now = datetime.now()
        target_time = now.replace(hour=TARGET_HOUR, minute=0, second=0, microsecond=0)

        if now >= target_time:
            target_time += timedelta(days=1)

        wait_seconds = (target_time - now).total_seconds()
        print(f"Следующий запуск по расписанию: {target_time} (через {wait_seconds / 3600:.1f} ч.)")

        time.sleep(wait_seconds)
        run_anomaly_detection()