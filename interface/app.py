import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Use seaborn style for nicer plots
sns.set_style("whitegrid")

# Kafka config
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "flights")
}

# PostgreSQL config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "satisfaction_db"),
    "user": os.getenv("DB_USER", "satisfaction_user"),
    "password": os.getenv("DB_PASSWORD", "satisfaction_pass")
}

def load_file(uploaded_file):
    try:
        df = pd.read_csv(uploaded_file)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        df['flight_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            producer.send(
                topic,
                value={
                    "id": row['flight_id'],
                    "data": row.drop('flight_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Initialize session state
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# UI
st.title("🛫 Airline Passenger Satisfaction Scoring")
st.markdown("Загрузите данные о полётах, отправьте в систему и просмотрите аналитику предсказаний.")

uploaded_file = st.file_uploader(
    "Загрузите CSV файл с данными о пассажирах и полётах",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

if st.session_state.uploaded_files:
    st.subheader("🗂 Загруженные файлы")
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}` | **Статус:** `{file_data['status']}`")
        with cols[2]:
            if st.button(f"Отправить", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()

# === Enhanced Analytics Section ===
st.markdown("---")
st.subheader("📊 Аналитика предсказаний")

if st.button("Обновить аналитику"):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        

        st.markdown("### 📈 Распределение всех скоров")
        df_scores = pd.read_sql_query("SELECT score FROM scoring_results", conn)
        if not df_scores.empty:
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.hist(df_scores['score'], bins=50, color='steelblue', edgecolor='black', alpha=0.8)
            ax.set_xlabel('Вероятность удовлетворённости')
            ax.set_ylabel('Частота')
            ax.set_title('Гистограмма всех предсказанных скоров')
            st.pyplot(fig)
        else:
            st.info("Нет данных для гистограммы.")


        df_full = pd.read_sql_query("""
            SELECT 
                satisfaction_flag,
                gender,
                class,
                customer_type,
                type_of_travel
            FROM scoring_results
            WHERE gender IS NOT NULL AND class IS NOT NULL
        """, conn)
        conn.close()

        if df_full.empty:
            st.warning("Нет данных с признаками (Gender, Class) в базе. Обновите scoring-db-writer.")
        else:
            st.markdown("### 👥 Удовлетворённость по полу")
            gender_stats = df_full.groupby('gender')['satisfaction_flag'].agg(
                mean='mean',
                count='size'
            ).reset_index()
            gender_stats['mean_pct'] = gender_stats['mean'] * 100

            fig, ax = plt.subplots(figsize=(6, 4))
            bars = ax.bar(gender_stats['gender'], gender_stats['mean_pct'], color=['skyblue', 'salmon'])
            ax.set_ylabel('Доля удовлетворённых (%)')
            ax.set_title('Удовлетворённость пассажиров по полу')
            ax.set_ylim(0, 100)
            for bar in bars:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                        f'{bar.get_height():.1f}%', ha='center', va='bottom')
            st.pyplot(fig)

            st.markdown("### 🎟 Удовлетворённость по классу обслуживания")
            class_order = ['Business', 'Eco Plus', 'Eco']
            class_stats = df_full.groupby('class')['satisfaction_flag'].mean().reindex(class_order, fill_value=0) * 100

            fig, ax = plt.subplots(figsize=(7, 4))
            bars = ax.bar(class_stats.index, class_stats.values, color='lightgreen')
            ax.set_ylabel('Доля удовлетворённых (%)')
            ax.set_title('Удовлетворённость по классу полёта')
            ax.set_ylim(0, 100)
            for bar in bars:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                        f'{bar.get_height():.1f}%', ha='center', va='bottom')
            st.pyplot(fig)

            st.markdown("### 💼 Удовлетворённость по типу клиента")
            cust_stats = df_full.groupby('customer_type')['satisfaction_flag'].mean() * 100
            fig, ax = plt.subplots(figsize=(6, 3))
            bars = ax.bar(cust_stats.index, cust_stats.values, color='orchid')
            ax.set_ylabel('Доля удовлетворённых (%)')
            ax.set_title('Лояльные vs Нелояльные клиенты')
            ax.set_ylim(0, 100)  
            for bar in bars:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 1,
                    f'{bar.get_height():.1f}%',
                    ha='center',
                    va='bottom',
                )
            st.pyplot(fig)

    except Exception as e:
        st.error(f"Ошибка загрузки данных из БД: {str(e)}")