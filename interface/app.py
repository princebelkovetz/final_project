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


sns.set_style("whitegrid")


KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "flights")
}


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


if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}


st.title("🛫 Airline Passenger Satisfaction Scoring")
st.markdown("Загрузите данные о полётах, отправьте в систему и просмотрите аналитику предсказаний.")


uploaded_file = st.file_uploader(
    "Загрузите CSV файл с данными о пассажирах и полётах",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    df = load_file(uploaded_file)
    if df is not None:
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": df
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
                    with st.spinner("Отправка в Kafka..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()

st.sidebar.markdown("### 🔍 Фильтры для аналитики")


try:
    conn = psycopg2.connect(**DB_CONFIG)
    filter_df = pd.read_sql_query("""
        SELECT DISTINCT gender, class, customer_type 
        FROM scoring_results 
        WHERE gender IS NOT NULL AND class IS NOT NULL AND customer_type IS NOT NULL
    """, conn)
    conn.close()

    gender_options = sorted(filter_df['gender'].dropna().unique())
    class_options = sorted(filter_df['class'].dropna().unique())
    cust_options = sorted(filter_df['customer_type'].dropna().unique())
except Exception as e:
    st.sidebar.error(f"Не удалось загрузить фильтры: {e}")
    gender_options = ["Male", "Female"]
    class_options = ["Business", "Eco Plus", "Eco"]
    cust_options = ["Loyal Customer", "disloyal Customer"]

selected_genders = st.sidebar.multiselect("Пол", gender_options, default=gender_options)
selected_classes = st.sidebar.multiselect("Класс", class_options, default=class_options)
selected_customer_types = st.sidebar.multiselect("Тип клиента", cust_options, default=cust_options)


age_min, age_max = 18, 80
try:
    conn = psycopg2.connect(**DB_CONFIG)
    age_df = pd.read_sql_query("SELECT MIN(age) as min_age, MAX(age) as max_age FROM scoring_results WHERE age IS NOT NULL", conn)
    conn.close()
    if not age_df.empty and pd.notnull(age_df.iloc[0]['min_age']):
        age_min = int(age_df.iloc[0]['min_age'])
        age_max = int(age_df.iloc[0]['max_age'])
except Exception as e:
    st.sidebar.warning(f"Не удалось определить диапазон возраста: {e}")

age_range = st.sidebar.slider("Возраст", age_min, age_max, (age_min, age_max))


st.markdown("---")
st.subheader("📊 Аналитика предсказаний")

if st.button("🔄 Обновить аналитику"):
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


        where_parts = []
        if selected_genders:
            genders_str = ",".join([f"'{g}'" for g in selected_genders])
            where_parts.append(f"gender IN ({genders_str})")
        if selected_classes:
            classes_str = ",".join([f"'{c}'" for c in selected_classes])
            where_parts.append(f"class IN ({classes_str})")
        if selected_customer_types:
            custs_str = ",".join([f"'{c}'" for c in selected_customer_types])
            where_parts.append(f"customer_type IN ({custs_str})")
        
        where_parts.append(f"age BETWEEN {age_range[0]} AND {age_range[1]}")
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        query = f"""
            SELECT 
                satisfaction_flag,
                gender,
                class,
                customer_type,
                type_of_travel,
                age
            FROM scoring_results
            WHERE {where_clause}
              AND gender IS NOT NULL 
              AND class IS NOT NULL
              AND customer_type IS NOT NULL
              AND age IS NOT NULL
        """

        df_full = pd.read_sql_query(query, conn)
        conn.close()

        if df_full.empty:
            st.warning("Нет данных, соответствующих выбранным фильтрам.")
        else:
            total = len(df_full)
            satisfied = df_full['satisfaction_flag'].sum()
            satisfaction_rate = satisfied / total * 100

            col1, col2, col3 = st.columns(3)
            col1.metric("Всего записей", f"{total:,}")
            col2.metric("Удовлетворены", f"{satisfied:,}")
            col3.metric("Общий уровень удовл.", f"{satisfaction_rate:.1f}%")


            st.download_button(
                label="📥 Скачать отфильтрованные данные",
                data=df_full.to_csv(index=False),
                file_name="satisfaction_analytics_filtered.csv",
                mime="text/csv"
            )


            st.markdown("### 👶🧑👵 Удовлетворённость по возрастным группам")
            bins = [0, 25, 40, 60, 100]
            labels = ['<25', '25–39', '40–59', '60+']
            df_full['age_group'] = pd.cut(df_full['age'], bins=bins, labels=labels, right=False)


            age_satisfaction = df_full.groupby('age_group')['satisfaction_flag'].agg(
                satisfaction_rate='mean',
                count='size'
            ).reset_index()
            age_satisfaction['satisfaction_rate_pct'] = age_satisfaction['satisfaction_rate'] * 100


            age_satisfaction = age_satisfaction.set_index('age_group').reindex(labels, fill_value=0).reset_index()

            fig, ax = plt.subplots(figsize=(8, 4.5))
            bars = ax.bar(age_satisfaction['age_group'], age_satisfaction['satisfaction_rate_pct'], color='mediumseagreen')
            ax.set_ylabel('Доля удовлетворённых (%)')
            ax.set_title('Удовлетворённость пассажиров по возрастным группам')
            ax.set_ylim(0, 100)
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, height + 1,
                        f'{height:.1f}%', ha='center', va='bottom')
            st.pyplot(fig)


            st.markdown("### 👥 Удовлетворённость по полу")
            gender_stats = df_full.groupby('gender')['satisfaction_flag'].agg(
                mean='mean', count='size'
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
            available_classes = [c for c in class_order if c in df_full['class'].unique()]
            class_stats = df_full.groupby('class')['satisfaction_flag'].mean().reindex(available_classes, fill_value=0) * 100

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


            st.markdown("### 📊 Удовлетворённость: Класс × Пол")
            heatmap_data = df_full.pivot_table(
                values='satisfaction_flag',
                index='class',
                columns='gender',
                aggfunc='mean'
            ).fillna(0) * 100

            if not heatmap_data.empty:
                fig, ax = plt.subplots(figsize=(8, 4))
                sns.heatmap(
                    heatmap_data,
                    annot=True,
                    fmt=".1f",
                    cmap="coolwarm",
                    vmin=0,
                    vmax=100,
                    cbar_kws={'label': 'Доля удовлетворённых (%)'},
                    ax=ax
                )
                ax.set_title("Доля удовлетворённых (%) по комбинации класса и пола")
                st.pyplot(fig)
            else:
                st.info("Недостаточно данных для heatmap.")

    except Exception as e:
        st.error(f"Ошибка загрузки данных из БД: {str(e)}")