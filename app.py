import streamlit as st
import pandas as pd
import io
from hdfs import InsecureClient  

# -------------------- CONFIGURATION --------------------
st.set_page_config(
    page_title="NYC Taxi Chatbot 🚖",
    page_icon="🚖",
    layout="wide",
)

# -------------------- CUSTOM STYLING --------------------
st.markdown("""
    <style>
        .sidebar .sidebar-content {
            background-color: #F0F0F0;
        }
        .stButton>button {
            background-color: #FFD700;
            color: black;
            font-weight: bold;
            border-radius: 5px;
            padding: 10px 15px;
        }
        .stMarkdown {
            font-size: 16px;
            font-family: Arial, sans-serif;
        }
    </style>
""", unsafe_allow_html=True)

# -------------------- SETUP HDFS CONNECTION --------------------
HDFS_URL = "http://node-master:9870"  # Ensure this is correct
HDFS_PATH = "/data/nyc_taxi_2_months_csv"  # Path to CSV files in HDFS
client = InsecureClient(HDFS_URL, user="supakin")

# -------------------- SIDEBAR --------------------
with st.sidebar:
    st.title("NYC Taxi Data Analytics 🚖")

    st.markdown("### **Project Details**")
    st.markdown(
        """
        **NYC Taxi Chatbot** helps analyze taxi & limousine data from **2023 ONLY**.  
        Data is sourced from NYC's **Taxi & Limousine Commission (TLC)**.

        **Key Features:**
        - 🚖 Get insights on taxi pickups & fares.
        - 📊 Analyze ride trends from 2023.
        - 🔎 Ask about locations, peak hours, and more.

        **📂 Data Source:**  
        [NYC TLC Official Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
        """
    )

    st.warning("📌 **All responses are based strictly on 2023 data**.")  
    st.divider()

    # -------------------- USER INPUT: NUMBER OF MONTHS --------------------
    num_months = st.slider("Select number of months to load:", 1, 2, 2)  # 1-2 months

# -------------------- LOAD CSV FILES FROM HDFS --------------------
st.markdown("### 📊 NYC Taxi Data Insights (2023)")
st.markdown(f"**Showing data for the last {num_months} months**.")

try:
    # Get all CSV files
    all_files = sorted(client.list(HDFS_PATH))  

    # Load only the requested number of months
    selected_files = all_files[:num_months * 4]  # 4 files per month (yellow, green, fhv, fhvhv)

    if selected_files:
        df_list = []
        for file in selected_files:
            with client.read(f"{HDFS_PATH}/{file}") as reader:
                csv_data = io.BytesIO(reader.read())  
                df = pd.read_csv(csv_data)  
                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        # Ensure required columns exist before plotting
        if "pickup_datetime" in df.columns and "fare_amount" in df.columns:
            st.markdown(f"#### 🚖 Taxi Rides by Hour (Last {num_months} Months)")
            df["pickup_hour"] = pd.to_datetime(df["pickup_datetime"]).dt.hour

            fig = df.groupby("pickup_hour")["fare_amount"].mean().plot(kind="bar")
            st.pyplot(fig.figure)

            st.write(df.describe())
        else:
            st.warning("⚠️ **Missing 'pickup_datetime' or 'fare_amount' column in dataset.** Check CSV files.")

    else:
        st.error("🚨 **No CSV files found in HDFS!** Please check HDFS directory path.")

except Exception as e:
    st.error(f"❌ **Error loading CSV files from HDFS:** {str(e)}")