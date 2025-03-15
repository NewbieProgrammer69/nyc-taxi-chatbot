import streamlit as st
import requests
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
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
HDFS_PATH = "/data/nyc_taxi_2023_clean"  # Path to Parquet files in HDFS
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
    num_months = st.slider("Select number of months to load:", 1, 12, 3)  # Default to 3 months

# -------------------- LOAD PARQUET FILES FROM HDFS --------------------
st.markdown("### 📊 NYC Taxi Data Insights (2023)")
st.markdown("Below are some key insights into the taxi rides in NYC for 2023.")

try:
    # Get all Parquet files
    all_files = client.list(HDFS_PATH)
    
    # Sort files by month and load only the selected number of months
    selected_files = sorted(all_files)[:num_months]  

    if selected_files:
        df_list = []
        for file in selected_files:
            with client.read(f"{HDFS_PATH}/{file}") as reader:
                parquet_data = io.BytesIO(reader.read())  
                df = pd.read_parquet(parquet_data, engine="fastparquet")  
                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        if "hour" in df.columns and "fare_amount" in df.columns:
            st.markdown(f"#### 🚖 Taxi Rides by Hour (Last {num_months} Months)")
            fig, ax = plt.subplots()
            df.groupby("hour")["fare_amount"].mean().plot(kind="bar", ax=ax)
            st.pyplot(fig)

            st.write(df.describe())
        else:
            st.warning("⚠️ **Missing 'hour' or 'fare_amount' column in the dataset.** Check Parquet files.")

    else:
        st.error("🚨 **No Parquet files found in HDFS!** Please check HDFS directory path.")

except Exception as e:
    st.error(f"❌ **Error loading Parquet files from HDFS:** {str(e)}")