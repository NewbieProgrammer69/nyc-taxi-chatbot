import streamlit as st
import requests
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import io
import pyarrow.parquet as pq
from hdfs import InsecureClient  # HDFS Client

# -------------------- CONFIGURATION --------------------
st.set_page_config(
    page_title="NYC Taxi Chatbot 🚖",
    page_icon="🚖",
    layout="wide",
)

# -------------------- HDFS CONFIG --------------------
HDFS_URL = "http://node-master:9870"  # WebHDFS Port (Ensure this is correct)
HDFS_PATH = "/data/nyc_taxi_2023_clean"  # Path to Parquet files in HDFS
client = InsecureClient(HDFS_URL, user="supakin")

# -------------------- FUNCTION TO LOAD CHAT HISTORY --------------------
CHAT_HISTORY_FILE = "chat_history.json"

def save_chat_history():
    with open(CHAT_HISTORY_FILE, "w") as f:
        json.dump(st.session_state.chat_history, f)

def load_chat_history():
    try:
        with open(CHAT_HISTORY_FILE, "r") as f:
            st.session_state.chat_history = json.load(f)
    except FileNotFoundError:
        st.session_state.chat_history = []

# Load chat history
if "chat_history" not in st.session_state:
    load_chat_history()

# -------------------- SIDEBAR --------------------
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/7/78/NYC_Taxi.svg", width=100)
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

    # Chat History Section
    st.markdown("### **Past Conversations**")
    for i, chat in enumerate(st.session_state.chat_history):
        if st.button(chat["title"], key=f"chat_{i}"):
            st.session_state.messages = chat["messages"]

    if st.button("🆕 Start New Chat", key="new_chat"):
        st.session_state.messages = []

    if st.button("🗑️ Clear All Chats", key="clear_history"):
        st.session_state.chat_history = []
        st.session_state.messages = []
        save_chat_history()
        st.rerun()

# -------------------- SESSION STATE --------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

# -------------------- DISPLAY MESSAGES --------------------
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=message["avatar"]):
        st.markdown(message["content"])

# -------------------- USER INPUT --------------------
if user_input := st.chat_input("Ask about NYC taxis (2023 data only)! 🚖"):
    with st.chat_message("user", avatar="🧑"):
        st.markdown(user_input)

    st.session_state.messages.append({"role": "user", "content": user_input, "avatar": "🧑"})

    # -------------------- FETCH RESPONSE FROM DEEPSEEK API --------------------
    if not (deepseek_api_key := st.secrets.get("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_API_KEY")):
        response_text = "⚠️ API key not found. Please check your environment settings."
    else:
        try:
            chat_context = [{"role": msg["role"], "content": msg["content"]} for msg in st.session_state.messages]
            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {deepseek_api_key}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "messages": chat_context},
            )
            response_text = response.json().get("choices", [{}])[0].get("message", {}).get("content", "No response.")
        except Exception as e:
            response_text = f"⚠️ Error calling DeepSeek API: {e}"

    with st.chat_message("assistant", avatar="🤖"):
        st.markdown(response_text)

    st.session_state.messages.append({"role": "assistant", "content": response_text, "avatar": "🤖"})

    st.session_state.chat_history.append({
        "title": f"{user_input[:30]}...",  
        "messages": st.session_state.messages
    })
    save_chat_history()

# -------------------- LOAD PARQUET FILES FROM HDFS --------------------
st.markdown("### 📊 NYC Taxi Data Insights (2023)")

try:
    parquet_files = client.list(HDFS_PATH)

    if parquet_files:
        df_list = []
        for file in parquet_files:
            with client.read(f"{HDFS_PATH}/{file}") as reader:
                table = pq.read_table(reader)  # Efficiently read Parquet into Arrow Table
                df_list.append(table.to_pandas())

        df = pd.concat(df_list, ignore_index=True)

        if "hour" in df.columns and "fare_amount" in df.columns:
            st.markdown("#### 🚖 Taxi Rides by Hour")
            fig, ax = plt.subplots()
            df.groupby("hour")["fare_amount"].mean().plot(kind="bar", ax=ax)
            st.pyplot(fig)
            st.write(df.describe())
        else:
            st.warning("⚠️ **Missing 'hour' or 'fare_amount' column in dataset.** Check Parquet files.")

    else:
        st.error("🚨 **No Parquet files found in HDFS!**")

except Exception as e:
    st.error(f"❌ **Error loading Parquet files from HDFS:** {str(e)}")

# -------------------- ADD FEEDBACK SYSTEM --------------------
st.markdown("### ⭐ Provide Feedback")
rating = st.radio("Was this answer helpful?", ["👍 Yes", "👎 No"], key="rating")
if rating == "👎 No":
    st.text_area("How can we improve?", key="feedback_input")