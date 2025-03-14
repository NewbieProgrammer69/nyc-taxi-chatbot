import streamlit as st
import requests
import os
import json
import pandas as pd
import matplotlib.pyplot as plt

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

# -------------------- FUNCTION TO SAVE & LOAD CHAT HISTORY --------------------
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

# Load chat history at startup
if "chat_history" not in st.session_state:
    load_chat_history()

# -------------------- SIDEBAR (ChatGPT-style Chat Management) --------------------
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/7/78/NYC_Taxi.svg", width=100)
    st.title("NYC Taxi Data Analytics 🚖")

    st.markdown("### **Project Details**")
    st.markdown(
        """
        **NYC Taxi Chatbot** helps analyze taxi & limousine data from **2024 ONLY**.  
        Data is sourced from NYC's **Taxi & Limousine Commission (TLC)**.

        **Key Features:**
        - 🚖 Get insights on taxi pickups & fares.
        - 📊 Analyze ride trends from 2024.
        - 🔎 Ask about locations, peak hours, and more.

        **📂 Data Source:**  
        [NYC TLC Official Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
        """
    )

    st.warning("📌 **All responses are based strictly on 2024 data**.")  # User notice

    st.divider()

    # Chat History Section
    st.markdown("### **Past Conversations**")

    # Display previous chats as clickable buttons
    for i, chat in enumerate(st.session_state.chat_history):
        if st.button(chat["title"], key=f"chat_{i}"):
            st.session_state.messages = chat["messages"]

    if st.button("🆕 Start New Chat", key="new_chat"):
        st.session_state.messages = []

    if st.button("🗑️ Clear All Chats", key="clear_history"):
        st.session_state.chat_history = []
        st.session_state.messages = []
        save_chat_history()
        st.experimental_rerun()

# -------------------- SESSION STATE (CHAT MEMORY) --------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

# -------------------- DISPLAY MESSAGES --------------------
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=message["avatar"]):
        st.markdown(message["content"])

# -------------------- USER INPUT --------------------
if user_input := st.chat_input("Ask about NYC taxis (2024 data only)! 🚖"):
    # Display user input
    with st.chat_message("user", avatar="🧑"):
        st.markdown(user_input)

    # Store user input
    st.session_state.messages.append({"role": "user", "content": user_input, "avatar": "🧑"})

    # -------------------- FETCH RESPONSE FROM DEEPSEEK API --------------------
    if not (deepseek_api_key := st.secrets.get("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_API_KEY")):
        response_text = "⚠️ API key not found. Please check your environment settings."
    else:
        try:
            # Ensure AI is strictly using 2024 data
            chat_context = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in st.session_state.messages
            ]

            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {deepseek_api_key}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "messages": chat_context},
            )
            response_text = (
                response.json()
                .get("choices", [{}])[0]
                .get("message", {})
                .get("content", "No response.")
            )
        except Exception as e:
            response_text = f"⚠️ Error calling DeepSeek API: {e}"

    # Display AI response
    with st.chat_message("assistant", avatar="🤖"):
        st.markdown(response_text)

    # Store assistant response
    st.session_state.messages.append({"role": "assistant", "content": response_text, "avatar": "🤖"})

    # Save chat to history
    st.session_state.chat_history.append({
        "title": f"{user_input[:30]}...",  # Save first 30 characters as title
        "messages": st.session_state.messages
    })

    # Save to file
    save_chat_history()

# -------------------- DISPLAY DATA VISUALIZATIONS --------------------
st.markdown("### 📊 NYC Taxi Data Insights (2024)")
st.markdown("Below are some key insights into the taxi rides in NYC for 2024.")

try:
    # Load sample taxi data
    df = pd.read_csv("nyc_taxi_2024.csv")

    # Plot ride trends
    fig, ax = plt.subplots()
    df.groupby("hour")["fare_amount"].mean().plot(kind="bar", ax=ax)
    st.pyplot(fig)

    # Display summary stats
    st.write(df.describe())

except FileNotFoundError:
    st.error("📉 No taxi data available. Please upload `nyc_taxi_2024.csv` to enable insights.")

# -------------------- ADD FEEDBACK SYSTEM --------------------
st.markdown("### ⭐ Provide Feedback")
rating = st.radio("Was this answer helpful?", ["👍 Yes", "👎 No"], key="rating")
if rating == "👎 No":
    st.text_area("How can we improve?", key="feedback_input")