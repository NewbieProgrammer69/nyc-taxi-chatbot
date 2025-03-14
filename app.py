import streamlit as st
import requests
import os

# -------------------- CONFIGURATION --------------------
st.set_page_config(
    page_title="NYC Taxi Chatbot 🚖",
    page_icon="🚖",
    layout="wide",
)

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

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Optimized chat history retrieval
    for chat in st.session_state.chat_history:
        if st.button(chat["title"]):
            st.session_state.messages = chat["messages"]

    if st.button("🆕 Start New Chat"):
        st.session_state.messages = []

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
            prompt = (
                f"You are an AI analyzing NYC Taxi & Limousine Commission data. "
                f"⚠️ **Only respond based on 2024 data.** Do NOT use data from previous years.\n\n"
                f"User Query: {user_input}"
            )
            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {deepseek_api_key}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}]},
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