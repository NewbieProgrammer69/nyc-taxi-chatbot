import streamlit as st
import requests
import os

# Page Configuration
st.set_page_config(page_title="NYC Taxi Chatbot 🚖", page_icon="🚖", layout="wide")

# Sidebar Information
with st.sidebar:
    st.title("About This Chatbot 🚖")
    st.markdown(
        """
        **NYC Taxi Chatbot** helps you explore NYC Taxi & Limousine Commission (TLC) data using AI. 
        Ask questions like:
        - "Show me the top 5 locations for pickups in the past month."
        - "What is the busiest hour for NYC taxis?"
        
        **API Used:** DeepSeek Chat API
        """
    )
    st.divider()
    st.markdown("🔗 [NYC TLC Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)")

# Set up conversation history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=message["avatar"]):
        st.markdown(message["content"])

# User Input
if user_input := st.chat_input("Ask me anything about NYC taxis! 🚖"):
    # Display user input in chat
    with st.chat_message("user", avatar="🧑"):
        st.markdown(user_input)

    # Store user input in session state
    st.session_state.messages.append(
        {"role": "user", "content": user_input, "avatar": "🧑"}
    )

    # Fetch response from DeepSeek API
    deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")  # Ensure API key is set in Streamlit secrets
    if not deepseek_api_key:
        response_text = "⚠️ API key not found. Please check your environment settings."
    else:
        try:
            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {deepseek_api_key}", "Content-Type": "application/json"},
                json={"model": "deepseek-chat", "messages": [{"role": "user", "content": user_input}]},
            )
            response_text = response.json().get("choices", [{}])[0].get("message", {}).get("content", "No response.")
        except Exception as e:
            response_text = f"⚠️ Error calling DeepSeek API: {str(e)}"

    # Display AI response
    with st.chat_message("assistant", avatar="🤖"):
        st.markdown(response_text)

    # Store assistant response
    st.session_state.messages.append(
        {"role": "assistant", "content": response_text, "avatar": "🤖"}
    )