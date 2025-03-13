import streamlit as st

st.title("NYC Taxi Chatbot 🚖")
st.write("Ask anything about NYC Taxi & Limousine Commission (TLC) data!")

# Input box
user_input = st.text_input("Enter your question:")
if user_input:
    st.write(f"You asked: {user_input}")
    st.write("Chatbot response will go here...")