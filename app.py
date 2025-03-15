import streamlit as st
import pandas as pd
import io
from hdfs import InsecureClient  # HDFS access

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
HDFS_URL = "http://node-master:9870"  
HDFS_PATH = "/data/nyc_taxi_2_months_csv"  
client = InsecureClient(HDFS_URL, user="supakin")

# -------------------- SPECIFIC CSV FILES TO LOAD --------------------
selected_files = [
    "fhv_tripdata_2023-01.csv",
    "fhv_tripdata_2023-02.csv",
    "fhvhv_tripdata_2023-01.csv",
    "fhvhv_tripdata_2023-02.csv",
    "green_tripdata_2023-01.csv",
    "green_tripdata_2023-02.csv",
    "yellow_tripdata_2023-01.csv",
    "yellow_tripdata_2023-02.csv",
]

# -------------------- SIDEBAR -------------------- 
with st.sidebar:
    st.title("NYC Taxi Data Analytics 🚖")

    st.markdown("### **Project Details**")
    st.markdown(
        """
        **NYC Taxi Chatbot** helps analyze taxi & limousine data from **January & February 2023 ONLY**.  
        Data is sourced from NYC's **Taxi & Limousine Commission (TLC)**.

        **Key Features:**
        - 🚖 Get insights on taxi pickups & fares.
        - 📊 Analyze ride trends from January & February 2023.
        - 🔎 Ask about locations, peak hours, and more.

        **📂 Data Source:**  
        [NYC TLC Official Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
        """
    )

    st.warning("📌 **All responses are based strictly on January & February 2023 data**.")  
    st.divider()

    st.markdown("### **Selected Data Files**")
    st.write("\n".join(selected_files))  # Display loaded file names in sidebar

# -------------------- FUNCTION TO LOAD CSV SEQUENTIALLY --------------------
def load_csv_lazy(file_path, chunksize=100000):
    """Load large CSV files in chunks and return concatenated DataFrame."""
    chunk_list = []
    try:
        with client.read(file_path) as reader:
            for chunk in pd.read_csv(reader, chunksize=chunksize, low_memory=False):
                # Fix column names for consistency
                chunk.columns = chunk.columns.str.replace("PUlocationID", "PULocationID", regex=False)
                chunk.columns = chunk.columns.str.replace("DOlocationID", "DOLocationID", regex=False)
                chunk_list.append(chunk)
        return pd.concat(chunk_list, ignore_index=True) if chunk_list else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error loading file: {file_path} - {e}")
        return pd.DataFrame()  # Return empty DataFrame if error occurs

# -------------------- CHATBOX FOR USER INTERACTION --------------------
st.markdown("## 💬 Ask NYC Taxi Chatbot")
user_query = st.text_input("Ask a question about NYC taxi data:", "")

if st.button("Submit Query"):
    if user_query:
        try:
            st.write(f"🔍 **Processing your query:** `{user_query}`")

            # -------------------- QUERY PROCESSING --------------------
            df_list = []
            for file in selected_files:
                file_path = f"{HDFS_PATH}/{file}"
                df = load_csv_lazy(file_path)
                if not df.empty:
                    df_list.append(df)

            if df_list:
                df = pd.concat(df_list, ignore_index=True)

                # -------------------- QUERY HANDLING --------------------

                # 🚖 **Total Taxi Rides**
                if "how many taxi rides" in user_query.lower():
                    st.write("🚖 **Calculating total taxi rides...**")
                    total_rides = len(df)
                    st.write(f"📊 **Total Taxi Rides:** {total_rides:,}")

                # 📍 **Most Pickup Locations**
                elif "most pickups" in user_query.lower():
                    st.write("📍 **Finding top pickup locations...**")
                    if "PULocationID" in df.columns:
                        pickup_counts = df["PULocationID"].value_counts().head(5)
                        st.write("### 🏆 Top 5 Pickup Locations")
                        st.write(pickup_counts)
                    else:
                        st.warning("⚠️ Column 'PULocationID' not found in the dataset.")

                # 💰 **Average Fare**
                elif "average fare" in user_query.lower():
                    st.write("💰 **Calculating average fare amount...**")
                    if "fare_amount" in df.columns:
                        avg_fare = df["fare_amount"].mean()
                        st.write(f"💲 **Average Fare Amount:** ${avg_fare:.2f}")
                    else:
                        st.warning("⚠️ Column 'fare_amount' not found in the dataset.")

                # ⏰ **Peak Hours**
                elif "peak hours" in user_query.lower():
                    st.write("⏰ **Finding peak hours for taxi rides...**")
                    if "pickup_datetime" in df.columns:
                        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors='coerce')
                        df["hour"] = df["pickup_datetime"].dt.hour
                        peak_hours = df["hour"].value_counts().head(5)
                        st.write("### ⏰ Top 5 Peak Hours")
                        st.write(peak_hours)
                    else:
                        st.warning("⚠️ Column 'pickup_datetime' not found in the dataset.")

                # 🚖 **Filter by Taxi Type**
                elif "green taxi" in user_query.lower():
                    st.write("🚖 **Finding Green Taxi rides...**")
                    green_df = df[df["VendorID"] == 2] if "VendorID" in df.columns else pd.DataFrame()
                    st.write(f"📊 **Total Green Taxi Rides:** {len(green_df):,}" if not green_df.empty else "⚠️ No Green Taxi data found.")

                elif "yellow taxi" in user_query.lower():
                    st.write("🚖 **Finding Yellow Taxi rides...**")
                    yellow_df = df[df["VendorID"] == 1] if "VendorID" in df.columns else pd.DataFrame()
                    st.write(f"📊 **Total Yellow Taxi Rides:** {len(yellow_df):,}" if not yellow_df.empty else "⚠️ No Yellow Taxi data found.")

                elif "fhv" in user_query.lower():
                    st.write("🚖 **Finding FHV (For-Hire Vehicle) rides...**")
                    fhv_df = df[df["VendorID"].isna()] if "VendorID" in df.columns else df
                    st.write(f"📊 **Total FHV Rides:** {len(fhv_df):,}" if not fhv_df.empty else "⚠️ No FHV data found.")

                else:
                    st.warning("⚠️ Sorry, I can't answer that question yet. Try asking about **most pickups, average fare, peak hours, or a specific taxi type.**")

            else:
                st.error("🚨 **No valid CSV files found in HDFS!** Please check the directory path.")

        except Exception as e:
            st.error(f"❌ **Error processing query:** {str(e)}")

    else:
        st.warning("⚠️ Please enter a question before submitting.")