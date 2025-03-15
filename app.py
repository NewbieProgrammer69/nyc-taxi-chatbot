import streamlit as st
import pandas as pd
from hdfs import InsecureClient  # HDFS access

# -------------------- CONFIGURATION --------------------
st.set_page_config(page_title="NYC Taxi Chatbot 🚖", page_icon="🚖", layout="wide")

HDFS_URL = "http://node-master:9870"
HDFS_PATH = "/data/nyc_taxi_2_months_csv"
client = InsecureClient(HDFS_URL, user="supakin")

selected_files = {
    "fhv_tripdata_2023-01.csv": "pickup_datetime",
    "fhv_tripdata_2023-02.csv": "pickup_datetime",
    "fhvhv_tripdata_2023-01.csv": "pickup_datetime",
    "fhvhv_tripdata_2023-02.csv": "pickup_datetime",
    "green_tripdata_2023-01.csv": "lpep_pickup_datetime",
    "green_tripdata_2023-02.csv": "lpep_pickup_datetime",
    "yellow_tripdata_2023-01.csv": "tpep_pickup_datetime",
    "yellow_tripdata_2023-02.csv": "tpep_pickup_datetime",
}

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

# -------------------- FUNCTION TO LOAD CSV IN CHUNKS --------------------
def load_csv_lazy(file_path, pickup_col, chunksize=500000):
    """Load CSV in chunks, handle missing columns dynamically."""
    chunk_list = []
    try:
        with client.read(file_path) as reader:
            for chunk in pd.read_csv(reader, chunksize=chunksize, low_memory=False):
                # Fix column names
                chunk.columns = chunk.columns.str.replace("PUlocationID", "PULocationID", regex=False)
                chunk.columns = chunk.columns.str.replace("DOlocationID", "DOLocationID", regex=False)

                # Convert pickup_datetime if column exists
                if pickup_col in chunk.columns:
                    chunk[pickup_col] = pd.to_datetime(chunk[pickup_col], errors="coerce")

                chunk_list.append(chunk)
                del chunk  # Free memory

        return pd.concat(chunk_list, ignore_index=True) if chunk_list else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error loading file: {file_path} - {e}")
        return pd.DataFrame()

# -------------------- CHATBOX FOR USER INTERACTION --------------------
st.markdown("## 💬 Ask NYC Taxi Chatbot")
user_query = st.text_input("Ask a question about NYC taxi data:", "")

if st.button("Submit Query"):
    if user_query:
        try:
            st.write(f"🔍 **Processing your query:** `{user_query}`")

            # -------------------- QUERY PROCESSING --------------------
            df_list = []
            for file, pickup_col in selected_files.items():
                file_path = f"{HDFS_PATH}/{file}"
                df = load_csv_lazy(file_path, pickup_col)
                if not df.empty:
                    df_list.append(df)

            if df_list:
                df = pd.concat(df_list, ignore_index=True)

                # -------------------- QUERY HANDLING --------------------

                # 🚖 **Total Taxi Rides**
                if "how many taxi rides" in user_query.lower():
                    st.write("🚖 **Calculating total taxi rides...**")
                    st.write(f"📊 **Total Taxi Rides:** {len(df):,}")

                # 📍 **Most Pickup Locations**
                elif any(keyword in user_query.lower() for keyword in ["most pickups", "top 5 pickup locations", "busiest locations"]):
                    st.write("📍 **Finding top pickup locations...**")
                    if "PULocationID" in df.columns:
                        pickup_counts = df["PULocationID"].value_counts().head(5)
                        st.write("### 🏆 Top 5 Pickup Locations")
                        st.write(pickup_counts)
                    else:
                        st.warning("⚠️ Column 'PULocationID' not found.")

                # 💰 **Average Fare**
                elif "average fare" in user_query.lower():
                    st.write("💰 **Calculating average fare amount...**")
                    if "fare_amount" in df.columns:
                        avg_fare = df["fare_amount"].mean()
                        st.write(f"💲 **Average Fare Amount:** ${avg_fare:.2f}")
                    else:
                        st.warning("⚠️ Column 'fare_amount' not found.")

                # ⏰ **Peak Hours**
                elif "peak hours" in user_query.lower():
                    st.write("⏰ **Finding peak hours for taxi rides...**")
                    peak_hours = []
                    for pickup_col in selected_files.values():
                        if pickup_col in df.columns:
                            df["hour"] = df[pickup_col].dt.hour
                            peak_hours.append(df["hour"].value_counts().head(5))
                    if peak_hours:
                        st.write("### ⏰ Top 5 Peak Hours")
                        st.write(pd.concat(peak_hours).groupby(level=0).sum().sort_values(ascending=False).head(5))
                    else:
                        st.warning("⚠️ No pickup time column found.")

                # 🚖 **Filter by Taxi Type**
                elif "green taxi" in user_query.lower():
                    st.write("🚖 **Finding Green Taxi rides...**")
                    green_df = df[df["VendorID"] == 2] if "VendorID" in df.columns else pd.DataFrame()
                    st.write(f"📊 **Total Green Taxi Rides:** {len(green_df):,}" if not green_df.empty else "⚠️ No Green Taxi data found.")

                elif "yellow taxi" in user_query.lower():
                    st.write("🚖 **Finding Yellow Taxi rides...**")
                    yellow_df = df[df["VendorID"] == 1] if "VendorID" in df.columns else pd.DataFrame()
                    st.write(f"📊 **Total Yellow Taxi Rides:** {len(yellow_df):,}" if not yellow_df.empty else "⚠️ No Yellow Taxi data found.")

                elif "fhvhv" in user_query.lower():
                    st.write("🚖 **Finding FHVHV rides...**")
                    fhvhv_df_list = []
                    for file, pickup_col in selected_files.items():
                        if "fhvhv_tripdata" in file:
                            file_path = f"{HDFS_PATH}/{file}"
                            df_chunk = load_csv_lazy(file_path, pickup_col)  # Load file in chunks
                            if not df_chunk.empty:
                                fhvhv_df_list.append(df_chunk)
                    if fhvhv_df_list:
                        fhvhv_df = pd.concat(fhvhv_df_list, ignore_index=True)
                        st.write(f"📊 **Total FHVHV Rides:** {len(fhvhv_df):,}")
                    else:
                        st.warning("⚠️ No FHVHV data found.")
                
                elif "fhv" in user_query.lower():
                    st.write("🚖 **Finding FHV rides...**")
                    fhv_df_list = []
                    for file, pickup_col in selected_files.items():
                        if "fhv_tripdata" in file and "fhvhv" not in file:  # Ensure only FHV and not FHVHV
                            file_path = f"{HDFS_PATH}/{file}"
                            df_chunk = load_csv_lazy(file_path, pickup_col)  # Load file in chunks
                            if not df_chunk.empty:
                                fhv_df_list.append(df_chunk)
                    if fhv_df_list:
                        fhv_df = pd.concat(fhv_df_list, ignore_index=True)
                        st.write(f"📊 **Total FHV Rides:** {len(fhv_df):,}")
                    else:
                        st.warning("⚠️ No FHV data found.")

                # ❌ Unrecognized query
                else:
                    st.warning("⚠️ Sorry, I can't answer that question yet. Try asking about **most pickups, average fare, peak hours, or a specific taxi type.**")

            else:
                st.error("🚨 **No valid CSV files found in HDFS!** Please check the directory path.")

        except Exception as e:
            st.error(f"❌ **Error processing query:** {str(e)}")

    else:
        st.warning("⚠️ Please enter a question before submitting.")