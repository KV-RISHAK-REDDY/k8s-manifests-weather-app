import streamlit as st
import requests
import os
from typing import List, Dict, Optional
import time

# Configuration
SERVICE_2_URL = os.getenv('SERVICE_2_URL', 'http://localhost:8080')
SERVICE_3_URL = os.getenv('SERVICE_3_URL', 'http://localhost:8502')

def call_weather_processing_service(cities: List[str]) -> Optional[Dict]:
    """Call Weather API Handler to process weather data and store in database"""
    try:
        payload = {"cities": cities}
        response = requests.post(
            f"{SERVICE_2_URL}/process-weather",
            json=payload,
            timeout=60  # Increased timeout for database operations
        )
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.Timeout:
        st.error("Request timed out. The weather service is taking too long to process your request.")
        return None
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to weather processing service. Please try again later.")
        return None
    except requests.exceptions.HTTPError as e:
        error_msg = "Weather service error"
        try:
            error_data = e.response.json()
            if error_data.get('error'):
                error_msg += f": {error_data['error']}"
        except:
            error_msg += f": HTTP {e.response.status_code}"
        st.error(error_msg)
        return None
    except Exception as e:
        st.error(f"Unexpected error: {str(e)}")
        return None

def main():
    st.set_page_config(
        page_title="Weather Dashboard Input",
        page_icon="🌤️",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("Multi-City Weather Dashboard")
    st.markdown("### Homepage - Enter Cities for Weather Information")
    
    # Initialize session state
    if 'processing' not in st.session_state:
        st.session_state.processing = False
    if 'last_request_successful' not in st.session_state:
        st.session_state.last_request_successful = None
    if 'request_id' not in st.session_state:
        st.session_state.request_id = None
    
    # Service status check
    with st.sidebar:
        st.header("System Status")
        
        # Check Service 2 status
        try:
            response = requests.get(f"{SERVICE_2_URL}/status", timeout=5)
            if response.status_code == 200:
                status_data = response.json()
                st.success("API Handler Service: Online")
                
                # Show database status
                db_status = status_data.get('database', {})
                if db_status.get('status') == 'connected':
                    st.success("Database: Connected")
                    st.info(f"Locations: {db_status.get('locations_count', 0)}")
                    st.info(f"Weather Records: {db_status.get('weather_records_count', 0)}")
                else:
                    st.error("Database: Disconnected")

                st.info(f"Recent Requests: {status_data.get('recent_requests', 0)}")
            else:
                st.error("API Handler Service: Offline")
        except:
            st.error("API Handler Service: Unreachable")
        
        # Check Service 3 status
        try:
            # Simple check - just see if we can connect
            response = requests.get(f"{SERVICE_3_URL}/_stcore/health", timeout=5)
            st.success("Dashboard Service: Online")
        except:
            st.warning("Dashboard Service: May be offline")
    
    # Input section
    st.markdown("---")
    
    # Create two columns for different input methods
    
    with st.container():
        st.subheader("📍 Individual Input")
        
        # Initialize city list in session state
        if 'city_list' not in st.session_state:
            st.session_state.city_list = []
        
        # Add city input
        new_city = st.text_input(
            "Add a city:", 
            key="new_city_input",
            placeholder="Enter city name"
        )
        
        col2_1, col2_2 = st.columns([3, 1])
        with col2_1:
            if st.button("➕ Add City", disabled=not new_city):
                if new_city and new_city not in st.session_state.city_list:
                    st.session_state.city_list.append(new_city)
                    st.rerun()
                elif new_city in st.session_state.city_list:
                    st.warning(f"'{new_city}' is already in the list!")
        
        with col2_2:
            if st.button("🗑️ Clear All"):
                st.session_state.city_list = []
                st.rerun()
        
        # Display current city list
        if st.session_state.city_list:
            st.write("**Current cities:**")
            for i, city in enumerate(st.session_state.city_list):
                col_city, col_remove = st.columns([4, 1])
                with col_city:
                    st.write(f"• {city}")
                with col_remove:
                    if st.button("❌", key=f"remove_{i}", help=f"Remove {city}"):
                        st.session_state.city_list.pop(i)
                        st.rerun()
    
    # Combine all cities
    all_cities = st.session_state.city_list
    unique_cities = list(dict.fromkeys(all_cities))  # Remove duplicates while preserving order
    
    # Display summary
    if unique_cities:
        st.markdown("---")
        st.subheader("📋 Request Summary")
        
        col_summary1, col_summary2 = st.columns([2, 1])
        with col_summary1:
            st.write(f"**Cities to process:** {len(unique_cities)}")
            cities_display = ", ".join(unique_cities[:5])
            if len(unique_cities) > 5:
                cities_display += f" ... and {len(unique_cities) - 5} more"
            st.write(f"**Cities:** {cities_display}")
        
        with col_summary2:
            if len(unique_cities) > 20:
                st.error("❌ Maximum 20 cities allowed")
                process_disabled = True
            elif len(unique_cities) == 0:
                st.warning("⚠️ No cities selected")
                process_disabled = True
            else:
                st.success(f"✅ Ready to process {len(unique_cities)} cities")
                process_disabled = False
    else:
        process_disabled = True
    
    # Process button
    st.markdown("---")
    
    col_btn1, col_btn3 = st.columns([2, 1])
    
    with col_btn1:
        process_button = st.button(
            "Process Weather Data",
            disabled=process_disabled or st.session_state.processing,
            type="primary",
            help="Send cities to API handler for processing and database storage"
        )
    
    with col_btn3:
        if st.button("Reset", help="Clear all inputs and start over"):
            st.session_state.city_list = []
            st.session_state.processing = False
            st.session_state.last_request_successful = None
            st.session_state.request_id = None
            st.rerun()
    
    # Processing section
    if process_button and unique_cities:
        st.session_state.processing = True
        st.rerun()
    
    if st.session_state.processing:
        st.markdown("---")
        st.subheader("Processing Weather Data")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Validate input
        progress_bar.progress(10)
        status_text.text("Validating cities...")
        time.sleep(0.5)
        
        # Step 2: Send to API handler
        progress_bar.progress(30)
        status_text.text("Sending request to API handler...")
        
        # Make the actual request
        result = call_weather_processing_service(unique_cities)
        
        if result:
            progress_bar.progress(60)
            status_text.text("Storing data in database...")
            time.sleep(1)
            
            progress_bar.progress(90)
            status_text.text("Processing complete!")
            time.sleep(0.5)
            
            progress_bar.progress(100)
            status_text.text("Success! Data processed and stored.")

            # Store success state
            st.session_state.last_request_successful = True
            st.session_state.request_id = result.get('request_id')
            st.session_state.processing = False
            
            # Display results
            st.success("✅ Weather data processing completed successfully!")
            
            # Show summary
            col_res1, col_res2, col_res3 = st.columns(3)
            
            with col_res1:
                st.metric(
                    "Successful Cities",
                    result.get('successful_cities', 0),
                    delta=f"of {len(unique_cities)} requested"
                )
            
            with col_res2:
                st.metric(
                    "Failed Cities",
                    result.get('failed_cities', 0)
                )
            
            with col_res3:
                st.metric(
                    "Request ID",
                    result.get('request_id', 'Unknown')[:8] + "..."
                )
            
            # Show errors if any
            if result.get('errors'):
                with st.expander(f"Processing Errors ({len(result['errors'])})", expanded=False):
                    for error in result['errors']:
                        st.warning(error)
            
            # Auto-redirect to dashboard
            st.markdown("### Next Steps")
            st.markdown("You can now view the processed weather data in the dashboard.")
            st.markdown(f"[Open Dashboard]({SERVICE_3_URL})", unsafe_allow_html=True)
        else:
            progress_bar.progress(100)
            status_text.text("Processing failed!")

            st.session_state.last_request_successful = False
            st.session_state.processing = False

            st.error("Failed to process weather data. Please check the errors above and try again.")
    
    # Footer with helpful information
    st.markdown("---")

    with st.expander("How it works", expanded=False):
        st.markdown("""
        **Workflow:**
        1. 🏠 **Input Stage**: Enter city names using either bulk input or individual city addition
        2. 📡 **Processing**: Cities are sent to the API Handler service
        3. 🌤️ **Data Fetching**: API Handler fetches weather data from WeatherAPI.com
        4. 💾 **Storage**: Weather data is stored in PostgreSQL database
        5. 📊 **Dashboard**: View processed data in the interactive dashboard
        
        **Features:**
        - Real-time weather data from WeatherAPI.com
        - Persistent storage in PostgreSQL database
        - Duplicate city handling
        - Error reporting and recovery
        - Automatic dashboard redirection
        """)
    
    with st.expander("🔧 Troubleshooting", expanded=False):
        st.markdown("""
        **Common Issues:**
        - **Service Offline**: Check the system status in the sidebar
        - **City Not Found**: Verify city names are spelled correctly
        - **Processing Timeout**: Large requests may take time, please wait
        - **Dashboard Not Loading**: Try refreshing or check if Service 3 is running
        
        **Tips:**
        - Use common city names (e.g., "London" instead of "London, UK")
        - Maximum 20 cities per request
        - Check system status before processing
        """)

if __name__ == "__main__":
    main()
        