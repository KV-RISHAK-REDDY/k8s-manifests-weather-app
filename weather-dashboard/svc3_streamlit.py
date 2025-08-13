import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from typing import Dict, List, Optional

# Configuration
SERVICE_1_URL = os.getenv('SERVICE_1_URL', 'http://localhost:8501')
SERVICE_2_URL = os.getenv('SERVICE_2_URL', 'http://localhost:8080')

humidity_string = 'Humidity (%)'
wind_speed_string = 'Wind Speed (km/h)'
temperature_string = 'Temperature (°C)'
uv_index_string = 'UV Index'

def get_recent_weather_data_from_service2() -> Optional[Dict]:
    """Get recent weather data from Service 2 database"""
    try:
        response = requests.get(f"{SERVICE_2_URL}/get-recent-data", timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        st.error("⏰ Request timed out while fetching weather data from database.")
        return None
    except requests.exceptions.ConnectionError:
        st.error("🔌 Cannot connect to API handler service. Please try again later.")
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            st.warning("📭 No recent weather data found. Please process some cities first.")
        else:
            st.error(f"❌ API handler service error: {e.response.status_code}")
        return None
    except Exception as e:
        st.error(f"💥 Unexpected error while fetching data: {str(e)}")
        return None

def get_weather_data_by_cities(cities: List[str]) -> Optional[Dict]:
    """Get weather data for specific cities from Service 2 database"""
    try:
        payload = {"cities": cities}
        response = requests.post(
            f"{SERVICE_2_URL}/get-data-by-cities",
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error fetching data for specific cities: {str(e)}")
        return None

def format_temperature(temp_c: float, temp_f: float = None) -> str:
    """Format temperature display"""
    if temp_f is None:
        temp_f = (temp_c * 9/5) + 32
    return f"{temp_c}°C ({temp_f:.1f}°F)"

def create_temperature_chart(weather_data: List[Dict]) -> go.Figure:
    """Create temperature comparison chart"""
    cities = []
    temperatures = []
    feels_like = []
    
    for city_data in weather_data:
        location = city_data.get('location', {})
        current = city_data.get('current', {})
        
        cities.append(location.get('name', 'Unknown'))
        temperatures.append(current.get('temp_c', 0))
        feels_like.append(current.get('feelslike_c', 0))
    
    fig = go.Figure()
    
    # Add temperature bars
    fig.add_trace(go.Bar(
        name='Actual Temperature',
        x=cities,
        y=temperatures,
        marker_color='rgba(55, 128, 191, 0.7)',
        text=[f"{temp}°C" for temp in temperatures],
        textposition='auto',
    ))
    
    # Add feels like bars
    fig.add_trace(go.Bar(
        name='Feels Like',
        x=cities,
        y=feels_like,
        marker_color='rgba(255, 153, 51, 0.7)',
        text=[f"{temp}°C" for temp in feels_like],
        textposition='auto',
    ))
    
    fig.update_layout(
        title='Temperature Comparison Across Cities',
        xaxis_title='Cities',
        yaxis_title=temperature_string,
        barmode='group',
        height=400,
        showlegend=True
    )
    
    return fig

def create_conditions_chart(weather_data: List[Dict]) -> go.Figure:
    """Create weather conditions pie chart"""
    conditions = {}
    
    for city_data in weather_data:
        current = city_data.get('current', {})
        condition = current.get('condition', {}).get('text', 'Unknown')
        conditions[condition] = conditions.get(condition, 0) + 1
    
    fig = go.Figure(data=[go.Pie(
        labels=list(conditions.keys()),
        values=list(conditions.values()),
        hole=0.3,
        textinfo='label+percent',
        textposition='auto',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )])
    
    fig.update_layout(
        title='Weather Conditions Distribution',
        height=400
    )
    
    return fig

def create_humidity_wind_chart(weather_data: List[Dict]) -> go.Figure:
    """Create humidity vs wind speed scatter plot"""
    cities = []
    humidity_values = []
    wind_speeds = []
    temperatures = []
    
    for city_data in weather_data:
        location = city_data.get('location', {})
        current = city_data.get('current', {})
        
        cities.append(location.get('name', 'Unknown'))
        humidity_values.append(current.get('humidity', 0))
        wind_speeds.append(current.get('wind_kph', 0))
        temperatures.append(current.get('temp_c', 0))
    
    fig = go.Figure(data=go.Scatter(
        x=humidity_values,
        y=wind_speeds,
        mode='markers+text',
        text=cities,
        textposition="top center",
        marker={
            'size': temperatures,
            'sizemode': 'diameter',
            'sizeref': 2.*max(temperatures)/(40.**2),
            'sizemin': 4,
            'color': temperatures,
            'colorscale': 'Viridis',
            'showscale': True,
            'colorbar': {'title': temperature_string}
        },
        hovertemplate='<b>%{text}</b><br>' +
                      'Humidity: %{x}%<br>' +
                      'Wind Speed: %{y} km/h<br>' +
                      'Temperature: %{marker.color}°C<extra></extra>'
    ))
    
    fig.update_layout(
        title='Humidity vs Wind Speed (Bubble size = Temperature)',
        xaxis_title=humidity_string,
        yaxis_title=wind_speed_string,
        height=500
    )
    
    return fig

def display_city_card(city_data: Dict):
    """Display individual city weather card"""
    location = city_data.get('location', {})
    current = city_data.get('current', {})
    
    city_name = location.get('name', 'Unknown City')
    country = location.get('country', 'Unknown Country')
    region = location.get('region', '')
    temp_c = current.get('temp_c', 0)
    temp_f = current.get('temp_f', 0)
    condition = current.get('condition', {})
    condition_text = condition.get('text', 'Unknown')
    humidity = current.get('humidity', 0)
    wind_kph = current.get('wind_kph', 0)
    wind_dir = current.get('wind_dir', 'N/A')
    feels_like_c = current.get('feelslike_c', 0)
    uv = current.get('uv', 0)
    pressure_mb = current.get('pressure_mb', 0)
    visibility = current.get('vis_km', 0)
    last_updated = current.get('last_updated', 'Unknown')

    icon = condition.get('icon', '')

    # # Display location with region if available
    # location_display = f"{city_name}, {country}"
    # if region and region != city_name:
    #     location_display = f"{city_name}, {region}, {country}"
    
    with st.container():
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            margin: 10px 0;
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.15);
            border: 1px solid rgba(255, 255, 255, 0.1);
        ">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                <div>
                    <h3 style="margin: 0 0 5px 0; font-size: 1.4em;">{city_name}</h3>
                    <p style="margin: 0; opacity: 0.9; font-size: 0.9em;">{region + ', ' if region and region != city_name else ''}{country}</p>
                </div>
                <div style="text-align: right;">
                    <div style="font-size: 3em; font-weight: bold; line-height: 1;">
                        {temp_c}°C
                    </div>
                    <div style="font-size: 0.9em; opacity: 0.8;">
                        {temp_f:.1f}°F
                    </div>
                </div>
            </div>
            <div style="display: flex; align-items: center; justify-content: center;">
                <img src="{icon}" alt="{condition_text}" style="width: 64px;
                    height: 64px;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                    margin: 0 16px;
                    background: rgba(255,255,255,0.1);
                    padding: 4px;
                " />
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <div style="font-size: 1.2em; margin-bottom: 5px; font-weight: 500;">
                        {condition_text}
                    </div>
                    <div style="font-size: 0.9em; opacity: 0.8;">
                        Feels like {feels_like_c}°C
                    </div>
                </div>
                <div style="text-align: right; font-size: 0.8em; opacity: 0.7;">
                    Updated: {last_updated}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Additional details in columns
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("💧 Humidity", f"{humidity}%")
        
        with col2:
            st.metric("💨 Wind", f"{wind_kph} km/h", delta=wind_dir)
        
        with col3:
            st.metric("☀️ UV Index", f"{uv}")
        
        with col4:
            st.metric("🌡️ Pressure", f"{pressure_mb} mb")
        
        with col5:
            st.metric("👁️ Visibility", f"{visibility} km")

def display_data_table(weather_data: List[Dict]) -> pd.DataFrame:
    """Create and display detailed data table"""
    table_data = []
    
    for city_data in weather_data:
        location = city_data.get('location', {})
        current = city_data.get('current', {})
        
        table_data.append({
            'City': location.get('name', 'Unknown'),
            'Country': location.get('country', 'Unknown'),
            'Region': location.get('region', 'Unknown'),
            temperature_string: current.get('temp_c', 0),
            'Temperature (°F)': current.get('temp_f', 0),
            'Feels Like (°C)': current.get('feelslike_c', 0),
            'Condition': current.get('condition', {}).get('text', 'Unknown'),
            humidity_string: current.get('humidity', 0),
            wind_speed_string: current.get('wind_kph', 0),
            'Wind Direction': current.get('wind_dir', 'Unknown'),
            'Pressure (mb)': current.get('pressure_mb', 0),
            uv_index_string: current.get('uv', 0),
            'Visibility (km)': current.get('vis_km', 0),
            'Last Updated': current.get('last_updated', 'Unknown'),
            'Latitude': location.get('lat', 0),
            'Longitude': location.get('lon', 0),
            'Timezone': location.get('tz_id', 'Unknown')
        })
    
    return pd.DataFrame(table_data)

def main():
    st.set_page_config(
        page_title="Weather Dashboard",
        page_icon="🌤️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Sidebar with navigation and controls
    with st.sidebar:
        st.title("🌤️ Dashboard Controls")
        
        # Navigation
        st.markdown("### 🧭 Navigation")
        col_nav1, col_nav2 = st.columns(2)
        
        with col_nav1:
            if st.button("🏠 Input Page", help="Go back to input page"):
                input_page_url = SERVICE_1_URL
                st.markdown(f"[Open Input Page]({input_page_url})", unsafe_allow_html=True)
                st.success("Click the link above to open the input page.")
        
        with col_nav2:
            if st.button("🔄 Refresh Data", help="Reload weather data from database"):
                st.rerun()
        
        # Data source selection
        st.markdown("### 📊 Data Source")
        data_source = st.radio(
            "Choose data source:",
            ["Recent Request", "Custom Cities"],
            help="Recent Request: Latest processed cities\nCustom Cities: Specify cities manually"
        )
        
        custom_cities = []
        if data_source == "Custom Cities":
            st.markdown("**Enter city names:**")
            city_input = st.text_area(
                "Cities (one per line):",
                placeholder="London\nParis\nTokyo",
                height=100
            )
            if city_input:
                custom_cities = [city.strip() for city in city_input.split('\n') if city.strip()]
                st.info(f"Selected {len(custom_cities)} cities")
        
        # Service status
        st.markdown("### 🔧 Service Status")
        try:
            response = requests.get(f"{SERVICE_2_URL}/status", timeout=5)
            if response.status_code == 200:
                status_data = response.json()
                st.success("✅ API Handler: Online")
                
                # Database status
                db_status = status_data.get('database', {})
                if db_status.get('status') == 'connected':
                    st.success("✅ Database: Connected")
                    st.caption(f"📊 {db_status.get('locations_count', 0)} locations")
                    st.caption(f"🌤️ {db_status.get('weather_records_count', 0)} records")
                else:
                    st.error("❌ Database: Error")
            else:
                st.error("❌ API Handler: Error")
        except:
            st.error("❌ API Handler: Offline")
    
    # Main content
    st.title("🌤️ Weather Dashboard")
    st.markdown("### 📊 Real-time Weather Data from Database")
    
    # Get weather data based on selection
    if data_source == "Recent Request":
        weather_response = get_recent_weather_data_from_service2()
    else:
        if custom_cities:
            weather_response = get_weather_data_by_cities(custom_cities)
        else:
            st.info("👆 Please enter city names in the sidebar to view custom data.")
            return
    
    if not weather_response:
        st.error("❌ Unable to retrieve weather data")
        
        with st.expander("🔧 Troubleshooting Steps"):
            st.markdown("""
            **Try these steps:**
            1. **Check if you've processed cities recently** - Go to the input page and process some cities
            2. **Verify API Handler service** - Check the service status in the sidebar
            3. **Database connection** - Ensure PostgreSQL is running and accessible
            4. **Refresh the page** - Click the refresh button in the sidebar
            5. **Try custom cities** - Use the "Custom Cities" option to specify cities manually
            """)
        
        # Show recent requests for debugging
        try:
            response = requests.get(f"{SERVICE_2_URL}/recent-requests", timeout=10)
            if response.status_code == 200:
                recent_data = response.json()
                if recent_data.get('requests'):
                    st.subheader("📋 Recent Requests")
                    for i, req in enumerate(recent_data['requests'][-3:]):  # Show last 3
                        with st.expander(f"Request {i+1} - {req.get('request_id', 'Unknown')[:8]}"):
                            st.write(f"**Cities:** {', '.join(req.get('cities', []))}")
                            st.write(f"**Time:** {req.get('requested_at', 'Unknown')}")
        except:
            pass
        
        return
    
    if not weather_response.get('success'):
        st.error(f"❌ Error retrieving data: {weather_response.get('error', 'Unknown error')}")
        return
    
    weather_data = weather_response.get('data', [])
    if not weather_data:
        st.warning("📭 No weather data available")
        return
    
    # Summary metrics at the top
    st.markdown("---")
    st.subheader("📊 Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="Cities",
            value=len(weather_data),
            delta="Retrieved from DB"
        )
    
    with col2:
        temps = [city['current'].get('temp_c', 0) for city in weather_data if city['current'].get('temp_c') is not None]
        avg_temp = sum(temps) / len(temps) if temps else 0
        st.metric(
            label="Avg Temperature",
            value=f"{avg_temp:.1f}°C",
            delta=f"{(avg_temp * 9/5) + 32:.1f}°F"
        )
    
    with col3:
        humidity_values = [city['current'].get('humidity', 0) for city in weather_data if city['current'].get('humidity') is not None]
        avg_humidity = sum(humidity_values) / len(humidity_values) if humidity_values else 0
        st.metric(
            label="Avg Humidity",
            value=f"{avg_humidity:.0f}%"
        )
    
    with col4:
        conditions = [city['current'].get('condition', {}).get('text', '') for city in weather_data]
        most_common = max(set(conditions), key=conditions.count) if conditions else "N/A"
        condition_count = conditions.count(most_common)
        st.metric(
            label="Most Common",
            value=most_common[:12] + "..." if len(most_common) > 12 else most_common,
            delta=f"{condition_count} cities"
        )
    
    with col5:
        if weather_response.get('retrieved_at'):
            try:
                retrieved_time = datetime.fromisoformat(weather_response['retrieved_at'].replace('Z', '+00:00'))
                time_str = retrieved_time.strftime("%H:%M")
                date_str = retrieved_time.strftime("%d/%m")
            except:
                time_str = "Unknown"
                date_str = "Time"
        else:
            time_str = "Unknown"
            date_str = "Time"
        
        st.metric(
            label="Retrieved At",
            value=time_str,
            delta=date_str
        )
    
    # Display request info if available
    if weather_response.get('request_id'):
        st.info(f"📋 Request ID: {weather_response.get('request_id')} | Retrieved: {weather_response.get('retrieved_at', 'Unknown')}")
    
    # Main dashboard content with tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🏙️ City Overview", "📈 Analytics", "📊 Advanced Charts", "📋 Data Export"])
    
    with tab1:
        st.subheader("🌤️ Weather by City")
        
        # Search and filter
        search_term = st.text_input("🔍 Search cities:", placeholder="Enter city name to filter...")
        
        # Filter data based on search
        filtered_data = weather_data
        if search_term:
            filtered_data = [
                city for city in weather_data 
                if search_term.lower() in city.get('location', {}).get('name', '').lower()
            ]
            
            if not filtered_data:
                st.warning(f"No cities found matching '{search_term}'")
                filtered_data = weather_data
            else:
                st.success(f"Found {len(filtered_data)} cities matching '{search_term}'")
        
        # Display city cards
        for city_data in filtered_data:
            display_city_card(city_data)
    
    with tab2:
        st.subheader("📈 Weather Analytics")
        
        if len(weather_data) > 1:
            # Temperature comparison chart
            temp_chart = create_temperature_chart(weather_data)
            st.plotly_chart(temp_chart, use_container_width=True)
            
            # Weather conditions distribution
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                conditions_chart = create_conditions_chart(weather_data)
                st.plotly_chart(conditions_chart, use_container_width=True)
            
            with col_chart2:
                # Create a simple metrics comparison
                metrics_data = []
                for city_data in weather_data:
                    location = city_data.get('location', {})
                    current = city_data.get('current', {})
                    
                    metrics_data.append({
                        'City': location.get('name', 'Unknown'),
                        humidity_string: current.get('humidity', 0),
                        wind_speed_string: current.get('wind_kph', 0),
                        uv_index_string: current.get('uv', 0),
                        'Pressure (mb)': current.get('pressure_mb', 0)
                    })
                
                df_metrics = pd.DataFrame(metrics_data)
                
                # Humidity bar chart
                fig_humidity = px.bar(
                    df_metrics, 
                    x='City', 
                    y=humidity_string,
                    title='Humidity Levels',
                    color=humidity_string,
                    color_continuous_scale='Blues'
                )
                fig_humidity.update_layout(height=400)
                st.plotly_chart(fig_humidity, use_container_width=True)
        else:
            st.info("📊 Analytics require data from multiple cities. Process more cities to see comparative charts.")
            
            # Show single city detailed info
            if weather_data:
                city_data = weather_data[0]
                st.subheader(f"📍 Detailed Info for {city_data.get('location', {}).get('name', 'Unknown')}")
                
                current = city_data.get('current', {})
                
                # Create gauge charts for single city
                col_gauge1, col_gauge2 = st.columns(2)
                
                with col_gauge1:
                    # Temperature gauge
                    temp_gauge = go.Figure(go.Indicator(
                        mode = "gauge+number+delta",
                        value = current.get('temp_c', 0),
                        domain = {'x': [0, 1], 'y': [0, 1]},
                        title = {'text': "Temperature (°C)"},
                        gauge = {
                            'axis': {'range': [-20, 50]},
                            'bar': {'color': "darkblue"},
                            'steps': [
                                {'range': [-20, 0], 'color': "lightblue"},
                                {'range': [0, 20], 'color': "yellow"},
                                {'range': [20, 50], 'color': "orange"}
                            ],
                            'threshold': {
                                'line': {'color': "red", 'width': 4},
                                'thickness': 0.75,
                                'value': current.get('feelslike_c', 0)
                            }
                        }
                    ))
                    temp_gauge.update_layout(height=300)
                    st.plotly_chart(temp_gauge, use_container_width=True)
                
                with col_gauge2:
                    # Humidity gauge
                    humidity_gauge = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = current.get('humidity', 0),
                        domain = {'x': [0, 1], 'y': [0, 1]},
                        title = {'text': "Humidity (%)"},
                        gauge = {
                            'axis': {'range': [0, 100]},
                            'bar': {'color': "darkgreen"},
                            'steps': [
                                {'range': [0, 30], 'color': "lightgray"},
                                {'range': [30, 70], 'color': "lightgreen"},
                                {'range': [70, 100], 'color': "green"}
                            ]
                        }
                    ))
                    humidity_gauge.update_layout(height=300)
                    st.plotly_chart(humidity_gauge, use_container_width=True)
    
    with tab3:
        st.subheader("📊 Advanced Weather Analysis")
        
        if len(weather_data) > 2:
            # Humidity vs Wind Speed scatter plot
            scatter_chart = create_humidity_wind_chart(weather_data)
            st.plotly_chart(scatter_chart, use_container_width=True)
            
            # Correlation analysis
            st.subheader("🔗 Weather Metrics Correlation")
            
            correlation_data = []
            for city_data in weather_data:
                current = city_data.get('current', {})
                correlation_data.append({
                    'Temperature': current.get('temp_c', 0),
                    'Feels Like': current.get('feelslike_c', 0),
                    'Humidity': current.get('humidity', 0),
                    'Wind Speed': current.get('wind_kph', 0),
                    'Pressure': current.get('pressure_mb', 0),
                    uv_index_string: current.get('uv', 0),
                    'Visibility': current.get('vis_km', 0)
                })
            
            df_corr = pd.DataFrame(correlation_data)
            correlation_matrix = df_corr.corr()
            
            # Create correlation heatmap
            fig_heatmap = px.imshow(
                correlation_matrix,
                text_auto=True,
                aspect="auto",
                title="Weather Metrics Correlation Matrix",
                color_continuous_scale='RdBu'
            )
            fig_heatmap.update_layout(height=500)
            st.plotly_chart(fig_heatmap, use_container_width=True)
            
            # Statistical summary
            st.subheader("📈 Statistical Summary")
            st.dataframe(df_corr.describe(), use_container_width=True)
        else:
            st.info("📊 Advanced charts require data from at least 3 cities.")
    
    with tab4:
        st.subheader("📋 Data Export & Raw Data")
        
        # Create detailed DataFrame
        df = display_data_table(weather_data)
        
        # Display the data table
        st.dataframe(df, use_container_width=True, height=400)
        
        # Export options
        st.markdown("---")
        st.subheader("📥 Export Options")
        
        col_exp1, col_exp2, col_exp3, col_exp4 = st.columns(4)
        
        with col_exp1:
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="📄 Download CSV",
                data=csv_data,
                file_name=f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Download weather data as CSV file"
            )
        
        with col_exp2:
            json_data = json.dumps(weather_response, indent=2)
            st.download_button(
                label="📋 Download JSON",
                data=json_data,
                file_name=f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                help="Download raw weather data as JSON"
            )
        
        with col_exp3:
            # Summary report
            summary_report = f"""
# Weather Data Summary Report
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview
- Total Cities: {len(weather_data)}
- Average Temperature: {sum([city['current'].get('temp_c', 0) for city in weather_data]) / len(weather_data):.1f}°C
- Data Source: PostgreSQL Database
- Request ID: {weather_response.get('request_id', 'N/A')}

## Cities Included
{chr(10).join([f"- {city['location']['name']}, {city['location']['country']}" for city in weather_data])}

## Temperature Range
- Minimum: {min([city['current'].get('temp_c', 0) for city in weather_data]):.1f}°C
- Maximum: {max([city['current'].get('temp_c', 0) for city in weather_data]):.1f}°C
- Average: {sum([city['current'].get('temp_c', 0) for city in weather_data]) / len(weather_data):.1f}°C
            """
            
            st.download_button(
                label="📝 Summary Report",
                data=summary_report,
                file_name=f"weather_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                mime="text/markdown",
                help="Download summary report in Markdown format"
            )
        
        with col_exp4:
            st.info("💡 More export formats available in production deployment")
    
    # Footer
    st.markdown("---")
    
    col_footer1, col_footer2, col_footer3 = st.columns([2, 2, 2])
    
    with col_footer1:
        if st.button("🔄 Refresh Dashboard", key="footer_refresh"):
            st.rerun()
    
    with col_footer2:
        if st.button("🏠 Back to Input", key="footer_input"):
            input_page_url = SERVICE_1_URL
            st.markdown(f"""
            <script type="text/javascript">
                window.open('{input_page_url}', '_self');
            </script>
            """, unsafe_allow_html=True)
    
    with col_footer3:
        if st.button("🗃️ View All Data", key="footer_all_data"):
            st.info("Showing all available data from recent request")

if __name__ == "__main__":
    main()