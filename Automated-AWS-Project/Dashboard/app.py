import streamlit as st
import pandas as pd
import plotly.express as px

# 1. Page Configuration
st.set_page_config(
    page_title="Global YouTube Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# 2. Title & Project Header
st.title("📊 Global YouTube Trending Analytics Dashboard")
st.markdown("""
### **Production Data Layer:** `yt_pipeline_gold.vw_global_youtube_master`
*This dashboard reads live from our production data lake optimized via AWS Lambda, Glue PySpark, and Amazon Athena.*
""")
st.write("---")

# 3. Load Dataset Safely
@st.cache_data
def load_data():
    # Reads the CSV you pulled from your Athena Gold Layer View
    df = pd.read_csv("C:\\Abhay Folder\\Youtube AWS project\\Automated-youtube-data-piepline-aws-s3-lambda-glue-athena-stepfunction\\Dashboard\\cleaned_youtube_trending_data.csv.csv")
    # Clean up column spaces and ensure correct types
    df.columns = df.columns.str.strip()
    # FIX: Fill any empty/missing values in metric columns with 0
    metric_cols = ['view_count', 'like_count', 'comment_count']
    for col in metric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

try:
    df = load_data()

    # 4. Top KPIs Metric Row
    total_views = df['view_count'].sum()
    total_likes = df['like_count'].sum()
    unique_channels = df['channel_title'].nunique()

    col1, col2, col3 = st.columns(3)
    col1.metric("📬 Total Dataset Views", f"{total_views:,}")
    col2.metric("👍 Total Engagement (Likes)", f"{total_likes:,}")
    col3.metric("📺 Unique Channels Trending", f"{unique_channels:,}")

    st.write("---")

    # 5. Data Visualizations (Side-by-Side Columns)
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("🔥 Top 10 Trending Categories")
        # Aggregating count of unique videos per category
        category_df = df['category_name'].value_counts().reset_index(name='Video Count')
        fig_donut = px.pie(category_df.head(10), values='Video Count', names='category_name', hole=0.4,
                           color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_donut, use_container_width=True)

    with chart_col2:
        st.subheader("👑 Top 10 Channels by Views")
        # Aggregating sum of views per channel
        channel_df = df.groupby('channel_title')['view_count'].sum().reset_index()
        channel_df = channel_df.sort_values(by='view_count', ascending=False).head(10)
        fig_bar = px.bar(channel_df, x='view_count', y='channel_title', orientation='h',
                         labels={'view_count': 'Total Views', 'channel_title': 'Channel Name'},
                         color='view_count', color_continuous_scale='Reds')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    st.write("---")

    # 6. Scatter Plot Engagement Section
    st.subheader("📈 Viral Engagement Matrix: Views vs Likes")
    fig_scatter = px.scatter(df, x='view_count', y='like_count', color='category_name',
                             hover_name='title', size='comment_count',
                             labels={'view_count': 'Views', 'like_count': 'Likes'},
                             title="Bubble size relates to Comment Count")
    st.plotly_chart(fig_scatter, use_container_width=True)

    # 7. Raw Data Showcase
    with st.expander("🔍 Inspect Production Gold Dataset"):
        st.dataframe(df)

except Exception as e:
    st.error(f"Error loading dashboard dataset: {e}")
    st.info("Make sure 'cleaned_youtube_trending_data.csv' is saved in your VS Code folder.")

