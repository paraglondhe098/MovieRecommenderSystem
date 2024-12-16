import streamlit as st
import pandas as pd
import requests
from utils.recommender import Recommender
from utils.general import load_kwargs

# Page Configuration
st.set_page_config(
    page_title="Movie Recommendation Engine",
    page_icon="🎬",
    layout="wide"
)

# Custom CSS for enhanced styling
st.markdown("""
    <style>
    .big-font {
        font-size:20px !important;
        font-weight:bold;
        color:#2C3E50;
    }
    .movie-card {
        background-color:#F7F9FC;
        border-radius:10px;
        padding:15px;
        margin-bottom:10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: transform 0.3s;
    }
    .movie-card:hover {
        transform: scale(1.02);
    }
    </style>
""", unsafe_allow_html=True)

# Title and Description
st.title("🎬 Cinematic Recommendations")
st.markdown("Discover movies that match your taste! Find recommendations by title, keywords, or ID.",
            help="Our recommendation engine uses advanced semantic similarity to find movies you'll love.")

# Initialize the Recommender (you'll need to adjust these arguments based on your setup)
@st.cache_resource
def load_recommender():
    return Recommender.from_weaviate(**load_kwargs("config/weaviate.yaml"))

recommender = load_recommender()

# Sidebar for Search Options
st.sidebar.header("🔍 Recommendation Options")
search_type = st.sidebar.selectbox(
    "Search By",
    ["Movie Title", "Partial Title/Keyword", "Movie ID"]
)

# Number of recommendations
num_recommendations = st.sidebar.slider(
    "Number of Recommendations",
    min_value=3,
    max_value=20,
    value=10
)

# Search Input based on selection
if search_type == "Movie Title":
    titles = sorted(recommender.metadata['title'].unique())
    selected_title = st.sidebar.selectbox("Select Movie", titles)
    query = selected_title
elif search_type == "Partial Title/Keyword":
    keyword = st.sidebar.text_input("Enter Movie Keyword")
    query = keyword
else:  # Movie ID
    movie_ids = sorted(recommender.metadata['id'].unique())
    selected_id = st.sidebar.selectbox("Select Movie ID", movie_ids)
    query = selected_id

# Recommendation Button
if st.sidebar.button("Get Recommendations"):
    try:
        # Perform recommendation based on search type
        if search_type == "Movie Title":
            recommendations = recommender.get_recommendations_by_title(query, k=num_recommendations)
        elif search_type == "Partial Title/Keyword":
            recommendations = recommender.get_recommendations_by_keywords(query, k=num_recommendations)
        else:  # Movie ID
            recommendations = recommender.get_recommendations_by_id(query, k=num_recommendations)

        # Display Recommendations
        if recommendations is not None and not recommendations.empty:
            st.success(f"Found {len(recommendations)} similar movies!")

            # Create columns for movie display
            cols = st.columns(3)

            for idx, movie in recommendations.iterrows():
                col = cols[idx % 3]

                with col:

                    with st.expander(movie['movie']):
                        st.image(Recommender.get_poster(movie.get('poster_path')), width=150)
                        st.write(f"**Release Date:** {movie.get('release_date', 'N/A')}")
                        st.write(f"**Genres:** {movie.get('genres', 'N/A')}")
                        st.write(f"**Similarity Score:** {movie.get('similarity_score', 'N/A')}")
                        st.write(f"**Overview:** {movie.get('synopsis', 'No overview available')}")
        else:
            st.warning("No recommendations found.")

    except Exception as e:
        st.error(f"An error occurred: {e}")

# About Section
st.sidebar.markdown("---")
st.sidebar.info("""
    ### About This App
    - Uses semantic similarity for movie recommendations
    - Powered by Weaviate Vector Database
    - Embedding Model: Sentence Transformers
""")