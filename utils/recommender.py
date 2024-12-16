import weaviate
from langchain.vectorstores import Weaviate
import pandas as pd
from langchain_huggingface import HuggingFaceEmbeddings
from utils.api_keys import fetch_api_key


class Recommender:
    def __init__(self, vectorstore, metadata_path):

        self.vectorstore = vectorstore
        self.metadata = pd.read_csv(metadata_path)

    @classmethod
    def from_weaviate(cls, metadata_path, embedding_model_args, weaviate_args):
        client = weaviate.Client(
            url=weaviate_args['url'],
            auth_client_secret=weaviate.AuthApiKey(fetch_api_key(weaviate_args['ak_name'], False))
        )
        embeddings = HuggingFaceEmbeddings(
            model_name=embedding_model_args['model_name'],
            model_kwargs=embedding_model_args['model_kwargs']
        )
        vectorstore = Weaviate(client=client, embedding=embeddings,
                               index_name=weaviate_args['index_name'],
                               text_key=weaviate_args['text_key'],
                               by_text=weaviate_args['by_text'],
                               attributes=weaviate_args['attributes'],
                               )
        rec = cls(metadata_path=metadata_path, vectorstore=vectorstore)
        return rec

    def guess_movie(self, keyword):
        return self.metadata[self.metadata['title'].str.contains(keyword)]['title'].values[0]

    def get_recommendations_by_id(self, tmdb_id, k=10):
        if tmdb_id not in self.metadata.id:
            raise ValueError(f"Id '{tmdb_id}' not found in indices")
        querry = self.metadata[self.metadata['id'] == tmdb_id][['soup']].values[0]
        return self.recommend(querry[0], k)

    def get_recommendations_by_title(self, title, k=10):
        if title not in list(self.metadata.title):
            raise ValueError(f"title '{title}' not found in indices")
        querry = self.metadata[self.metadata['title'] == title][['soup']].values[0]
        return self.recommend(querry[0], k)

    def get_recommendations_by_keywords(self, keyword, k=10):
        title = self.guess_movie(keyword)
        return self.get_recommendations_by_title(title, k=k)

    @staticmethod
    def get_poster(poster_path):
        return "https://image.tmdb.org/t/p/original/" + poster_path

    def recommend(self, query, k):
        try:
            results = self.vectorstore.similarity_search_with_score(query, k=k + 1)

            top_k = []

            for x in results[1:]:
                movie_metadata = {
                    'movie': x[0].metadata['movie'],
                    "tmdb_id": x[0].metadata['tmdb_id'],
                    "imdb_id": x[0].metadata['imdb_id'],
                    'genres': x[0].metadata['genres'],
                    "release_date": x[0].metadata['release_date'],
                    "cast": x[0].metadata['cast'],
                    "crew": x[0].metadata['crew'],
                    "collection": x[0].metadata['collection'],
                    "budget": x[0].metadata['budget'],
                    "revenue": x[0].metadata['revenue'],
                    "runtime": x[0].metadata['runtime'],
                    "language": x[0].metadata['language'],
                    "popularity": x[0].metadata['popularity'],
                    "synopsis": x[0].metadata['synopsis'],
                    "poster_path": x[0].metadata['poster_path'],
                    "homepage": x[0].metadata['homepage'],
                    'similarity_score': round(x[1], 2),
                }
                top_k.append(movie_metadata)

            df_top_k = pd.DataFrame(top_k)
            return df_top_k

        except Exception as e:
            print(f"Error during query: {e}")
            return None
