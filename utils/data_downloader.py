import requests
import json
import os
import pandas as pd
import time
from tqdm import tqdm
import gzip
import time
from typing import List, Dict, Optional, Set, Tuple
from datetime import datetime, timedelta
from io import BytesIO


class TMDBDataDownloader:
    BASE_API_URL = 'https://api.themoviedb.org/3/{category}/{entry_id}'
    EXPORT_BASE_URL = 'http://files.tmdb.org/p/exports/'

    def __init__(self, api_key: str, categories: Tuple[str] = ('movie',)):
        """
        Initialize the TMDB data downloader

        :param api_key: TMDB API key
        :param categories: Tuple of categories to download
        """
        self.api_key = api_key
        self.categories = categories

        # Configuration for API calls
        self.config = {
            'max_concurrent_requests': 1,  # Removed async parallelism
            'rate_limit_delay': 1,  # seconds between requests
            'max_retries': 3,
            'download_batch_size': 50
        }

        # Columns to drop from the dataset
        self.columns_to_drop: Set[str] = {
            'adult', 'backdrop_path', 'belongs_to_collection', 'profile_path', 'video'
        }

        # Columns to convert to JSON
        self.json_columns: Set[str] = {
            'genres', 'keywords', 'production_countries',
            'production_companies', 'spoken_languages'
        }

    def fetch_with_retry(self, url: str) -> Optional[Dict]:
        """
        Fetch data from URL with retry mechanism

        :param url: URL to fetch
        :return: JSON response or None
        """
        for attempt in range(self.config['max_retries']):
            try:
                response = requests.get(url)

                if response.status_code == 200:
                    return response.json()

                # Handle rate limiting
                if response.status_code == 429:
                    time.sleep(self.config['rate_limit_delay'])
                else:
                    break

                time.sleep(1)  # Backoff between retries
            except Exception as e:
                print(f"Error fetching {url}: {e}")

        return None

    def download_category_ids(self, category: str) -> pd.DataFrame:
        """
        Download list of IDs for a specific category

        :param category: Category to download IDs for
        :return: DataFrame of IDs
        """
        # Generate filename based on previous day's date
        yesterday = datetime.now() - timedelta(days=1)
        filename = f'{category}_ids_{yesterday.strftime("%m_%d_%Y")}.json.gz'

        url = f'{self.EXPORT_BASE_URL}{filename}'

        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Could not download IDs for {category}")

        # Decompress and parse gzipped file
        with gzip.open(BytesIO(response.content), 'rt', encoding='utf-8') as f:
            ids_data = [json.loads(line) for line in f]

        df = pd.DataFrame(ids_data)

        # Filter out non-movie entries and adult content
        if 'original_title' in df.columns:
            df = df[~df.original_title.str.contains(' Collection', na=False)]

        if 'adult' in df.columns:
            df = df[~df['adult']]

        return df

    def fetch_entry_details(self, entry_id: int, category: str) -> Optional[Dict]:
        """
        Fetch details for a specific entry

        :param entry_id: ID of the entry
        :param category: Category of the entry
        :return: Entry details or None
        """
        params = {
            'api_key': self.api_key,
            'append_to_response': 'credits,keywords' if category == 'movie' else ''
        }

        url = f'{self.BASE_API_URL.format(category=category, entry_id=entry_id)}'
        url += f'?api_key={self.api_key}'
        if params['append_to_response']:
            url += f'&append_to_response={params["append_to_response"]}'

        return self.fetch_with_retry(url)

    def download_entries(self, category: str, id_list: List[int]):
        """
        Download details for entries in batches

        :param category: Category to download
        :param id_list: List of entry IDs
        """
        # Remove already downloaded entries
        if os.path.exists(f'{category}_data.csv'):
            existing_ids = set(pd.read_csv(f'{category}_data.csv', usecols=['id'], dtype=str)['id'])
            id_list = [id for id in id_list if str(id) not in existing_ids]

        for i in range(0, len(id_list), self.config['download_batch_size']):
            batch = id_list[i:i + self.config['download_batch_size']]

            # Sequential downloads
            results = []
            for entry_id in batch:
                result = self.fetch_entry_details(entry_id, category)
                if result:
                    results.append(result)
                time.sleep(self.config['rate_limit_delay'])  # Respect rate limits

            # Process and save results
            self.process_and_export_data(category, results)

            print(f'Processed batch {i // self.config["download_batch_size"] + 1}')

    def process_and_export_data(self, category: str, entries: List[Dict]):
        """
        Process and export downloaded data

        :param category: Category of entries
        :param entries: List of entry details
        """
        if not entries:
            return

        df = pd.DataFrame(entries)

        # Drop unnecessary columns
        df = df.drop(columns=[col for col in self.columns_to_drop if col in df.columns])

        # Filter out entries without valid IDs
        df = df[df['id'].notna() & df['id'].astype(str).str.isnumeric()]

        # Process JSON columns
        for column in self.json_columns:
            if column in df.columns:
                df[column] = df[column].apply(json.dumps)

        # Special handling for movie credits
        if 'credits' in df.columns:
            credits_df = self.extract_credits(df)
            credits_df.to_csv(f'data/{category}_credits.csv', mode='a', header=not os.path.exists(f'data/{category}_credits.csv'),
                              index=False)
            df = df.drop(columns=['credits'])

        # Export data
        df.to_csv(f'data/{category}_data.csv', mode='a', header=not os.path.exists(f'data/{category}_data.csv'), index=False)

    def extract_credits(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract credits from movie details

        :param df: DataFrame containing movie details
        :return: DataFrame of credits
        """
        credits_data = []
        for _, row in df.iterrows():
            if 'credits' in row and row['credits']:
                movie_credits = {
                    'movie_id': row['id'],
                    'movie_title': row.get('title', ''),
                    'cast': json.dumps([
                        {k: v for k, v in cast.items() if k != 'profile_path'}
                        for cast in row['credits'].get('cast', [])
                    ]),
                    'crew': json.dumps([
                        {k: v for k, v in crew.items() if k != 'profile_path'}
                        for crew in row['credits'].get('crew', [])
                    ])
                }
                credits_data.append(movie_credits)

        return pd.DataFrame(credits_data)

    def download_all_data(self):
        """
        Download data for all specified categories
        """
        for category in self.categories:
            print(f'Processing category: {category}')

            # Get list of IDs
            id_df = self.download_category_ids(category)
            id_list = id_df['id'].tolist()

            # Download and process entries
            self.download_entries(category, id_list)

            print(f'Completed download for {category}')
