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


class TMBDDownloader:
    BASE_API_URL = 'https://api.themoviedb.org/3/movie/{entry_id}'
    EXPORT_BASE_URL = 'http://files.tmdb.org/p/exports/'

    def __init__(self, api_key: str,
                 filepath: str,
                 filepath_creds: str,
                 batch_size: int = 50,
                 max_batches: int = float('inf'),
                 max_retries: int = 3
                 ):
        self.api_key = api_key
        self.config = {
            'max_recurrent_requests': 1,
            'rate_limit_delay': 1,
            'max_retries': max_retries,
            'batch_size': batch_size
        }
        self.max_batches = max_batches
        self.filepath = filepath
        self.filepath_creds = filepath_creds

    def fetch_with_retry(self, url: str):
        for attempt in range(self.config['max_retries']):
            try:
                response = requests.get(url)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    time.sleep(self.config['rate_limit_delay'])
                else:
                    break
                time.sleep(1)
            except Exception as e:
                print(f"Error fetching {url}: {e}")
        return None

    def download__ids(self) -> pd.Series:

        yesterday = datetime.now() - timedelta(days=1)
        filename = f'movie_ids_{yesterday.strftime("%m_%d_%Y")}.json.gz'

        url = f'{self.EXPORT_BASE_URL}{filename}'

        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"Could not download IDs")

        with gzip.open(BytesIO(response.content), 'rt', encoding='utf-8') as f:
            ids_data = [json.loads(line) for line in f]

        df = pd.DataFrame(ids_data)
        return df['id']

    def fetch_entry_details(self, entry_id):
        url = f"{self.BASE_API_URL.format(entry_id=entry_id)}"
        url += f"?api_key={self.api_key}"
        url += f'&append_to_response=credits,keywords'
        return self.fetch_with_retry(url)

    @staticmethod
    def jsonify(entry):
        return entry if isinstance(entry, str) else json.dumps(entry)

    @staticmethod
    def extract_credits(df: pd.DataFrame) -> pd.DataFrame:
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

    def process_and_export(self, entries):
        if not entries:
            return
        df = pd.DataFrame(entries)
        df = df[df['id'].notna() & df['id'].astype(str).str.isnumeric()]

        creds = self.extract_credits(df)
        df = df.drop(columns=['credits'])

        for col in df.columns:
            df[col] = df[col].apply(self.jsonify)

        creds.to_csv(self.filepath_creds, mode='a', header=not os.path.exists(self.filepath_creds), index=False)
        df.to_csv(self.filepath, mode='a', header=not os.path.exists(self.filepath), index=False)

    def download_entries(self, id_list: List[int]):
        if os.path.exists(self.filepath):
            existing = set(pd.read_csv(f'movie_data.csv', usecols=['id'], dtype=str)['id'])
            id_list = [id for id in id_list if str(id) not in existing]
        print(f"total_ids_found: {len(id_list)}")
        runs = min(self.max_batches * (self.config['batch_size']), len(id_list))
        for i in tqdm(range(0, runs, self.config['batch_size'])):
            batch = id_list[i: i + self.config['batch_size']]

            results = []
            for entry_id in batch:
                result = self.fetch_entry_details(entry_id)
                if result:
                    results.append(result)
                time.sleep(self.config['rate_limit_delay'])
            self.process_and_export(results)
            # print(f'Processed batch {i // self.config["batch_size"] + 1}')

    def download_data(self):
        print(f"Download started")

        ids = self.download__ids()
        ids = ids.tolist()

        self.download_entries(ids)
