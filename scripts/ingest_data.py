import pandas as pd
from download_unsplash import DATASET_PATH, DOWNLOADED_PHOTOS_PATH
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm
import argparse

import sys
sys.path.append('/Users/shreyajain/Documents/image-search/')
import clip_image_search.utils as utils
from clip_image_search import CLIPFeatureExtractor, Searcher
import requests
import json
import os
import time
import redis
from google.oauth2 import service_account
import pandas_gbq as gbq
from google.cloud import bigquery


credentials = service_account.Credentials.from_service_account_file(
    '../quizizz-mlai.json',
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../quizizz-mlai.json'

class UnsplashDataset(Dataset):
    def __init__(self):
        super().__init__()
        self.photo_files = list(DOWNLOADED_PHOTOS_PATH.glob("*.jpg"))
        df = pd.read_csv(DATASET_PATH / "photos.tsv000", sep="\t", usecols=["photo_id", "photo_image_url"])
        self.id_to_url = {photo_id: photo_image_url for photo_id, photo_image_url in df.values.tolist()}

    def __len__(self):
        return len(self.photo_files)

    def __getitem__(self, idx):
        photo_file = self.photo_files[idx]
        photo_id = photo_file.name.split(".")[0]
        image = utils.pil_loader(photo_file)
        photo_image_url = self.id_to_url[photo_id]
        return photo_id, photo_image_url, image

class QuizizzTextDataset(Dataset):
    def __init__(self):
        super().__init__()
        if(self.dump_data_to_bq()):
            print('data dumped to bigquery')
        else:
            print('error in getting data from bigquery') 

    def dump_data_to_bq(self):
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig()
        sql = ' '.join(open('get_data_from_quizizz.sql', 'r').readlines())

        try:
            # Get data from bigQuery
            print('big query for getting image search data')
            t = time.time()
            job_config.destination = client.dataset('search').table('quizizz_image_search')
            job_config.allow_large_results = True
            job_config.write_disposition = 'WRITE_TRUNCATE'
            self.df = pd.read_gbq(sql, project_id = 'quizizz-org', dialect = 'standard', credentials = credentials, progress_bar_type='tqdm')
            print('big query for image search data ran')
            return True
        
        except Exception as e:
            summary = 'user preference calculation error :' + e
            payload = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "```" + summary + "```"
                        }
                    }
                ]
            }
            print(payload)
            return False

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        quiz_name = self.df['quiz_name'][idx]
        image_url = self.df['image'][idx]
        question_text = self.df['question_text'][idx]
        option_text = self.df['option_text'][idx]
        questionId = self.df['questionId'][idx]
        quiz_quality_score = self.df['quiz_quality_score'][idx]
        return quiz_name, image_url, question_text, option_text, questionId, quiz_quality_score


def collate(batch):
    return zip(*batch)


def generate_data(dataset_key):
    if dataset_key == "UnsplashDataset":
        dataset = UnsplashDataset()
        dataloader = DataLoader(dataset, batch_size=64, shuffle=False, collate_fn=collate)
        feature_extractor = CLIPFeatureExtractor()

        for batch in tqdm(dataloader):
            photo_ids, photo_image_urls, images = batch
            image_features = feature_extractor.get_image_features(images)
            batch_size = len(photo_ids)
            for i in range(batch_size):
                yield {
                    "_index": "image",
                    "_id": photo_ids[i],
                    "url": photo_image_urls[i],
                    "feature_vector": image_features[i],
                }
    elif dataset_key == "QuizizzText":
        dataset = QuizizzTextDataset()
        dataloader = DataLoader(dataset, batch_size=64, shuffle=False, collate_fn=collate)
        for batch in tqdm(dataloader):
            quiz_name, image_url, question_text, option_text, questionId, quiz_quality_score = batch
            batch_size = len(questionId)
            for i in range(batch_size):
                yield {
                    "_index": "text",
                    "_id": questionId[i],
                    "url": image_url[i],
                    "question_text": question_text[i],
                    "option_text": option_text[i],
                    "quiz_name": quiz_name[i],
                    "question_quality_score": quiz_quality_score[i]
                }

def main():
    searcher = Searcher()

   
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset_key", default="UnsplashDataset", help="Dataset key to ingest")
    parser.add_argument("--chunk_size", type=int, default=128, help="Chunk size for ingestion")
    args = parser.parse_args()
    print("Creating an index...")
    searcher.create_index(args.dataset_key)
    print("Indexing images...")
    searcher.bulk_ingest(generate_data(args.dataset_key), chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
