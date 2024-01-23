
### Install dependencies

```
pip install -e . --no-cache-dir
```

### Download the Unsplash dataset

```
python scripts/download_unsplash.py --image_width=480 --threads_count=32
```

### Create index and upload image feature vectors to Elasticsearch

```
python scripts/ingest_data.py
```

