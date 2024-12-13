import pandas as pd
import logging
from pathlib import Path

def split_data_80():
    logging.basicConfig(level=logging.DEBUG)
    data_path = Path("data/ml-latest-small/ratings.csv")
    train_path = Path("/shared_data/train.csv")
    test_path = Path("/shared_data/test.csv")

    ratings = pd.read_csv(data_path)
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    ratings = ratings.sort_values('timestamp')
    split_index = int(len(ratings) * 0.8)

    train = ratings.iloc[:split_index]
    test = ratings.iloc[split_index:]

    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)

    logging.info(f"Data split into train ({len(train)}) and test ({len(test)}) sets.")
