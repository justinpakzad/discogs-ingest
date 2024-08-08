from parser import *
from writer import *
from writers_config import *
import time
from downloader import S3DiscogsDowloader
from pathlib import Path
import os
from tqdm import tqdm
import argparse
import os

bucket_name = "discogs-data-dumps"
prefix = "data/"


def get_args():
    parser = argparse.ArgumentParser(description="Discogs Ingest")
    parser.add_argument(
        "--dir", help="Enter directory to save files to.", required=True
    )
    parser.add_argument(
        "--raw_dir", help="Enter directory of raw data dumps", required=False
    )
    parser.add_argument(
        "--sample",
        help="To only process a sample of the data (50k)",
        action="store_true",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Option to download most recent data dumps",
        required=False,
    )
    args = parser.parse_args()
    return args


def main():
    base_path = Path(os.path.abspath("")).parent

    args = get_args()

    if args.download:
        S3DiscogsDowloader(bucket_name, prefix).run(directory=f"{base_path}/raw_data")

    csv_path = base_path / args.csv_dir if args.csv_dir else None
    os.makedirs(csv_path, exist_ok=True)
    writers = setup_writers(csv_path=csv_path)
    raw_data_path = base_path / args.raw_dir if args.raw_dir else base_path / "raw_data"
    label_file = raw_data_path / "discogs_20240701_labels.xml.gz"
    artist_file = raw_data_path / "discogs_20240701_artists.xml.gz"
    release_file = raw_data_path / "discogs_20240701_releases.xml.gz"
    master_file = raw_data_path / "discogs_20240701_masters.xml.gz"
    label_parser = LabelParser(file_path=label_file, sample=args.sample)
    artist_parser = ArtistParser(file_path=artist_file, sample=args.sample)
    release_parser = ReleasesParser(file_path=release_file, sample=args.sample)
    master_parser = MasterParser(file_path=master_file, sample=args.sample)
    data_map = {
        "label": label_parser,
        "artist": artist_parser,
        "release": release_parser,
        "master": master_parser,
    }
    start = time.time()
    for category, writer in tqdm(writers.items()):
        cat = category.split("_")[0]
        parser = data_map.get(cat)
        writer.write_rows(parser.parse_file())
    end = time.time()
    print(f"Time taken {end - start}")


if __name__ == "__main__":
    main()
