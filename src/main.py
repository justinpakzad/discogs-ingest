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
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def get_parsers(raw_data_path, args):
    files_and_parsers = {
        "label": ("discogs_20240701_labels.xml.gz", LabelParser),
        "artist": ("discogs_20240701_artists.xml.gz", ArtistParser),
        "release": ("discogs_20240701_releases.xml.gz", ReleaseParser),
        "master": ("discogs_20240701_masters.xml.gz", MasterParser),
    }
    parsers = {}
    for key, (filename, parser) in files_and_parsers.items():
        file_path = raw_data_path / filename
        parsers[key.split("_")[0]] = parser(file_path=file_path, sample=args.sample)
    return parsers


def process_data(writer, parser):
    start_time = time.time()
    parsed_data = parser.parse_file()
    writer.write_rows(parsed_data)
    writer.close_file()
    end_time = time.time()
    duration = (end_time - start_time) / 60
    return f"{writer.__class__.__name__} completed in {duration:.2f} minutes."


def main():
    base_path = Path(os.path.abspath("")).parent

    args = get_args()

    if args.download:
        S3DiscogsDowloader(bucket_name, prefix).run(directory=f"{base_path}/raw_data")

    csv_path = base_path / args.dir if args.dir else None
    os.makedirs(csv_path, exist_ok=True)
    raw_data_path = base_path / args.raw_dir if args.raw_dir else base_path / "raw_data"
    writers = setup_writers(csv_path=csv_path)
    parsers = get_parsers(raw_data_path, args)
    with ThreadPoolExecutor(max_workers=len(parsers)) as executor:
        futures = {}
        for writer_name, writer in writers.items():
            base_category = writer_name.split("_")[0]
            if base_category in parsers:
                future = executor.submit(process_data, writer, parsers[base_category])
                futures[future] = writer_name
            else:
                print(f"No parser available for {base_category}")

        for future in as_completed(futures):
            writer_name = futures[future]
            try:
                result = future.result()
                print(f"{writer_name}: {result}")
            except Exception as exc:
                print(f"Error processing {writer_name}: {exc}")


if __name__ == "__main__":
    main()
