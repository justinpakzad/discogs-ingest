import duckdb
import argparse
from tqdm import tqdm


def get_args():
    parser = argparse.ArgumentParser(description="Discogs Ingest")
    parser.add_argument("--db", required=True)
    parser.add_argument("--csvs", required=True)

    args = parser.parse_args()
    return args


def create_tables(db_path, csv_path):
    con = duckdb.connect(f"{db_path}/discogs.db")
    tables_and_files = {
        "artist_alias": "artist_alias.csv",
        "artist_name_variation": "artist_name_variation.csv",
        "artist_url": "artist_url.csv",
        "artist": "artist.csv",
        "label_url": "label_url.csv",
        "label": "label.csv",
        "master_style": "master_style.csv",
        "master_video": "master_video.csv",
        "master_artist": "master_artist.csv",
        "master": "master.csv",
        "release_artist": "release_artist.csv",
        "release_company": "release_company.csv",
        "release_extra_artist": "release_extra_artist.csv",
        "release_style": "release_style.csv",
        "release_track": "release_tracks.csv",
        "release_video": "release_video.csv",
        "release": "release.csv",
        "sub_label": "sub_label.csv",
    }
    for table_name, file_name in tqdm(tables_and_files.items()):
        full_file_path = f"{csv_path}/{file_name}"
        drop_query = f"DROP TABLE IF EXISTS {table_name}"
        con.execute(drop_query)
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{full_file_path}')"
        con.execute(query)
        print(f"Table {table_name} created from {file_name}")


def main():
    args = get_args()

    create_tables(args.db, args.csvs)


if __name__ == "__main__":
    main()
