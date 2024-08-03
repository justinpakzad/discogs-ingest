import boto3
import os
from datetime import datetime
from tqdm.contrib.concurrent import thread_map
from botocore.config import Config
from botocore import UNSIGNED

bucket_name = "discogs-data-dumps"
prefix = "data/"


class S3DiscogsDowloader:
    """
    Downloads most recent discogs data dump from public S3 Bucket.
    """

    def __init__(self, bucket_name, prefix) -> None:
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def s3_list_objects(self):
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.prefix
            )
            return response
        except Exception as e:
            raise RuntimeError(
                f"Error fetching objects from bucket {self.bucket_name} with prefix {self.prefix}: {e}"
            )

    def filter_files(self, response):
        files = []
        if not response:
            return None
        for content in response.get("Contents", []):
            file_name = content.get("Key", [])
            last_modified = content.get("LastModified", [])
            year = last_modified.year
            month = last_modified.month
            current_year = int(datetime.now().year)
            current_month = int(datetime.now().month)

            if year == current_year and month == current_month:
                files.append(file_name)
        return files

    def download_file(self, item, directory):
        file_name = item.split("/")[-1]
        if not os.path.exists(directory):
            os.mkdir(directory)
        folder_path = os.path.join(directory, file_name)
        try:
            self.s3_client.download_file(self.bucket_name, item, folder_path)
        except Exception as e:
            raise RuntimeError(f"Error downloading file {file_name} from {item}: {e}")

    def download_files(self, files, directory):
        thread_map(
            lambda file: self.download_file(file, directory),
            files,
            max_workers=5,
            desc="Downloading files",
        )

    def run(self, directory):
        response = self.s3_list_objects()
        file_list = self.filter_files(response)
        self.download_files(file_list, directory)


# discogs_downloader = S3DiscogsDowloader(bucket_name, prefix)
# discogs_downloader.run(directory="data")
