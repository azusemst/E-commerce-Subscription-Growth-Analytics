"""
AWS S3 Client Module
Handles all AWS S3 operations for data upload/download
"""

import boto3
import pandas as pd
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class AWSS3Client:
    """
    AWS S3 Client for uploading and downloading data.

    After testing in Notebook 2, complete all TODO methods here.
    """

    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """
        Initialize S3 client.

        Args:
            bucket_name: Name of the S3 bucket
            aws_access_key_id: AWS access key (optional, uses credentials from environment if not provided)
            aws_secret_access_key: AWS secret key (optional)
        """
        self.bucket_name = bucket_name

        # TODO: Initialize boto3 S3 client with credentials if provided
        # If not provided, boto3 will automatically use environment variables or IAM role
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def upload_dataframe_to_s3(self, df: pd.DataFrame, file_key: str) -> bool:
        """
        Upload pandas DataFrame to S3 as CSV.

        TODO: In Notebook 2, test this function and complete the implementation.
        Convert pandas DataFrame to CSV bytes stream and upload to S3.
        This data can be used by Tableau or downstream applications.

        Args:
            df: pandas DataFrame to upload
            file_key: S3 object key (path in bucket, e.g., 'data/rfm_segments.csv')

        Returns:
            bool: True if upload successful, False otherwise
        """

        if file_key.endswith(".csv"):
            df_bytes = df.to_csv(index=False).encode("utf-8")
        elif file_key.endswith(".parquet"):
            df_bytes = df.to_parquet(index=False, engine="pyarrow")
        else:
            print(f"Unsupported file type")
            return False
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=file_key, Body=df_bytes)
            return True
        except Exception as e:
            print(f"Error uploading DataFrame to S3: {e}")
            return False

    def upload_file_to_s3(self, file_path: str, file_key: str) -> bool:
        """
        Upload a local file to S3.

        Args:
            file_path: Local path to the file to upload
            file_key: S3 object key (path in bucket)

        Returns:
            bool: True if upload successful, False otherwise
        """

        try:
            self.s3.upload_file(file_path, self.bucket_name, file_key)
            return True
        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            return False

    def download_csv_from_s3(self, file_key: str) -> Optional[pd.DataFrame]:
        """
        Download CSV file from S3 and return as pandas DataFrame.

        TODO: Complete function to read data from S3 and return DataFrame.

        Args:
            file_key: S3 object key (path in bucket)

        Returns:
            pd.DataFrame: Downloaded data, or None if download failed
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        except Exception as e:
            print(f"Error downloading CSV from S3: {e}")
            return None
        df = pd.read_csv(response["Body"])
        return df

    def list_objects_in_bucket(self, prefix: str = "") -> list:
        """
        List all objects in the S3 bucket with given prefix.

        Args:
            prefix: Optional prefix to filter objects

        Returns:
            list: List of object keys in the bucket
        """
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        if "Contents" in response:
            return [obj["Key"] for obj in response["Contents"]]
        else:
            return []

    def delete_object_from_s3(self, file_key: str) -> bool:
        """
        Delete an object from S3.

        Args:
            file_key: S3 object key to delete

        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=file_key)
            return True
        except Exception as e:
            print(f"Error deleting object from S3: {e}")
            return False
