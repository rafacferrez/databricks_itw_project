from typing import Iterator
import fnmatch

from databricks.sdk import WorkspaceClient
from helpers import utils

w = WorkspaceClient() #  -> serverless only
dbutils = w.dbutils #  -> serverless only


def copy_files_between_locations(
    origin_path: str,
    destination_path: str,
    file_extension: str = None,
    name_pattern: str = None,
    start_from: int = 1,
) -> Iterator[str]:
    """
    Iterator to copy one file at a time from origin_path to destination_path.

    Parameters:
        origin_path (str): Source folder in DBFS.
        destination_path (str): Destination folder in DBFS.
        file_extension (str, optional): Filter files by extension, e.g., '.csv'.
        name_pattern (str, optional): Filter files by a glob pattern, e.g., 'data_*'.
        start_from (int, optional): 1-based index to start copying from in the sorted list. Default is 1.

    Yields:
        str: The name of the copied file.
    """
    # List all files in the origin path
    files = [f.path for f in dbutils.fs.ls(origin_path) if not f.path.endswith('/')]

    # Filter by extension if provided
    if file_extension:
        files = [f for f in files if f.lower().endswith(file_extension.lower())]

    # Filter by name pattern if provided
    if name_pattern:
        files = [f for f in files if fnmatch.fnmatch(f.split('/')[-1], name_pattern)]

    # Sort files by name
    files.sort()

    # Convert start_from to zero-based index
    start_index = max(start_from - 1, 0)

    # Skip until start_from
    for file_path in files[start_index:]:
        file_name = file_path.split('/')[-1]
        dest_file_path = f"{destination_path}/{file_name}"
        dbutils.fs.cp(file_path, dest_file_path)
        yield file_name


def copy_base_files(table_name: str, env: str, num_files: int = 0):
    """
    Copies the first `num_files` files (by name order) from source to destination.
    If `num_files` is 0, all files are copied.
    """
    origin_path = f"/Volumes/capstone_src/{env}_bronze/raw_files/{table_name}"
    destination_path = f"/Volumes/capstone_{env}/{utils.get_base_user_schema()}_bronze/raw_files/{table_name}"
    print(f"Copying files from {origin_path} to {destination_path}")

    it = 1
    for file_name in copy_files_between_locations(origin_path, destination_path, ".csv", table_name + "*"):
        print(f"{file_name} Copied")
        if it == num_files:
            break
        it += 1


def copy_extra_files(table_name: str, env: str, start_from: int, num_files: int = 1):
    """
    Copies `num_files` files starting from the `start_from` position (1-based index).
    """
    origin_path = f"/Volumes/capstone_src/{env}_bronze/raw_files/{table_name}"
    destination_path = f"/Volumes/capstone_{env}/{utils.get_base_user_schema()}_bronze/raw_files/{table_name}"
    print(f"Copying files from {origin_path} to {destination_path}")

    it = 1
    for file_name in copy_files_between_locations(origin_path, destination_path, ".csv", table_name + "*", start_from):
        print(f"{file_name} Copied")
        if it == num_files:
            break
        it += 1
