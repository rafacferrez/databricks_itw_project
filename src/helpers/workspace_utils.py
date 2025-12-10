from databricks.sdk import WorkspaceClient
import io
import csv
from typing import List, Dict


def save_data_to_buffer(data: List[Dict], field_names: List[str]) -> io.StringIO:
    """
    Save the user dataset to an in-memory buffer as CSV.
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=field_names
    )
    writer.writeheader()
    writer.writerows(data)
    buffer.seek(0)
    return buffer


def upload_buffer_data_to_databricks(buffer: io.StringIO, filename: str, base_path: str) -> None:
    """
    Upload the in-memory buffer data to the Databricks volume using WorkspaceClient.
    """
    client = WorkspaceClient()
    destination_path = f"{base_path}/{filename}"
    client.files.upload(destination_path, buffer.getvalue().encode('utf-8'), overwrite=True)
    print(f"Upload completed: {destination_path}")


def upload_buffer_data_to_local(buffer: io.StringIO, filename: str, base_path: str) -> None:
    """
    Upload the in-memory buffer data to the Databricks volume using WorkspaceClient.
    """
    destination_path = f"{base_path}/{filename}"

    with open(destination_path, "wb") as f:
        f.write(buffer.getvalue().encode('utf-8'))

    print(f"Upload completed: {destination_path}")
