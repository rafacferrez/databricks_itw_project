import random
import datetime
from faker import Faker
from typing import Dict
from helpers import utils, workspace_utils


fake = Faker()


class UsersGenerator:
    def __init__(
        self,
        env: str,
        num_users: int = 50,
        start_id: int = 1001,
        end_id: int = 1050,
        base_date: datetime.date = datetime.date(2025, 12, 10),
    ):
        self.name = "user"
        self.field_names = ["user_id", "name", "email", "phone", "is_active", "last_modified"]
        self.base_path = f"/Volumes/sales_{env}/{utils.get_base_user_schema()}_bronze/raw_files/users"
        self.num_users = num_users
        self.start_id = start_id
        self.end_id = end_id
        self.base_date = base_date
        self.users = self._generate_initial_users()

    def _generate_email_from_name(self, name: str) -> str:
        """
        Generate an email based on the user's name with a random domain.
        """
        local_part = name.lower().replace(" ", ".")
        domain = random.choice(["gmail.com", "outlook.com", "mail.com"])
        return f"{local_part}@{domain}"


    def _generate_initial_users(self) -> Dict[int, Dict]:
        """
        Generate the initial user dataset with unique IDs, names, emails, phones,
        is_active status, and last_modified date.
        """
        users: Dict[int, Dict] = {}
        for uid in range(self.start_id, self.end_id + 1):
            name = fake.name()
            users[uid] = {
                "user_id": uid,
                "name": name,
                "email": self._generate_email_from_name(name),
                "phone": fake.phone_number(),
                "is_active": random.choice([True, False]),
                "last_modified": self.base_date.isoformat(),
            }
        return users

    def generate_new_file(self) -> None:
        """
        Generate daily updates for the user dataset and upload each day to Databricks.
        """

        # Build filename and upload to Databricks volume
        filename = f"users_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        all_rows = list(self.users.values())
        buffer = workspace_utils.save_data_to_buffer(all_rows, self.field_names)
        workspace_utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)

        print(f"OK {filename} — {len(all_rows)} rows")
