import random
import datetime
from faker import Faker
from typing import Dict

from instructors.src.helpers import workspace_utils as utils


fake = Faker()


class UsersGenerator:
    def __init__(
        self,
        base_path: str,
        num_users: int = 50,
        start_id: int = 1001,
        end_id: int = 1050,
        base_date: datetime.date = datetime.date(2025, 10, 1),
    ):
        self.name = "user"
        self.field_names = ["user_id", "name", "email", "phone", "is_active", "last_modified"]
        self.num_days = 0

        self.base_path = base_path
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

    def _update_users_daily(self, current_date: datetime.date):
        """
        Update the user dataset to simulate daily changes.
        Each user has a chance to change email, phone, is_active status,
        and last_modified timestamp.
        """
        for uid, u in self.users.items():
            updated = False
            if random.random() < 0.2:
                u["email"] = self._generate_email_from_name(u["name"])
                updated = True
            if random.random() < 0.2:
                u["phone"] = fake.phone_number()
                updated = True
            if random.random() < 0.1:
                u["is_active"] = not u["is_active"]
                updated = True
            if updated:
                u["last_modified"] = current_date.isoformat()

    def generate_new_file(self) -> None:
        """
        Generate daily updates for the user dataset and upload each day to Databricks.
        """
        day_offset = self.num_days
        current_date = self.base_date + datetime.timedelta(days=day_offset)
        
        # Apply daily updates after the first day
        if day_offset > 0:
            self._update_users_daily(current_date)

        # Build filename and upload to Databricks volume
        filename = f"users_{current_date.strftime('%Y%m%d')}.csv"
        all_rows = list(self.users.values())
        buffer = utils.save_data_to_buffer(all_rows, self.field_names)
        utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)
        #utils.upload_buffer_data_to_local(buffer, filename, self.base_path)
        self.num_days += 1

        print(f"OK {filename} — {len(all_rows)} rows")
