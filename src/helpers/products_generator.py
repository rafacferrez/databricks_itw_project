import random
import datetime
from typing import Dict, List

from instructors.src.helpers import workspace_utils as utils


class ProductsGenerator:
    def __init__(
        self,
        base_path: str,
        num_products: int = 50,
        base_date: datetime.date = datetime.date(2025, 10, 1),
        categories: List[str] = None,
        price_range: tuple = (5, 500),
    ):
        self.name = "product"
        self.field_names = ["product_id", "name", "category", "price", "last_modified"]
        self.num_days = 0

        self.base_path = base_path
        self.num_products = num_products
        self.base_date = base_date
        self.categories = categories or ["Electronics", "Home", "Books", "Clothing", "Sports"]
        self.price_range = price_range

        self.products = self._generate_initial_products()

    def _generate_initial_products(self) -> Dict[int, Dict]:
        """
        Generate the initial product catalog with product_id, name, category, price, and last_modified.
        """
        products = {}
        for pid in range(1, self.num_products + 1):
            products[pid] = {
                "product_id": pid,
                "name": f"Product_{pid}",
                "category": random.choice(self.categories),
                "price": round(random.uniform(*self.price_range), 2),
                "last_modified": self.base_date.isoformat(),
            }
        return products

    def _update_products_daily(self, current_date: datetime.date):
        """
        Simulate daily incremental changes to products.
        - 20% chance to change price
        - 5% chance to rename (marketing rename)
        """
        for pid, p in self.products.items():

            # 20% chance to change price
            if random.random() < 0.2:
                delta = random.uniform(-0.1, 0.1)
                p["price"] = round(p["price"] * (1 + delta), 2)
                p["last_modified"] = current_date.isoformat()

            # 5% chance to rename
            if random.random() < 0.05 and not p["name"].endswith("_NEW"):
                p["name"] = p["name"] + "_NEW"
                p["last_modified"] = current_date.isoformat()

    def _add_duplicates(self, rows: List[Dict], num_duplicates: int) -> List[Dict]:
        """
        Randomly sample a few rows and append them as duplicates.
        """
        duplicates = random.sample(rows, k=num_duplicates)
        return rows + duplicates
    
    def generate_new_file(self, num_duplicates: int = 5) -> None:
        """
        Generate and upload daily product files with incremental updates and duplicates.
        """
        day_offset = self.num_days
        current_date = self.base_date + datetime.timedelta(days=day_offset)

        # Apply daily updates after the first day
        if day_offset > 0:
            self._update_products_daily(current_date)

        # Prepare daily rows and add duplicates
        daily_rows = list(self.products.values())
        all_rows = self._add_duplicates(daily_rows, num_duplicates)

        # Build filename and upload to Databricks volume
        filename = f"products_{current_date.strftime('%Y%m%d')}.csv"
        buffer = utils.save_data_to_buffer(all_rows, self.field_names)
        utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)
        #utils.upload_buffer_data_to_local(buffer, filename, self.base_path)
        self.num_days += 1

        print(f"OK {filename} — {len(all_rows)} rows")
