import random
import datetime
from typing import Dict, List
from helpers import utils, workspace_utils


class ProductsGenerator:
    def __init__(
        self,
        env: str,
        num_products: int = 50,
        base_date: datetime.date = datetime.date(2025, 12, 10),
        categories: List[str] = None,
        price_range: tuple = (5, 500),
    ):
        self.name = "product"
        self.field_names = ["product_id", "name", "category", "price", "last_modified"]
        self.num_days = 0

        self.base_path = f"/Volumes/sales_{env}/{utils.get_base_user_schema()}_bronze/raw_files/products"
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

        # Prepare daily rows and add duplicates
        daily_rows = list(self.products.values())
        all_rows = self._add_duplicates(daily_rows, num_duplicates)

        # Build filename and upload to Databricks volume
        filename = f"products_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        buffer = workspace_utils.save_data_to_buffer(all_rows, self.field_names)
        workspace_utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)

        print(f"OK {filename} — {len(all_rows)} rows")
