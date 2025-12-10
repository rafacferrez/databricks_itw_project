import random
import datetime
from typing import Dict, List, Tuple
from helpers import utils, workspace_utils


class OrdersGenerator:
    def __init__(
        self,
        env: str,
        num_historical: int = 10000,
        base_date: datetime.date = datetime.date(2025, 9, 30),
        product_id_range: Tuple[int, int] = (1, 50),
        user_id_range: Tuple[int, int] = (1001, 1050),
        price_range: Tuple[int, int] = (5, 500),
    ):
        self.name = "sales"
        self.field_names = ["sale_id", "product_id", "user_id", "qty", "price", "status", "updated_at"]
        self.num_days = 0

        self.base_path = f"/Volumes/sales_{env}/{utils.get_base_user_schema()}_bronze/raw_files/orders"
        self.num_historical = num_historical
        self.base_date = base_date
        self.product_id_range = product_id_range
        self.user_id_range = user_id_range
        self.price_range = price_range
        self.status_list = ["pending", "processing", "cancelled", "shipped", "completed"]

        self.sales = self._generate_historical_sales()

    def _make_sale(self, sale_id: int, current_date: datetime.date, status="Insert", include_region=False) -> Dict:
        """
        Generate a single sale record with optional region column.
        """
        row = {
            "sale_id": sale_id,
            "product_id": random.randint(*self.product_id_range),
            "user_id": random.randint(*self.user_id_range),
            "qty": random.randint(1, 25),
            "price": round(random.uniform(*self.price_range), 2),
            "status": random.choice(self.status_list),
            "updated_at": current_date.isoformat(),
        }
        return row

    def _generate_historical_sales(self) -> Dict[int, Dict]:
        """
        Generate the initial historical sales snapshot.
        """
        sales = {}
        for sid in range(1, self.num_historical + 1):
            sales[sid] = self._make_sale(sid, self.base_date, include_region=False)
        return sales


    def generate_new_file(self) -> None:
        """
        Generate daily sales delta files with inserts, updates, and deletes.
        """
        # Daily context
        next_sale_id = max(self.sales.keys()) + 1

        day_offset = self.num_days
        current_date = self.base_date + datetime.timedelta(days=day_offset)

        # Save and upload
        filename = f"sales_{current_date.strftime('%Y%m%d')}.csv"

        all_rows = list(self.sales.values())

        buffer = workspace_utils.save_data_to_buffer(all_rows, self.field_names)
        workspace_utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)

        print(f"OK {filename} - {len(all_rows)} rows")
