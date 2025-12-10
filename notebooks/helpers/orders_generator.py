import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple
from helpers import utils, workspace_utils


class OrdersGenerator:
    def __init__(
        self,
        env: str,
        num_historical: int = 100000,
        product_id_range: Tuple[int, int] = (1, 50),
        user_id_range: Tuple[int, int] = (1001, 1050),
        price_range: Tuple[int, int] = (5, 500),
    ):
        self.name = "sales"
        self.field_names = ["sale_id", "product_id", "user_id", "qty", "price", "status", "updated_at"]
        self.base_path = f"/Volumes/sales_{env}/{utils.get_base_user_schema()}_bronze/raw_files/orders"
        self.num_historical = num_historical
        self.product_id_range = product_id_range
        self.user_id_range = user_id_range
        self.price_range = price_range
        self.status_list = ["pending", "processing", "cancelled", "shipped", "completed"]
        self.sales = self._generate_historical_sales()

    def _make_sale(self, sale_id: int, current_date: date, status="Insert", include_region=False) -> Dict:
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
            "updated_at": current_date.isoformat()
        }
        return row
    
    def _get_random_date(self) -> date:

        now = datetime.now()
        current_month_start = datetime(now.year, now.month, 1)
        last_month_start = datetime(now.year, now.month - 1 if now.month > 1 else 12, 1)

        start = last_month_start.timestamp()
        end = now.timestamp()

        random_date = datetime.fromtimestamp(random.uniform(start, end))

        return random_date

    def _generate_historical_sales(self) -> Dict[int, Dict]:
        """
        Generate the initial historical sales snapshot.
        """
        sales = {}
        for sid in range(1, self.num_historical + 1):
            sales[sid] = self._make_sale(sid, self._get_random_date())
        return sales


    def generate_new_file(self) -> None:
        """
        Generate daily sales delta files with inserts, updates, and deletes.
        """

        # Save and upload
        filename = f"sales_{datetime.now().strftime('%Y%m%d')}.csv"

        all_rows = list(self.sales.values())

        buffer = workspace_utils.save_data_to_buffer(all_rows, self.field_names)
        workspace_utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)

        print(f"OK {filename} - {len(all_rows)} rows")
