import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple
from helpers import utils, workspace_utils


class OrderItemsGenerator:
    def __init__(
        self,
        env: str,
        num_historical_order_id: int = 100000,
        product_id_range: Tuple[int, int] = (1, 50),
        price_range: Tuple[int, int] = (5, 500),
        items_in_order_range: Tuple[int, int] = (1, 10)
    ):
        self.name = "sales"
        self.field_names = ["order_id", "product_id", "order_item_id", "qty", "price", "status", "updated_at"]
        self.base_path = f"/Volumes/sales_{env}/{utils.get_base_user_schema()}_bronze/raw_files/order_items"
        self.num_historical_order_id = num_historical_order_id
        self.product_id_range = product_id_range
        self.price_range = price_range
        self.items_in_order_range = items_in_order_range
        self.status_list = ["pending", "processing", "cancelled", "shipped", "completed"]
        self.order_items = self._generate_order_items()

    def _make_sale(self, order_id: int, current_date: date) -> Dict:
        """
        Generate a single sale record with optional region column.
        """
        product_id = random.randint(*self.product_id_range)
        order_item_id = order_id + product_id
        row = {
            "order_id": order_id,
            "product_id": product_id,
            "order_item_id": order_item_id,
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

    def _generate_order_items(self) -> Dict[int, Dict]:
        """
        Generate the initial historical sales snapshot.
        """
        order_items = {}

        for sid in range(1, self.num_historical_order_id + 1):

            random_date = self._get_random_date()

            for oid in range(1, random.randint(*self.items_in_order_range)):

                order_items[sid] = self._make_sale(sid, self._get_random_date())

        return order_items


    def generate_new_file(self) -> None:
        """
        Generate daily sales delta files with inserts, updates, and deletes.
        """

        # Save and upload
        filename = f"order_items_{datetime.now().strftime('%Y%m%d')}.csv"

        all_rows = list(self.order_items.values())

        buffer = workspace_utils.save_data_to_buffer(all_rows, self.field_names)
        workspace_utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)

        print(f"OK {filename} - {len(all_rows)} rows")
