import random
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple
from helpers import utils, workspace_utils


class OrdersGenerator:
    def __init__(
        self,
        env: str,
        num_orders: int = 100000,
        user_id_range: Tuple[int, int] = (1001, 1050),
    ):
        self.field_names = ["order_id", "user_id", "status", "updated_at"]
        self.base_path = f"/Volumes/sales_{env}/{utils.get_base_user_schema()}_bronze/raw_files/orders"
        self.user_id_range = user_id_range
        self.num_orders = num_orders
        self.status_list = ["pending", "processing", "cancelled", "shipped", "completed"]
        self.orders = self._generate_orders()

    def _make_sale(self, order_id: int, current_date: date) -> Dict:
        """
        Generate a single sale record with optional region column.
        """
        row = {
            "order_id": order_id,
            "user_id": random.randint(*self.user_id_range),
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

    def _generate_orders(self) -> Dict[int, Dict]:
        """
        Generate the initial historical sales snapshot.
        """
        orders = {}
        for sid in range(1, self.num_orders + 1):
            orders[sid] = self._make_sale(sid, self._get_random_date())
        return orders


    def generate_new_file(self) -> None:

        # Save and upload
        filename = f"orders_{datetime.now().strftime('%Y%m%d')}.csv"

        all_rows = list(self.orders.values())

        buffer = workspace_utils.save_data_to_buffer(all_rows, self.field_names)
        workspace_utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)

        print(f"OK {filename} - {len(all_rows)} rows")
