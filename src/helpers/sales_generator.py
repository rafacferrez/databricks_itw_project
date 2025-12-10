import random
import datetime
from typing import Dict, List, Tuple

from instructors.src.helpers import workspace_utils as utils


class SalesGenerator:
    def __init__(
        self,
        base_path: str,
        num_historical: int = 100,
        inserts_per_day: int = 10,
        updates_per_day: int = 2,
        deletes_per_day: int = 2,
        base_date: datetime.date = datetime.date(2025, 9, 30),
        product_id_range: Tuple[int, int] = (1, 50),
        user_id_range: Tuple[int, int] = (1001, 1050),
        price_range: Tuple[int, int] = (5, 500),
        regions: List[str] = None,
        region_start_day: int = 4,
    ):
        self.name = "sales"
        self.field_names = ["sale_id", "product_id", "user_id", "qty", "price", "status", "updated_at"]
        self.num_days = 0
        self.deleted_ids = set()

        self.base_path = base_path
        self.num_historical = num_historical
        self.inserts_per_day = inserts_per_day
        self.updates_per_day = updates_per_day
        self.deletes_per_day = deletes_per_day
        self.base_date = base_date
        self.product_id_range = product_id_range
        self.user_id_range = user_id_range
        self.price_range = price_range
        self.regions = regions or ["US", "CA"]
        self.region_start_day = region_start_day

        self.sales = self._generate_historical_sales()
        self._persist_historical()

    def _make_sale(self, sale_id: int, current_date: datetime.date, status="Insert", include_region=False) -> Dict:
        """
        Generate a single sale record with optional region column.
        """
        row = {
            "sale_id": sale_id,
            "product_id": random.randint(*self.product_id_range),
            "user_id": random.randint(*self.user_id_range),
            "qty": random.randint(1, 10),
            "price": round(random.uniform(*self.price_range), 2),
            "status": status,
            "updated_at": current_date.isoformat(),
        }
        if include_region:
            row["region"] = random.choice(self.regions)
        return row

    def _generate_historical_sales(self) -> Dict[int, Dict]:
        """
        Generate the initial historical sales snapshot.
        """
        sales = {}
        for sid in range(1, self.num_historical + 1):
            sales[sid] = self._make_sale(sid, self.base_date, include_region=False)
        return sales

    def _apply_updates(
            self, current_date: datetime.date, update_count: int, include_region: bool
    ) -> Tuple[List[Dict], List]:
        """
        Apply random updates to a subset of active sales records.
        """
        active_ids = [sid for sid in self.sales.keys() if sid not in self.deleted_ids]
        update_candidates = random.sample(active_ids, min(update_count, len(active_ids)))
        updated_rows = []

        for sid in update_candidates:
            row = self.sales[sid]
            row["qty"] = random.randint(1, 10)
            row["price"] = round(row["price"] * random.uniform(0.9, 1.1), 2)
            row["status"] = "Update"
            row["updated_at"] = current_date.isoformat()

            if "region" not in self.sales[sid] and include_region:
                row["region"] = None

            updated_rows.append(row)

        return updated_rows, update_candidates

    def _apply_deletes(
            self, current_date: datetime.date, delete_count: int, exclude_ids: List[int]
    ) -> Tuple[List[Dict], List]:
        """
        Apply random deletes to sales records not already updated or deleted.
        """
        remaining_ids = [sid for sid in self.sales.keys() if sid not in exclude_ids and sid not in self.deleted_ids]
        delete_candidates = random.sample(remaining_ids, min(delete_count, len(remaining_ids)))
        deleted_rows = []

        for sid in delete_candidates:
            row = self.sales[sid]
            row["status"] = "Delete"
            row["updated_at"] = current_date.isoformat()
            deleted_rows.append(row)
            self.deleted_ids.add(sid)

        return deleted_rows, delete_candidates

    def _apply_inserts(
            self, current_date: datetime.date, insert_count: int, next_sale_id: int, include_region: bool
    ) -> Tuple[List[Dict], int]:
        """
        Insert new sales records.
        """
        inserted_rows = []
        for _ in range(insert_count):
            new_row = self._make_sale(next_sale_id, current_date, include_region=include_region)
            self.sales[next_sale_id] = new_row
            inserted_rows.append(new_row)
            next_sale_id += 1
        return inserted_rows, next_sale_id
    
    def _persist_historical(self):
        """
        Historical snapshot.
        """
        filename = f"sales_{self.base_date.strftime('%Y%m%d')}.csv"
        buffer = utils.save_data_to_buffer(list(self.sales.values()), self.field_names)
        utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)
        #utils.upload_buffer_data_to_local(buffer, filename, self.base_path)
        self.num_days += 1
        print(f"OK Generated historical snapshot: {filename} ({len(self.sales)} inserts)")

    def generate_new_file(self) -> None:
        """
        Generate daily sales delta files with inserts, updates, and deletes.
        """
        # Daily context
        next_sale_id = max(self.sales.keys()) + 1

        day_offset = self.num_days
        current_date = self.base_date + datetime.timedelta(days=day_offset)
        include_region = day_offset >= self.region_start_day

        # Apply updates
        updated_rows, updated_ids = self._apply_updates(current_date, self.updates_per_day, include_region)

        # Apply deletes
        deleted_rows, _ = self._apply_deletes(current_date, self.deletes_per_day, updated_ids)

        # Apply inserts
        inserted_rows, _ = self._apply_inserts(current_date, self.inserts_per_day, next_sale_id, include_region)

        # Collect all daily changes
        daily_changes = updated_rows + deleted_rows + inserted_rows

        # Limit prices to prevent drift
        for row in daily_changes:
            row["price"] = min(max(row["price"], self.price_range[0]), self.price_range[1])

        # Save and upload
        filename = f"sales_{current_date.strftime('%Y%m%d')}.csv"

        if include_region and "region" not in self.field_names:
            self.field_names.insert(-1, "region")

        buffer = utils.save_data_to_buffer(daily_changes, self.field_names)
        utils.upload_buffer_data_to_databricks(buffer, filename, self.base_path)
        #utils.upload_buffer_data_to_local(buffer, filename, self.base_path)
        self.num_days += 1

        # Reset statuses for next iteration (keep soft deletes) ---
        for sale in self.sales.values():
            if sale["status"] in ("Insert", "Update"):
                sale["status"] = "Active"

        print(
            f"OK {filename} — Inserts:{len(inserted_rows)}, Updates:{len(updated_rows)}, Deletes:{len(deleted_rows)}, region:{include_region}"
        )
