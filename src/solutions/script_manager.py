from instructors.src.solutions.batch_ingestion import BatchIngestion
from instructors.src.solutions.cdc_merge import CDCMerge
from instructors.src.solutions.gold_layer import GoldLayer


class ScriptManager:
    def __init__(self, env: str, catalog_name: str = "capstone_src"):
        """
        Constructor for the ScriptManager class.
        Args:
            env (str): Environment name, either 'dev' or 'prd'.
            catalog_name (str): Catalog name, default is "capstone_src".
        """
        if env not in ["dev", "prd"]:
            raise ValueError("Variable env must be either 'dev' or 'prd'")
    
        self.env = env
        self.catalog_name = catalog_name
        
        # Schema Definition
        self.bronze_schema = f"{env}_bronze"
        self.silver_schema = f"{env}_silver"
        self.gold_schema = f"{env}_gold"

        # Volume Definition
        self.data_volume = f"/Volumes/{self.catalog_name}/{self.bronze_schema}/raw_files"
        self.schema_volume = f"/Volumes/{self.catalog_name}/{self.bronze_schema}/schema_files"
        self.checkpoint_volume = f"/Volumes/{self.catalog_name}/{self.bronze_schema}/checkpoint_files"

        # Table Definition
        self.users_table_name = "users"
        self.products_table_name = "products"
        self.sales_table_name = "sales"

        self.users_gold = "dim_user"
        self.products_gold = "dim_product"
        self.sales_gold = "fact_sales"

        self._users_bronze = f"{self.catalog_name}.{self.bronze_schema}.{self.users_table_name}"
        self._users_silver = f"{self.catalog_name}.{self.silver_schema}.{self.users_table_name}"
        self._users_gold = f"{self.catalog_name}.{self.gold_schema}.{self.users_gold}"

        self._products_bronze = f"{self.catalog_name}.{self.bronze_schema}.{self.products_table_name}"
        self._products_silver = f"{self.catalog_name}.{self.silver_schema}.{self.products_table_name}"
        self._products_gold = f"{self.catalog_name}.{self.gold_schema}.{self.products_gold}"

        self._sales_bronze = f"{self.catalog_name}.{self.bronze_schema}.{self.sales_table_name}"
        self._sales_silver = f"{self.catalog_name}.{self.silver_schema}.{self.sales_table_name}"
        self._sales_gold = f"{self.catalog_name}.{self.gold_schema}.{self.sales_gold}"

    def users_batch_ingestion(self, folder_name: str = "users"):
        data_vol = f"{self.data_volume}/{folder_name}"

        sol = BatchIngestion()
        print("Creating Bronze Table")
        sol.create_bronze_users(self._users_bronze)
        print("Copy Into Bronze Table")
        sol.copy_into(self._users_bronze, data_vol)
        print("Creating Silver Table")
        sol.create_silver_users(self._users_bronze, self._users_silver)

    def products_batch_ingestion(self, folder_name: str = "products"):
        data_vol = f"{self.data_volume}/{folder_name}"

        sol = BatchIngestion()
        print("Creating Bronze Table")
        sol.create_bronze_products(self._products_bronze)
        print("Copy Into Bronze Table")
        sol.copy_into(self._products_bronze, data_vol)
        print("Creating Silver Table")
        sol.create_silver_products(self._products_bronze, self._products_silver)
      
    def sales_cdc_merge(self, folder_name: str = "sales"):
        data_vol = f"{self.data_volume}/{folder_name}"
        schema_vol = f"{self.schema_volume}/{folder_name}"
        check_vol = f"{self.checkpoint_volume}/{folder_name}"

        sol = CDCMerge()
        print("Creating Bronze Table")
        sol.create_bronze_sales(self._sales_bronze)
        print("Auto Loader Into Bronze Table")
        sol.auto_loader_with_metadata(self._sales_bronze, data_vol, schema_vol, check_vol)
        print("Creating Silver Table")
        sol.create_silver_sales(self._sales_silver)

        dates = sol.list_dates(self._sales_bronze)
        print("CDC Merge Operation")
        for date in dates:
            print(f"Processing {date}")
            sol.cdc_merge(self._sales_bronze, self._sales_silver, date)

    def gold_layer(self):
        sol = GoldLayer
        print("Creating Dimension User")
        sol.create_dim_user(self._users_silver, self._users_gold)
        print("Creating Dimension Product")
        sol.create_dim_product(self._products_silver, self._products_gold)
        print("Creating Fact Table Sales")
        sol.create_fact_sales(self._sales_silver, self._sales_gold)
