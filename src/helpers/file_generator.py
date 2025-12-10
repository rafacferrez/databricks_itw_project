from instructors.src.helpers.products_generator import ProductsGenerator
from instructors.src.helpers.users_generator import UsersGenerator
from instructors.src.helpers.sales_generator import SalesGenerator


def generate_products(base_path: str, num_days: int = 1) -> None:
    """
    Generate products data for n NUM_DAYS.
    """
    pg = ProductsGenerator(base_path)
    for _ in range(num_days):
        pg.generate_new_file()


def generate_users(base_path: str, num_days: int = 1) -> None:
    """
    Generate users data for n NUM_DAYS.
    """
    ug = UsersGenerator(base_path)
    for _ in range(num_days):
        ug.generate_new_file()


def generate_sales(base_path: str, num_days: int = 1) -> None:
    """
    Generate sales data for n NUM_DAYS.
    """
    sg = SalesGenerator(base_path)
    for _ in range(num_days):
        sg.generate_new_file()
