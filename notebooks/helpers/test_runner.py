import os
import re 
import inspect
import traceback
import importlib

from helpers.tests import * # all tests imported via __all__ module in __init__.py 

class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    ORANGE = "\033[93m"
    RESET = "\033[0m"

def run(**kwargs):
    """
Automatically detects the current Databricks notebook name via an environment variable:

    import os
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    os.environ["NOTEBOOK_NAME"] = notebook_path.split("/")[-1]

This must be present in all notebooks for them to be properly tested.

The function imports the matching test module from `helpers/tests/` and runs all `test_*` functions.  
Imports are handled via `tests/__init__.py`; review this file before developing new tests.

Automatically captures `catalog_name` and `base_user` as global variables if not passed explicitly in the function call.

All variables required by the tests must be passed in the function call.  
This call can be hard-coded in the student notebook to simplify the process.  
Ensure all necessary variables are provided; the function will distribute them correctly to the tests.

Example function call:

    from helpers import test_runner

    test_runner.run(
        base_user=base_user,
        catalog_name=catalog_name,
        df=df_final,          # or another dataframe
        df_schema=df.schema(),
        ...
    )

Variable names in the function call must match the names used in the test functions (`helpers/test/*`).  
For example, if a test function expects a dataframe schema as `schema_evoluted`, the call should be:

    test_runner.run(
        ...
        schema_evoluted=df.schema(),
        ...
    )

Alternatively, variable assignment can also be handled manually within the test logic.
    """

    # 1. Detecting notebook name
    
    notebook_name = os.getenv("NOTEBOOK_NAME", "")
    if notebook_name == "":
        print(f"{Colors.RED}ERROR{Colors.RESET}: Could not detect notebook name, make sure to have environment variable set up")
        return

    # 2. Grabbing the correct test module
    
    base_name = re.sub(r'^\d+-', '', notebook_name) # remove leading numbers
    base_name = os.path.splitext(base_name)[0]      # strip .ipynb 
    base_name = base_name.replace("-", "_")         # normalize
    module_name = f"helpers.tests.{base_name}_tests"

    print(f"\nRunning tests for: {Colors.ORANGE}{base_name}{Colors.RESET}")

    # 3. Collect test_* functions 

    # Automatically collect base_user if not passed by function call
    base_user_found, catalog_name_found= False, False
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            caller_globals = frame.f_back.f_globals
            if "base_user" in caller_globals and "base_user" not in kwargs:
                kwargs["base_user"] = caller_globals["base_user"]
                base_user_found = True
            if "catalog_name" in caller_globals and "catalog_name" not in kwargs:
                kwargs["catalog_name"] = caller_globals["catalog_name"]
                catalog_name_found = True
    except Exception:
        pass

    if base_user_found:
        print("Detected and injected base_user from notebook globals")
    if catalog_name_found:
        print("Detected and injected catalog_name from notebook globals")

    # Continue looking for the test module + its functions
    try:
        test_module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        print(f"{Colors.RED}ERROR{Colors.RESET}: Could not find test module {module_name}")
        return

    test_functions = [
            (name, func)
            for name, func in inspect.getmembers(test_module, inspect.isfunction)
            if name.startswith("test_")
            ]
    if not test_functions:
        print(f"{Colors.RED}ERROR{Colors.RESET}: No test_* functions found in module.")
        return
    
    # 4. Execute all tests

    results = {"passed":0, "failed":0}
    for name, func in test_functions:
        try:
            func(**kwargs)
            print(f"\t- [{Colors.GREEN}PASSED{Colors.RESET}] {name}")
            results["passed"] += 1
        except Exception:
            print(f"\t- [{Colors.RED}FAILED{Colors.RESET}] {name}")
            print(traceback.format_exc())
            results["failed"] += 1

    total = results["passed"] + results["failed"]
    print(f"\n✅ Test summary: {results['passed']}/{total} passed, {results['failed']} failed")

