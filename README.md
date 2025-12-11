# databricks_itw_project

# Project Overview

This project demonstrates how to build and deploy a data pipeline in **Databricks** using **Lakeflow Declarative Pipelines** and **Asset Bundles**. It includes environment setup, transformations, and automated deployment workflows.

---

## Project Structure

```
.
├── notebooks/
│   ├── helpers/                             # helpers functions
│   ├── setup_schemas_and_volumes.ipynb      # Environment setup: schemas and volumes
│   ├── queries.ipynb                        # ad hoc queries
│   └── clean_up_environment.ipynb           # Cleanup script: removes tables, volumes, schemas
│
├── orders_pipeline/
│   └── transformations/                     # Transformation logic using Lakeflow Declarative 
│
└── project_bundle/                          # Deployment configuration using Asset Bundles
```

---

## Getting Started in Databricks

This guide explains how to set up the environment, run the pipeline locally on Databricks, and deploy it using Asset Bundles and Lakeflow.

### Prerequisites

- A Databricks workspace (DBR 14.x or higher recommended)
- Unity Catalog enabled
- Git integration configured in Databricks (optional but recommended)
- Databricks CLI 0.205+ installed locally (for bundle deployment)

---

## 1. Environment Setup

Before running any pipeline logic, you must create the necessary **schemas**, **tables**, and **volumes**.

### Step 1 — Run the setup notebook

Open the following notebook in your Databricks workspace:

```
notebooks/setup_schemas_and_volumes.ipynb
```

This notebook:
- Creates the required schemas for the project
- Configures volumes for input/output data
- Prepares the workspace for pipeline execution

Run all cells to initialize the environment.

---

## 2. Environment Cleanup (Optional)

To remove all created assets (schemas, tables, volumes), use the cleanup notebook:

```
notebooks/clean_up_environment.ipynb
```

This is useful if you want to reset the workspace and rerun the project from scratch.

---

## 3. Transformations Using Lakeflow Declarative Pipelines

All transformation logic lives inside:

```
orders_pipeline/transformations/
```

This folder contains declarative definitions for:
- Input datasets
- Transformations
- Dimensional model definitions
- Fact and dimension table logic
- Data pipeline orchestration

Databricks **Lakeflow** compiles these declarative definitions into a reproducible and traceable data workflow.

You can run transformations in two ways:

### Option A — Interactive execution
Open the transformation code inside Databricks and run it interactively for development.

### Option B — Run as a Lakeflow pipeline
When deployed via Asset Bundles, the pipeline becomes a scheduled or triggered job.

---

## 4. Deploying with Asset Bundles

The `project_bundle/` directory contains the configuration needed to deploy the data pipeline into Databricks.

### What Asset Bundles are used for
- Packaging code and configuration
- Deploying Lakeflow pipelines to development/production
- Managing environment variables and workspace paths
- Ensuring reproducible, version-controlled deployments

### Deployment Steps (CLI)

1. Navigate to the bundle directory:
   ```bash
   cd project_bundle
   ```

2. Validate the bundle:
   ```bash
   databricks bundle validate
   ```

3. Deploy to your workspace:
   ```bash
   databricks bundle deploy -t {env}
   ```

4. (Optional) Trigger a pipeline run:
   ```bash
   databricks bundle run orders_pipeline
   ```

After deployment, the pipeline will appear under **Workflows → Lakeflow Pipelines** in Databricks.

---

## 5. Running the Pipeline End-to-End

1. Run `setup_schemas_and_volumes.ipynb`
2. Deploy the bundle (`databricks bundle deploy`)
3. Start the Lakeflow pipeline via:
   - Databricks UI, or
   - CLI (`bundle run`)
4. (Optional) Clean the environment using `clean_up_environment.ipynb`

--- 