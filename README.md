# Zomato AI Data Engineering Pipeline

An end-to-end **learning project** focused on building a modern data engineering pipeline for food-delivery data, from raw CSV files to analytics-ready data models and AI-powered applications.

The project explores how cloud storage, a data warehouse, transformation tooling, workflow orchestration, and large language models can work together in a practical data platform.

> **Project purpose:** This repository is maintained for learning, experimentation, and hands-on practice. It is not presented as a production deployment. The architecture and documentation are being developed in phases; check the roadmap for the current implementation status.

## Pipeline overview

```text
Food-delivery CSV data
        |
        v
   Amazon S3
        |
        v
Snowflake RAW (Bronze)
        |
        v
dbt STAGING (Silver)
        |
        v
 dbt MARTS (Gold)
        |
        +-----------------------------+
        |                             |
        v                             v
Business analytics              AI enrichment
                                      |
                                      v
                                Snowflake AI
                                      |
                         +------------+------------+
                         |                         |
                         v                         v
                    Review RAG                Text-to-SQL

              Apache Airflow orchestrates the workflow
              Streamlit provides the interactive AI apps
```

An architecture diagram is available at [`docs/architecture.png`](docs/architecture.png), if present in the repository.

## What this project explores

- **Cloud data lake:** Organising raw CSV files in Amazon S3 using a `raw/<table>/` folder structure.
- **Secure AWS-to-Snowflake access:** Using a Snowflake storage integration and an AWS IAM role instead of storing AWS access keys in Snowflake.
- **Medallion architecture:** Separating data into Bronze (RAW), Silver (STAGING), and Gold (MARTS) layers.
- **Data transformation with dbt:** Building staging models, dimensions, incremental fact tables, business marts, and data quality tests.
- **Workflow orchestration:** Using Apache Airflow to coordinate data loading, transformations, and AI enrichment.
- **LLM-based enrichment:** Exploring structured sentiment and topic extraction from customer reviews.
- **Retrieval-Augmented Generation (RAG):** Retrieving relevant reviews to provide context-grounded answers.
- **Text-to-SQL:** Exploring natural-language questions against curated warehouse models.

## Architecture and data layers

| Layer | Platform / schema | Responsibility |
|---|---|---|
| Source | Local `data/` directory | Input dimension and fact CSV files |
| Data lake | Amazon S3 | Stores raw CSV files by table |
| Bronze | Snowflake `ZOMATO.RAW` | Loads source data with `COPY INTO` |
| Silver | Snowflake `ZOMATO.STAGING` | Cleans, standardises, and types source data using dbt |
| Gold | Snowflake `ZOMATO.MARTS` | Provides dimensions, fact tables, and business-facing marts |
| AI | Snowflake `ZOMATO.AI` | Stores enriched review data for AI-oriented use cases |
| Orchestration | Apache Airflow | Coordinates the pipeline workflow |
| Applications | Streamlit | Hosts the review RAG and text-to-SQL interfaces |

## Dataset

The project is designed around food-delivery data with the following approximate scale:

| Dataset | Approximate rows | Description |
|---|---:|---|
| Restaurants | 148,000 | Restaurant information, including inconsistent source values |
| Users | Not specified | Customer dimension data |
| Food | Not specified | Food item dimension data |
| Menu | Not specified | Restaurant menu data |
| Orders | 10 million | Generated order records |
| Order items | 23 million | Generated order-item records |
| Reviews | 300,000 | Generated free-text customer reviews |

The complete dataset is approximately **2.3 GB** and is not stored in this repository. Obtain the dataset from the [dataset download folder](https://drive.google.com/drive/folders/1_dwsOGOMeiklN4Xi6_wAJoQ7-OuKLnQM?usp=drive_link) and place the required CSV files in the local `data/` directory.

Do not commit the dataset to Git unless the repository's storage and data-licensing requirements have been checked.

## Technology stack

| Area | Tools |
|---|---|
| Programming and data processing | Python, Pandas, NumPy |
| Cloud object storage | Amazon S3 |
| Cloud permissions | AWS IAM |
| Data warehouse | Snowflake |
| Data transformation | dbt, `dbt-snowflake` |
| Workflow orchestration | Apache Airflow 3 |
| Containerisation | Docker, Docker Compose |
| LLM and embeddings | OpenAI API, `gpt-4o-mini`, `text-embedding-3-small` |
| Data applications | Streamlit |

## Repository structure

```text
.
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── example.env
│   └── dags/
│       └── zomato_batch.py
│
├── zomato/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── macros/
│
├── ai/
│   ├── enrich_reviews.py
│   ├── rag_chat.py
│   ├── text_to_sql.py
│   └── example.env
│
├── snowflake/
│   ├── 01_setup.sql
│   ├── 02_storage_integration.sql
│   ├── 03_stage_and_formats.sql
│   ├── 04_raw_tables.sql
│   └── 05_copy_into.sql
│
├── aws/
│   └── iam/
│
├── docs/
│   └── architecture.png
│
└── data/                 # Local dataset; not committed
```

The tree describes the intended project layout. Some files or folders may need to be created as the implementation progresses.

## How the pipeline works

### 1. Prepare and upload the source data

The pipeline uses seven CSV datasets:

```text
restaurants/
users/
food/
menu/
orders/
order_items/
reviews/
```

Upload each CSV to its corresponding folder in an Amazon S3 bucket:

```text
s3://<BUCKET>/raw/<table>/
```

Replace `<BUCKET>` with the name of your own S3 bucket.

### 2. Configure secure access from Snowflake to S3

Snowflake accesses the S3 bucket through a **storage integration** backed by an AWS IAM role. This avoids placing long-lived AWS access keys in Snowflake configuration.

The IAM configuration is organised around these files:

| File | Purpose |
|---|---|
| `aws/iam/s3-read-policy.json` | Grants the required read access to the S3 bucket |
| `aws/iam/snowflake-role-trust-policy-initial.json` | Initial trust policy used while creating the IAM role |
| `aws/iam/snowflake-role-trust-policy-final.json` | Trust policy configured with the Snowflake IAM principal and external ID |

The general setup sequence is:

1. Create the S3 bucket and upload the source files.
2. Create the IAM read policy and IAM role.
3. Create the Snowflake storage integration using the IAM role ARN.
4. Run `DESC INTEGRATION` in Snowflake to retrieve the Snowflake IAM user ARN and external ID.
5. Update the IAM role trust policy with those values.
6. Verify that Snowflake can access the external stage.

**Important:** Keep the IAM principal and external ID in the trust policy consistent with the values returned by Snowflake. Recreating a storage integration can change its external ID, so verify the trust relationship again if the integration is replaced.

### 3. Load the raw data into Snowflake

The SQL scripts in `snowflake/` are intended to be executed in sequence:

1. `01_setup.sql` — creates the warehouse, database, schemas, and role.
2. `02_storage_integration.sql` — configures the S3 storage integration.
3. `03_stage_and_formats.sql` — creates file formats and external stages.
4. `04_raw_tables.sql` — creates the raw tables.
5. `05_copy_into.sql` — loads the CSV data into Snowflake using `COPY INTO`.

The raw tables form the **Bronze** layer. Loading behaviour should be chosen deliberately: permissive error handling can help inspect messy dimension data, while strict handling is useful when fact-table row counts must be validated.

### 4. Transform data with dbt

The dbt project organises transformations into three conceptual layers:

- **Staging (Silver):** Cleans and standardises source values, converts data types, and creates consistent column names. Examples include converting placeholder values such as `--` to `NULL`, parsing price strings, normalising email values, and deriving delivery-status fields.
- **Dimensions and facts (Gold):** Builds analytical models such as `dim_restaurants`, `dim_customer`, `dim_food`, `dim_date`, `fct_orders`, and `fact_order_items`.
- **Business marts (Gold):** Produces reporting models such as daily city revenue, restaurant performance, delivery SLA metrics, and review insights.

Incremental fact models are intended to use dbt's incremental materialisation and `MERGE` strategy. Data quality checks include tests such as `unique`, `not_null`, `relationships`, and `accepted_values`.

### 5. Orchestrate the workflow with Airflow

The intended daily workflow is defined in the `zomato_batch` DAG:

```text
reload_raw
    |
    v
dbt_build_core
    |
    v
enrich_reviews
    |
    v
dbt_build_ai
```

| Task | Responsibility |
|---|---|
| `reload_raw` | Loads source files from S3 into Snowflake |
| `dbt_build_core` | Builds core dbt models and runs tests |
| `enrich_reviews` | Enriches review data using the OpenAI API |
| `dbt_build_ai` | Builds the AI-oriented dbt models |

Credentials should be supplied through environment variables or Airflow connections. Do not hardcode passwords, API keys, or other secrets in DAG files or dbt models.

### 6. Explore the AI applications

The AI layer contains three learning components:

1. **Review enrichment — `ai/enrich_reviews.py`**  
   Uses an LLM to extract structured information from review text, such as sentiment, sentiment score, topic, and key issue. The intended output is stored in `ZOMATO.AI.REVIEW_ENRICHED` and can be transformed into review insight marts. Sampling and idempotent processing help control API cost and avoid unnecessary reprocessing.

2. **Review RAG — `ai/rag_chat.py`**  
   Creates embeddings for review text, retrieves semantically similar reviews, and uses the retrieved content as context for answering questions. The application is intended to show supporting review sources with its answers.

3. **Text-to-SQL — `ai/text_to_sql.py`**  
   Uses warehouse schema context to generate SQL for natural-language questions. SQL validation and a read-only database role are important safeguards. A simple keyword filter alone should not be treated as a complete SQL security boundary.

These components are included as learning exercises. Their behaviour, reliability, security, and end-to-end integration should be tested before describing them as production-ready.

## Prerequisites

Before attempting the full pipeline, prepare:

| Requirement | Purpose |
|---|---|
| AWS account | S3 bucket, IAM policy, and IAM role |
| Snowflake account | Warehouse, database, schemas, storage integration, and tables |
| OpenAI API key | Review enrichment and AI application experiments |
| Docker Desktop | Running the Airflow services |
| Python 3.10+ | Running the Python, dbt, and Streamlit components |
| Dataset files | Source CSVs placed in the local `data/` directory |

The project uses Python packages including `dbt-snowflake`, `snowflake-connector-python[pandas]`, `openai`, `python-dotenv`, `streamlit`, `pandas`, `numpy`, and `pyarrow`. Install compatible versions for the environment you are using.

## Configuration

Create local configuration files based on the example environment templates. Exact variable names should match the code and Docker Compose configuration in the repository.

Typical configuration values include:

```dotenv
# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=ZOMATO_WH
SNOWFLAKE_DATABASE=ZOMATO
SNOWFLAKE_SCHEMA=AI

# OpenAI
OPENAI_API_KEY=

# AI processing
SAMPLE_N=5
```

The dbt profile should read credentials using environment variables (for example, `env_var()`) rather than storing secrets directly in `profiles.yml`.

> **Security:** Never commit `.env` files, populated dbt profiles, API keys, passwords, private keys, or other credentials. Keep local secrets out of version control and use appropriately scoped credentials.

## Running the project

The following is the intended execution sequence. It assumes that the cloud resources, configuration files, and dataset have already been prepared.

### 1. Set up AWS and Snowflake

- Create the S3 bucket and upload the CSV files.
- Configure the IAM policy and role.
- Execute the Snowflake SQL scripts in numerical order.
- Verify the external stage and raw data loading.

### 2. Build the dbt models

```bash
cd zomato

export SNOWFLAKE_ACCOUNT="<account>"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_PASSWORD="<password>"

dbt debug
dbt build --exclude tag:ai
```

### 3. Start Airflow

```bash
cd airflow

# Create the local .env file before starting the services.
docker compose build
docker compose up -d
```

Open `http://localhost:8080`, check the Airflow services and connections, and trigger the `zomato_batch` DAG when the environment is ready.

### 4. Run the AI components

From the repository root, configure the required environment variables and run:

```bash
python ai/enrich_reviews.py

streamlit run ai/rag_chat.py
streamlit run ai/text_to_sql.py
```

Commands and configuration may need adjustment to match the current state of the repository. Run each component independently and verify its output before connecting it to the full orchestration workflow.

## Design review and planned improvements

The following are areas identified for review during development. They are improvement tasks, not claims that the fixes have already been implemented.

| Area | Observation | Planned improvement |
|---|---|---|
| Airflow and AI configuration | The enrichment script expects Snowflake warehouse, database, and schema settings; these must be passed into the relevant containers | Add and validate the required environment variables in Docker Compose |
| AI cost control | The sample size should be configurable rather than fixed in code | Read `SAMPLE_N` from the environment and validate its value |
| Airflow image | Required Python packages may be missing from the image | Confirm and install the dependencies used by the DAG and scripts |
| Configuration documentation | Environment templates and setup instructions need to be complete | Maintain usable example environment files and a setup runbook |
| Naming consistency | IAM role names should be consistent across documentation, SQL, and policy files | Standardise resource names |
| Snowflake setup | Setup SQL should contain only the schemas and configuration used by this architecture | Remove obsolete or unrelated configuration |
| dbt validation | Snapshot and reconciliation coverage may be incomplete | Add an SCD Type 2 snapshot and singular data reconciliation tests |
| Text-to-SQL safety | Substring-based keyword checks can reject valid identifiers and do not provide sufficient protection | Use robust SQL validation and enforce read-only database permissions |
| Airflow security | Default credentials and placeholder secrets are not suitable for shared deployments | Configure secure credentials and secret management before deployment |

## Roadmap

The checklist below separates documentation/review work from infrastructure and end-to-end implementation. Update it as each task is actually completed.

### Review and documentation

- [x] Review the architecture, code structure, and intended data flow.
- [x] Document the pipeline layers, components, and setup sequence.
- [x] Record implementation risks and improvement opportunities.

### Data platform implementation

- [ ] Provision AWS resources (S3 and IAM).
- [ ] Configure Snowflake and secure S3 integration.
- [ ] Load the RAW layer and validate row counts.
- [ ] Run dbt staging and marts successfully.
- [ ] Add and validate the SCD Type 2 snapshot.
- [ ] Add data reconciliation tests.

### Orchestration and AI

- [ ] Fix and validate AI script configuration.
- [ ] Run review enrichment and verify its output.
- [ ] Test the RAG application and source attribution.
- [ ] Test text-to-SQL validation with a read-only role.
- [ ] Run the complete Airflow DAG successfully.
- [ ] Add screenshots of the Airflow DAG, dbt lineage, and Streamlit applications.

## Learning outcomes

Through this project, the goal is to gain practical experience with:

- Designing a cloud-based data lake and warehouse workflow.
- Understanding secure access between AWS and Snowflake.
- Applying the Bronze, Silver, and Gold medallion architecture.
- Building reliable transformations and tests with dbt.
- Orchestrating dependent data tasks with Apache Airflow.
- Integrating LLM-based enrichment into a data pipeline.
- Building introductory RAG and text-to-SQL applications.
- Identifying data quality, security, cost, and operational concerns.

---

**This is an independent learning project.** The implementation status is tracked in the roadmap above; features should be considered complete only after they have been implemented and tested in the repository's current environment.
