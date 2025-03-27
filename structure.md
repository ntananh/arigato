job-search-pipeline/
│
├── docker-compose.yaml          # Docker Compose for Airflow setup
├── Dockerfile                   # Custom Airflow image definition
├── requirements.txt             # Python dependencies
├── README.md                    # Project documentation
│
├── dags/                        # Airflow DAGs directory
│   ├── job_search_pipeline.py   # Main pipeline DAG
│   ├── notifications_dag.py     # Notification-specific DAG
│   └── data_quality_dag.py      # Data quality checks
│
├── plugins/                     # Custom Airflow plugins
│   ├── __init__.py
│   ├── job_search_hooks/        # Custom hooks
│   │   ├── __init__.py
│   │   ├── upwork_hook.py
│   │   └── linkedin_hook.py
│   │
│   ├── job_search_operators/    # Custom operators
│   │   ├── __init__.py
│   │   ├── job_source_operator.py
│   │   ├── job_matching_operator.py
│   │   └── notification_operator.py
│   │
│   └── ml_models/               # Machine learning model plugins
│       ├── __init__.py
│       └── job_matcher_model.py
│
├── include/                     # Additional project files
│   ├── config/                  # Configuration files
│   │   ├── job_preferences.yaml
│   │   └── airflow_config.yaml
│   │
│   ├── sql/                     # SQL scripts for data operations
│   │   ├── create_job_table.sql
│   │   └── insert_job_data.sql
│   │
│   └── ml_models/               # Serialized ML models
│       └── job_matcher_model.pkl
│
├── tests/                       # Test suite
│   ├── dags/                    # DAG validation tests
│   │   └── test_dag_integrity.py
│   │
│   ├── operators/               # Operator tests
│   │   └── test_job_search_operators.py
│   │
│   └── hooks/                   # Hook tests
│       └── test_job_source_hooks.py
│
├── logs/                        # Airflow logs
├── plugins/                     # Airflow plugins
└── .env                         # Environment variables
