import os
import re
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def collect_jobs_from_rapidapi_linkedin(job_types, keywords, locations, max_results, **kwargs):
    import json
    from plugins.job_board_hooks.rapid_api_hook import DynamicRapidApiHook

    ti = kwargs['ti']

    headers_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config', 'headers.json')
    try:
        with open(headers_path, 'r') as headers_file:
            headers_config = json.load(headers_file)
    except FileNotFoundError:
        print(f"Headers file not found at {headers_path}, using default headers")
        headers_config = {
            'linkedin': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.linkedin.com/jobs/'
            }
        }

    hook = DynamicRapidApiHook.create_hook("upwork")

    all_jobs = []
    for job_type in job_types:
        for location in locations:
            print(f"Searching for {job_type} jobs in {location}")
            results = hook.search_jobs(
                job_type=job_type,
                location=location,
                keywords=keywords,
                max_results=max_results
            )
            all_jobs.extend(results)

    ti.xcom_push(key='linkedin_jobs', value=all_jobs)

    return len(all_jobs)

def collect_jobs_from_rapidapi_upwork(job_types, keywords, locations, max_results, **kwargs):
    from plugins.job_board_hooks.rapid_api_hook import DynamicRapidApiHook
    import json
    import os

    ti = kwargs['ti']

    headers_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config', 'headers.json')
    try:
        with open(headers_path, 'r') as headers_file:
            headers_config = json.load(headers_file)
    except FileNotFoundError:
        print(f"Headers file not found at {headers_path}, using default headers")
        headers_config = {
            'rapidapi': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json'
            }
        }

    rapidapi_key = os.environ.get('RAPIDAPI_KEY', '')
    if not rapidapi_key:
        raise ValueError("RAPIDAPI_KEY environment variable is required")

    hook = DynamicRapidApiHook.create_hook("linkedin")

    all_jobs = []
    for job_type in job_types:
        for location in locations:
            print(f"Searching for {job_type} jobs in {location}")
            results = hook.search_jobs(
                job_type=job_type,
                location=location,
                keywords=keywords,
                max_results=max_results
            )
            all_jobs.extend(results)

    ti.xcom_push(key='upwork_jobs', value=all_jobs)

    return len(all_jobs)

def process_and_match_jobs(**kwargs):
    """Process and match collected jobs with preferences"""
    import sys
    import os

    plugins_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'plugins')
    if plugins_path not in sys.path:
        sys.path.insert(0, plugins_path)

    try:
        from plugins.ml_models.job_matcher_model import JobMatcherModel
    except ImportError:
        print("Could not import JobMatcherModel, using simplified version")

        class JobMatcherModel:
            def __init__(self, preferences_path):
                import yaml
                try:
                    with open(preferences_path, 'r') as file:
                        self.preferences = yaml.safe_load(file)
                except Exception:
                    self.preferences = {}

            def match_job(self, job_data):
                skills = job_data.get('skills', [])
                must_have = self.preferences.get('skills', {}).get('must_have', [])

                matches = sum(1 for skill in must_have if skill in skills)
                match_score = matches / max(1, len(must_have)) if must_have else 0.5

                return {
                    'match_score': match_score,
                    'skill_score': match_score,
                    'location_score': 0.5,
                    'salary_score': 0.5,
                    'company_score': 0.5,
                    'description_score': 0.5
                }

    ti = kwargs['ti']

    linkedin_jobs = ti.xcom_pull(task_ids='collect_linkedin_jobs', key='linkedin_jobs')
    upwork_jobs = ti.xcom_pull(task_ids='collect_upwork_jobs', key='upwork_jobs')

    preferences_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config', 'job_preferences.yaml')
    matcher = JobMatcherModel(preferences_path)

    processed_jobs = []
    matched_jobs = []

    pg_hook = PostgresHook(postgres_conn_id='postgres_default') # type: ignore

    if linkedin_jobs:
        for job in linkedin_jobs:
            raw_job_id = store_raw_job(pg_hook, 'linkedin', job.get('id', ''), job)

            processed_job = process_linkedin_job(raw_job_id, job)

            processed_id = store_processed_job(pg_hook, processed_job)

            match_scores = matcher.match_job(processed_job)

            store_match_results(pg_hook, processed_id, match_scores)

            processed_jobs.append(processed_job)
            matched_jobs.append({**processed_job, **match_scores})

    if upwork_jobs:
        for job in upwork_jobs:
            raw_job_id = store_raw_job(pg_hook, 'upwork', job.get('id', ''), job)

            processed_job = process_upwork_job(raw_job_id, job)

            processed_id = store_processed_job(pg_hook, processed_job)

            match_scores = matcher.match_job(processed_job)

            store_match_results(pg_hook, processed_id, match_scores)

            processed_jobs.append(processed_job)
            matched_jobs.append({**processed_job, **match_scores})

    ti.xcom_push(key='processed_jobs', value=processed_jobs)
    ti.xcom_push(key='matched_jobs', value=matched_jobs)

    return len(matched_jobs)

def send_notifications(email, email_subject, min_score, max_jobs, **kwargs):
    import json
    import datetime
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    results = pg_hook.get_records(
        """
        SELECT
            m.id,
            p.job_title,
            p.company_name,
            p.location,
            p.url,
            p.skills,
            p.salary_min,
            p.salary_max,
            p.salary_currency,
            p.remote,
            m.match_score,
            m.skill_score,
            m.location_score,
            m.salary_score
        FROM matched_job_postings m
        JOIN processed_job_postings p ON m.processed_job_id = p.id
        WHERE m.match_score >= %s
        AND m.notified = FALSE
        ORDER BY m.match_score DESC
        LIMIT %s
        """,
        parameters=(min_score, max_jobs)
    )

    # Convert to list of dictionaries
    jobs = []
    for row in results:
        jobs.append({
            'id': row[0],
            'job_title': row[1],
            'company_name': row[2],
            'location': row[3],
            'url': row[4],
            'skills': json.loads(row[5]) if row[5] else [],
            'salary_min': row[6],
            'salary_max': row[7],
            'salary_currency': row[8],
            'remote': row[9],
            'match_score': row[10],
            'skill_score': row[11],
            'location_score': row[12],
            'salary_score': row[13]
        })

    if not jobs:
        print("No qualifying jobs found for notification")
        return 0

    print(f"Found {len(jobs)} jobs for notification")

    job_details = []
    for job in jobs:
        # Format salary information
        salary_info = ""
        if job['salary_min'] > 0 or job['salary_max'] > 0:
            currency_symbol = {'USD': '$', 'EUR': '€', 'GBP': '£'}.get(job['salary_currency'], '')
            if job['salary_min'] == job['salary_max']:
                salary_info = f"{currency_symbol}{job['salary_min']:,.0f}"
            else:
                salary_info = f"{currency_symbol}{job['salary_min']:,.0f} - {currency_symbol}{job['salary_max']:,.0f}"

        location_info = "Remote" if job['remote'] else job['location']

        skills_text = ", ".join(job['skills'])

        match_percentage = f"{job['match_score'] * 100:.1f}%"

        job_details.append(
            f"<div style='margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 5px;'>"
            f"<h3 style='margin-top: 0;'>{job['job_title']} ({match_percentage} match)</h3>"
            f"<p><strong>Company:</strong> {job['company_name']}</p>"
            f"<p><strong>Location:</strong> {location_info}</p>"
            f"<p><strong>Skills:</strong> {skills_text}</p>"
            f"<p><strong>Salary:</strong> {salary_info}</p>"
            f"<p><a href='{job['url']}' style='color: #0066cc;'>View Job</a></p>"
            f"</div>"
        )

    # Create email
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"{email_subject} - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = "job-pipeline@example.com"  # Replace with actual sender
    msg['To'] = email

    # Create HTML content
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            h1 {{ color: #0066cc; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .footer {{ margin-top: 30px; font-size: 0.8em; color: #666; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Job Matches - {datetime.datetime.now().strftime('%Y-%m-%d')}</h1>
            <p>Here are your top {len(jobs)} job matches for today:</p>

            {"".join(job_details)}

            <div class="footer">
                <p>This is an automated message from your job pipeline.</p>
            </div>
        </div>
    </body>
    </html>
    """

    msg.attach(MIMEText(html, 'html'))

    print(f"Email notification prepared with {len(jobs)} jobs")
    print(f"Email would be sent to: {email}")

    print("Email notification would be sent here in production")

    for job_id in [job['id'] for job in jobs]:
        pg_hook.run(
            """
            UPDATE matched_job_postings
            SET notified = TRUE
            WHERE id = %s
            """,
            parameters=(job_id,)
        )

    generate_dashboard_data(pg_hook)

    return len(jobs)

def generate_dashboard_data(pg_hook):
    import os
    import csv

    print("Generating dashboard data")

    matched_jobs = pg_hook.get_records(
        """
        SELECT
            p.job_title,
            p.company_name,
            p.location,
            p.url,
            p.skills,
            p.remote,
            m.match_score,
            m.skill_score,
            m.location_score,
            m.salary_score,
            m.company_score,
            m.description_score,
            p.processed_at
        FROM matched_job_postings m
        JOIN processed_job_postings p ON m.processed_job_id = p.id
        ORDER BY m.match_score DESC
        LIMIT 100
        """
    )

    dashboard_data = []
    for row in matched_jobs:
        dashboard_data.append({
            'job_title': row[0],
            'company_name': row[1],
            'location': row[2],
            'url': row[3],
            'skills': row[4],
            'remote': row[5],
            'match_score': row[6],
            'skill_score': row[7],
            'location_score': row[8],
            'salary_score': row[9],
            'company_score': row[10],
            'description_score': row[11],
            'processed_at': row[12].isoformat() if row[12] else None
        })

    if dashboard_data:
        data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
        os.makedirs(data_dir, exist_ok=True)

        csv_path = os.path.join(data_dir, 'dashboard_data.csv')

        try:
            with open(csv_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=dashboard_data[0].keys())
                writer.writeheader()
                writer.writerows(dashboard_data)
            print(f"Dashboard data exported to {csv_path}")
        except Exception as e:
            print(f"Error exporting dashboard data: {str(e)}")

def store_raw_job(pg_hook, source, source_id, job_data):
    """Store raw job in database and return its ID"""
    import json

    result = pg_hook.get_first(
        """
        INSERT INTO raw_job_postings
        (source, source_id, job_title, company_name, location, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, source_id) DO UPDATE
        SET job_title = EXCLUDED.job_title,
            company_name = EXCLUDED.company_name,
            location = EXCLUDED.location,
            raw_data = EXCLUDED.raw_data,
            collected_at = CURRENT_TIMESTAMP
        RETURNING id
        """,
        parameters=(
            source,
            source_id,
            job_data.get('title', ''),
            job_data.get('company', ''),
            job_data.get('location', ''),
            json.dumps(job_data)
        )
    )

    return result[0] if result else None

def process_linkedin_job(job_id, job_data):
    """Process a LinkedIn job posting"""
    import re

    description = job_data.get('description', '')

    salary_min, salary_max, currency = extract_salary(description)

    skills = extract_skills(description)

    return {
        'raw_job_id': job_id,
        'job_title': job_data.get('title', ''),
        'company_name': job_data.get('company', ''),
        'job_description': description,
        'location': job_data.get('location', ''),
        'job_type': job_data.get('employmentType', ''),
        'salary_min': salary_min,
        'salary_max': salary_max,
        'salary_currency': currency,
        'remote': 'remote' in job_data.get('location', '').lower() or 'remote' in description.lower(),
        'url': job_data.get('url', ''),
        'skills': skills
    }

def process_upwork_job(job_id, job_data):
    """Process an Upwork job posting"""
    import re

    description = job_data.get('description', '')

    # Extract hourly/fixed budget
    budget = job_data.get('budget', '')
    salary_min, salary_max, currency = 0, 0, ''

    if isinstance(budget, str):
        if 'hour' in budget.lower() and '-' in budget:
            # Extract hourly rate range
            rates = re.findall(r'(\d+\.?\d*)', budget)
            if len(rates) >= 2:
                salary_min = float(rates[0])
                salary_max = float(rates[1])
                currency = 'USD'  # Upwork typically uses USD
        elif 'hour' in budget.lower():
            # Extract single hourly rate
            rates = re.findall(r'(\d+\.?\d*)', budget)
            if rates:
                salary_min = salary_max = float(rates[0])
                currency = 'USD'
        elif re.search(r'\d', budget):
            # Extract fixed price
            amount = re.findall(r'(\d+\.?\d*)', budget)
            if amount:
                salary_min = salary_max = float(amount[0])
                currency = 'USD'

    # Extract skills from description
    skills = extract_skills(description)

    return {
        'raw_job_id': job_id,
        'job_title': job_data.get('title', ''),
        'company_name': job_data.get('company', ''),
        'job_description': description,
        'location': 'Remote',  # Upwork jobs are typically remote
        'job_type': job_data.get('type', ''),
        'salary_min': salary_min,
        'salary_max': salary_max,
        'salary_currency': currency,
        'remote': True,
        'url': job_data.get('url', ''),
        'skills': skills
    }

def extract_salary(text):
    """Extract salary information from text"""
    import re

    # Common patterns for salary information
    patterns = [
        # $50,000 - $70,000 or $50k - $70k
        r'(\$[\d,]+(?:\.?\d+)?(?:k)?)\s*(?:-|to)\s*(\$[\d,]+(?:\.?\d+)?(?:k)?)',
        # €50,000 - €70,000 or €50k - €70k
        r'(€[\d,]+(?:\.?\d+)?(?:k)?)\s*(?:-|to)\s*(€[\d,]+(?:\.?\d+)?(?:k)?)',
        # 50,000 - 70,000 EUR/USD/etc.
        r'([\d,]+(?:\.?\d+)?(?:k)?)\s*(?:-|to)\s*([\d,]+(?:\.?\d+)?(?:k)?)\s*(EUR|USD|GBP|€|\$|£)',
        # 50k - 70k EUR/USD/etc.
        r'([\d,]+(?:\.?\d+)?k)\s*(?:-|to)\s*([\d,]+(?:\.?\d+)?k)\s*(EUR|USD|GBP|€|\$|£)',
    ]

    for pattern in patterns:
        matches = re.search(pattern, text, re.IGNORECASE)
        if matches:
            min_salary, max_salary = matches.group(1), matches.group(2)

            # Extract currency
            currency = 'USD'  # Default
            if '$' in min_salary:
                currency = 'USD'
            elif '€' in min_salary:
                currency = 'EUR'
            elif '£' in min_salary:
                currency = 'GBP'
            elif len(matches.groups()) > 2:
                currency_match = matches.group(3)
                if currency_match:
                    if currency_match == '$':
                        currency = 'USD'
                    elif currency_match == '€':
                        currency = 'EUR'
                    elif currency_match == '£':
                        currency = 'GBP'
                    else:
                        currency = currency_match

            # Clean up and convert values
            min_salary = re.sub(r'[^\d.]', '', min_salary.replace('k', '000'))
            max_salary = re.sub(r'[^\d.]', '', max_salary.replace('k', '000'))

            try:
                return float(min_salary), float(max_salary), currency
            except (ValueError, TypeError):
                continue

    return 0, 0, ''

def extract_skills(text):
    tech_skills = [
        'python', 'javascript', 'typescript', 'java', 'c++', 'c#', 'go', 'rust', 'swift',
        'react', 'angular', 'vue', 'node', 'express', 'django', 'flask', 'spring', 'hibernate',
        'sql', 'nosql', 'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch',
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins',
        'git', 'github', 'gitlab', 'bitbucket', 'jira', 'confluence',
        'api', 'rest', 'graphql', 'microservices', 'serverless', 'kafka', 'rabbitmq',
        'machine learning', 'ai', 'data science', 'data engineering', 'big data',
        'agile', 'scrum', 'kanban', 'devops', 'ci/cd', 'tdd', 'unit testing'
    ]

    text_lower = text.lower()

    found_skills = []
    for skill in tech_skills:
        pattern = r'\b' + re.escape(skill.lower()) + r'\b'
        if re.search(pattern, text_lower):
            found_skills.append(skill)

    return list(set(found_skills))

def store_processed_job(pg_hook, job_data):
    import json

    result = pg_hook.get_first(
        """
        INSERT INTO processed_job_postings
        (raw_job_id, job_title, company_name, job_description, location, job_type,
         salary_min, salary_max, salary_currency, remote, url, skills)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (raw_job_id) DO UPDATE
        SET job_title = EXCLUDED.job_title,
            company_name = EXCLUDED.company_name,
            job_description = EXCLUDED.job_description,
            location = EXCLUDED.location,
            job_type = EXCLUDED.job_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            salary_currency = EXCLUDED.salary_currency,
            remote = EXCLUDED.remote,
            url = EXCLUDED.url,
            skills = EXCLUDED.skills,
            processed_at = CURRENT_TIMESTAMP
        RETURNING id
        """,
        parameters=(
            job_data['raw_job_id'],
            job_data['job_title'],
            job_data['company_name'],
            job_data['job_description'],
            job_data['location'],
            job_data['job_type'],
            job_data['salary_min'],
            job_data['salary_max'],
            job_data['salary_currency'],
            job_data['remote'],
            job_data['url'],
            json.dumps(job_data['skills'])
        )
    )

    return result[0] if result else None

def store_match_results(pg_hook, processed_job_id, scores):
    if not processed_job_id:
        print("Cannot store match results: processed job ID is missing")
        return

    pg_hook.run(
        """
        INSERT INTO matched_job_postings
        (processed_job_id, match_score, skill_score, location_score,
         salary_score, company_score, description_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (processed_job_id) DO UPDATE
        SET match_score = EXCLUDED.match_score,
            skill_score = EXCLUDED.skill_score,
            location_score = EXCLUDED.location_score,
            salary_score = EXCLUDED.salary_score,
            company_score = EXCLUDED.company_score,
            description_score = EXCLUDED.description_score,
            matched_at = CURRENT_TIMESTAMP
        """,
        parameters=(
            processed_job_id,
            scores['match_score'],
            scores['skill_score'],
            scores['location_score'],
            scores['salary_score'],
            scores['company_score'],
            scores['description_score']
        )
    )

def process_config(config):
    processed_config = config.copy()

    if 'default_args' in processed_config:
        if isinstance(processed_config['default_args'].get('retry_delay'), dict):
            retry_delay_dict = processed_config['default_args']['retry_delay']

            if 'minutes' in retry_delay_dict:
                processed_config['default_args']['retry_delay'] = timedelta(
                    minutes=retry_delay_dict['minutes']
                )
            elif 'seconds' in retry_delay_dict:
                processed_config['default_args']['retry_delay'] = timedelta(
                    seconds=retry_delay_dict['seconds']
                )
            else:
                processed_config['default_args']['retry_delay'] = timedelta(minutes=5)

        if isinstance(processed_config['default_args'].get('execution_timeout'), dict):
            timeout_dict = processed_config['default_args']['execution_timeout']

            if 'minutes' in timeout_dict:
                processed_config['default_args']['execution_timeout'] = timedelta(
                    minutes=timeout_dict['minutes']
                )
            elif 'hours' in timeout_dict:
                processed_config['default_args']['execution_timeout'] = timedelta(
                    hours=timeout_dict['hours']
                )
            else:
                # Fallback to 1 hour
                processed_config['default_args']['execution_timeout'] = timedelta(hours=1)

    return processed_config

def load_config(config_path):
    default_config = {
        'default_args': {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(hours=1),
        },
        'dag': {
            'schedule_interval': '0 8 * * *',  # Run every day at 8 AM
            'start_date': '2025-03-15',
            'catchup': False,
            'tags': ['jobs', 'data_pipeline', 'project'],
        },
        'email': {
            'to': 'your.email@example.com',
            'subject': 'Job Pipeline Daily Digest',
        },
        'apis': {
            'linkedin': {
                'enabled': True,
                'rate_limit': 10,
                'max_results': 50,
            },
            'upwork': {
                'enabled': True,
                'rate_limit': 10,
                'max_results': 50,
            }
        }
    }
    try:
        with open(config_path, 'r') as config_file:
            loaded_config = yaml.safe_load(config_file) or {}

        def deep_merge(default, override):
            if not isinstance(default, dict) or not isinstance(override, dict):
                return override if override is not None else default

            merged = default.copy()
            for key, value in override.items():
                merged[key] = deep_merge(merged.get(key, {}), value)
            return merged

        merged_config = deep_merge(default_config, loaded_config)

        processed_config = process_config(merged_config)

        print("Configuration loaded successfully.")
        return processed_config

    except FileNotFoundError:
        print(f"Config file not found at {config_path}. Using default configuration.")
        return default_config
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        return default_config

config_paths = [
    os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config', 'airflow_config.yaml'),
    os.path.join(os.path.dirname(__file__), 'airflow_config.yaml'),
    os.path.join(os.getcwd(), 'airflow_config.yaml')
]

config = None
for path in config_paths:
    config = load_config(path)
    if config:
        break

if not config:
    config = {
        'default_args': {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(hours=1),
        },
        'dag': {
            'schedule_interval': '0 8 * * *',
            'start_date': '2025-03-15',
            'catchup': False,
            'tags': ['jobs', 'data_pipeline', 'project'],
        }
    }

dag = DAG(
    'job_search_pipeline',
    default_args=config['default_args'],
    description='Collects, processes, and matches job postings based on preferences',
    schedule_interval=config['dag']['schedule_interval'],
    start_date=datetime.fromisoformat(config['dag']['start_date']) if isinstance(config['dag']['start_date'], str) else config['dag']['start_date'],
    catchup=config['dag']['catchup'],
    tags=config['dag']['tags'],
)

try:
    sql_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'sql', 'create_job_table.sql')
    with open(sql_path, 'r') as f:
        sql = f.read()
except FileNotFoundError:
    print(f"SQL file not found at {sql_path}, using default SQL")
    sql = """
    -- Create job tables for the data pipeline

    -- Raw job postings table
    CREATE TABLE IF NOT EXISTS raw_job_postings (
        id SERIAL PRIMARY KEY,
        source VARCHAR(50) NOT NULL,
        source_id VARCHAR(100) NOT NULL,
        job_title VARCHAR(255),
        company_name VARCHAR(255),
        location VARCHAR(255),
        raw_data JSONB NOT NULL,
        collected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, source_id)
    );

    -- Processed job postings table
    CREATE TABLE IF NOT EXISTS processed_job_postings (
        id SERIAL PRIMARY KEY,
        raw_job_id INTEGER NOT NULL REFERENCES raw_job_postings(id),
        job_title VARCHAR(255) NOT NULL,
        company_name VARCHAR(255) NOT NULL,
        job_description TEXT,
        location VARCHAR(255),
        job_type VARCHAR(100),
        salary_min NUMERIC,
        salary_max NUMERIC,
        salary_currency VARCHAR(10),
        remote BOOLEAN,
        url VARCHAR(500),
        skills JSONB,
        processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(raw_job_id)
    );

    -- Matched job postings table
    CREATE TABLE IF NOT EXISTS matched_job_postings (
        id SERIAL PRIMARY KEY,
        processed_job_id INTEGER NOT NULL REFERENCES processed_job_postings(id),
        match_score NUMERIC NOT NULL,
        skill_score NUMERIC,
        location_score NUMERIC,
        salary_score NUMERIC,
        company_score NUMERIC,
        description_score NUMERIC,
        notified BOOLEAN DEFAULT FALSE,
        matched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(processed_job_id)
    );
    """

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql=sql,
    dag=dag,
)

preferences_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config', 'job_preferences.yaml')
try:
    with open(preferences_path, 'r') as pref_file:
        preferences = yaml.safe_load(pref_file)
except FileNotFoundError:
    print(f"Preferences file not found at {preferences_path}, using default preferences")
    preferences = {
        'job_types': ['backend', 'software engineer', 'python developer'],
        'skills': {
            'must_have': ['python', 'sql', 'api'],
            'nice_to_have': ['docker', 'kubernetes', 'aws', 'data engineering']
        },
        'locations': {
            'remote': True,
            'countries': ['Finland', 'Sweden', 'Norway', 'Denmark', 'Estonia']
        },
        'keywords': ['python', 'backend', 'api', 'sql'],
        'notification': {
            'min_score': 0.7,
            'daily_limit': 5
        }
    }

linkedin_jobs = PythonOperator(
    task_id='collect_linkedin_jobs',
    python_callable=collect_jobs_from_rapidapi_linkedin,
    op_kwargs={
        'job_types': preferences.get('job_types', []),
        'keywords': preferences.get('skills', {}).get('must_have', []) + preferences.get('keywords', []),
        'locations': ['remote'] + preferences.get('locations', {}).get('countries', []),
        'max_results': config['apis']['linkedin']['max_results'],
    },
    dag=dag,
)

upwork_jobs = PythonOperator(
    task_id='collect_upwork_jobs',
    python_callable=collect_jobs_from_rapidapi_upwork,
    op_kwargs={
        'job_types': preferences.get('job_types', []),
        'keywords': preferences.get('skills', {}).get('must_have', []) + preferences.get('keywords', []),
        'locations': ['remote'],
        'max_results': config['apis']['upwork']['max_results'],
    },
    dag=dag,
)

match_jobs = PythonOperator(
    task_id='match_jobs',
    python_callable=process_and_match_jobs,
    dag=dag,
)

notify = PythonOperator(
    task_id='send_notifications',
    python_callable=send_notifications,
    op_kwargs={
        'email': config['email']['to'],
        'email_subject': config['email']['subject'],
        'min_score': preferences.get('notification', {}).get('min_score', 0.7),
        'max_jobs': preferences.get('notification', {}).get('daily_limit', 5),
    },
    dag=dag,
)

create_tables >> [linkedin_jobs, upwork_jobs] >> match_jobs >> notify