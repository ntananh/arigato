"""
NotificationOperator - Sends notifications for high-matching jobs
"""
import os
import json
import logging
from typing import Dict, Any, List
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class NotificationOperator(BaseOperator):
    """
    Operator that sends notifications for jobs that match user preferences.

    Parameters:
    -----------
    email : str
        Email address to send notifications to
    email_subject : str
        Subject for the email notification
    min_score : float
        Minimum match score for jobs to be included in notification
    max_jobs : int
        Maximum number of jobs to include in notification
    """

    @apply_defaults
    def __init__(
        self,
        email: str,
        email_subject: str,
        min_score: float = 0.7,
        max_jobs: int = 5,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.email = email
        self.email_subject = email_subject
        self.min_score = min_score
        self.max_jobs = max_jobs

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the notification process
        """
        self.log.info("Starting job notification process...")

        # Get top matching jobs
        top_jobs = self._get_top_matching_jobs()

        if not top_jobs:
            self.log.info("No qualifying jobs found for notification")
            return

        self.log.info(f"Found {len(top_jobs)} jobs for notification")

        # Generate and send email notification
        self._send_email_notification(top_jobs)

        # Mark jobs as notified
        self._mark_jobs_as_notified([job['id'] for job in top_jobs])

        # Generate dashboard data
        self._generate_dashboard_data()

        self.log.info("Job notification process completed")

    def _get_top_matching_jobs(self) -> List[Dict[str, Any]]:
        """Get top matching jobs from database"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Get jobs with match score above threshold that haven't been notified yet
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
            parameters=(self.min_score, self.max_jobs)
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

        return jobs

    def _send_email_notification(self, jobs: List[Dict[str, Any]]) -> None:
        """
        Send email notification with top matching jobs

        Note: In a real implementation, you would configure email settings
        properly in Airflow and use EmailOperator or similar.
        This is a simplified example.
        """
        self.log.info(f"Preparing email notification for {len(jobs)} jobs")

        # Format job details
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

            # Format location/remote info
            location_info = "Remote" if job['remote'] else job['location']

            # Format skills as comma-separated list
            skills_text = ", ".join(job['skills'])

            # Format match score as percentage
            match_percentage = f"{job['match_score'] * 100:.1f}%"

            # Add job details
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
        msg['Subject'] = f"{self.email_subject} - {datetime.datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = "job-pipeline@example.com"  # Replace with actual sender
        msg['To'] = self.email

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

        # Attach HTML content
        msg.attach(MIMEText(html, 'html'))

        # Log email content for debugging (would actually send in production)
        self.log.info(f"Email notification prepared with {len(jobs)} jobs")
        self.log.info(f"Email would be sent to: {self.email}")

        # In a real implementation, you would send the email here
        # For this example, we'll just log that we would send it
        self.log.info("Email notification would be sent here in production")

    def _mark_jobs_as_notified(self, job_ids: List[int]) -> None:
        """Mark jobs as notified in the database"""
        if not job_ids:
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Update matched_job_postings to set notified=TRUE
        for job_id in job_ids:
            pg_hook.run(
                """
                UPDATE matched_job_postings
                SET notified = TRUE
                WHERE id = %s
                """,
                parameters=(job_id,)
            )

        self.log.info(f"Marked {len(job_ids)} jobs as notified")

    def _generate_dashboard_data(self) -> None:
        """
        Generate data for the dashboard visualization

        In a real implementation, this would prepare data for a dashboard
        service like Streamlit, Tableau, or a custom web app.
        """
        self.log.info("Generating dashboard data")

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Get all matched jobs with score info
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

        # Convert to list of dictionaries for CSV export
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

        # In a real implementation, would save this data to a file or service
        # For this example, we'll just log the number of records
        self.log.info(f"Generated dashboard data with {len(dashboard_data)} records")

        # Export to CSV for dashboard to use
        if dashboard_data:
            data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
            os.makedirs(data_dir, exist_ok=True)

            csv_path = os.path.join(data_dir, 'dashboard_data.csv')

            # Write CSV header and data
            try:
                import csv
                with open(csv_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=dashboard_data[0].keys())
                    writer.writeheader()
                    writer.writerows(dashboard_data)
                self.log.info(f"Dashboard data exported to {csv_path}")
            except Exception as e:
                self.log.error(f"Error exporting dashboard data: {str(e)}")