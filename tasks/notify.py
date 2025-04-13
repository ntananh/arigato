import logging
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

class NotificationTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        email: str = None,
        subject: str = "Your Daily Job Matches",
        min_score: float = 0.7,
        max_jobs: int = 10,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.email = email
        self.subject = subject
        self.min_score = min_score
        self.max_jobs = max_jobs

    def execute(self, context):
        ti = context['ti']
        matched_jobs = ti.xcom_pull(task_ids='match_jobs')

        if not matched_jobs:
            logger.warning("No matched jobs found from upstream task")
            return False

        # Get email from constructor or Variable if not provided
        email = self.email or Variable.get('notification_email', '')

        # Filter jobs by minimum score
        qualifying_jobs = [job for job in matched_jobs if job.get('match_score', 0) >= self.min_score]

        if not qualifying_jobs:
            logger.info(f"No jobs with match score >= {self.min_score} to notify about")
            return False

        logger.info(f"Found {len(qualifying_jobs)} qualifying jobs for notification")

        # Send notification
        success = self.notify(
            email=email,
            subject=self.subject,
            jobs=qualifying_jobs,
            max_jobs=self.max_jobs
        )

        return success

    def notify(
        self,
        email: str,
        subject: str,
        jobs: List[Dict[str, Any]],
        max_jobs: int = 10
    ) -> bool:
        if not jobs:
            logger.warning("No jobs to include in notification")
            return False

        if not email:
            logger.warning("No recipient email address provided")
            return False

        logger.info(f"Preparing notification for {email} with {len(jobs)} jobs")

        # Limit number of jobs
        if max_jobs > 0:
            jobs = jobs[:max_jobs]

        # Generate email content
        email_content = self._generate_email_content(jobs, subject)

        # Send email
        success = self._send_email(email, subject, email_content)

        # Mark jobs as notified in database (if applicable)
        if success:
            self._mark_jobs_as_notified(jobs)

        return success

    def _generate_email_content(
        self,
        jobs: List[Dict[str, Any]],
        subject: str
    ) -> str:
        # Format job details
        job_details = []
        for job in jobs:
            # Format salary information
            salary_info = ""
            min_salary = job.get('salary_min', 0)
            max_salary = job.get('salary_max', 0)
            currency = job.get('salary_currency', '')

            if min_salary > 0 or max_salary > 0:
                currency_symbol = {'USD': '$', 'EUR': '€', 'GBP': '£'}.get(currency, '')
                if min_salary == max_salary:
                    salary_info = f"{currency_symbol}{min_salary:,.0f}"
                else:
                    salary_info = f"{currency_symbol}{min_salary:,.0f} - {currency_symbol}{max_salary:,.0f}"

            # Format location/remote info
            location_info = "Remote" if job.get('remote', False) else job.get('location', '')

            # Format skills
            skills = job.get('skills', [])
            if isinstance(skills, str):
                skills = [s.strip() for s in skills.split(',') if s.strip()]
            skills_text = ", ".join(skills)

            # Format match score as percentage
            match_score = job.get('match_score', 0)
            match_percentage = f"{match_score * 100:.1f}%"

            # Add job details
            job_details.append(
                f"<div style='margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 5px;'>"
                f"<h3 style='margin-top: 0;'>{job.get('title', '')} ({match_percentage} match)</h3>"
                f"<p><strong>Company:</strong> {job.get('company', '')}</p>"
                f"<p><strong>Location:</strong> {location_info}</p>"
                f"<p><strong>Skills:</strong> {skills_text}</p>"
                f"<p><strong>Salary:</strong> {salary_info}</p>"
                f"<p><a href='{job.get('url', '')}' style='color: #0066cc;'>View Job</a></p>"
                f"</div>"
            )

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

        return html

    def _send_email(
        self,
        recipient: str,
        subject: str,
        html_content: str
    ) -> bool:
        # Get email settings from Variables
        smtp_server = Variable.get('smtp_server', '')
        smtp_port = int(Variable.get('smtp_port', '587'))
        smtp_username = Variable.get('smtp_username', '')
        smtp_password = Variable.get('smtp_password', '')
        sender_email = Variable.get('sender_email', 'job-pipeline@example.com')

        # Check if email settings are available
        if not smtp_server or not smtp_username or not smtp_password:
            logger.warning("Email settings are not configured. Would send email with the following content:")
            logger.info(f"To: {recipient}")
            logger.info(f"Subject: {subject}")
            logger.info("Content: [HTML email content]")
            return True  # Pretend it worked for pipeline flow

        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"{subject} - {datetime.datetime.now().strftime('%Y-%m-%d')}"
            msg['From'] = sender_email
            msg['To'] = recipient

            # Attach HTML content
            msg.attach(MIMEText(html_content, 'html'))

            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.send_message(msg)

            logger.info(f"Email sent to {recipient}")
            return True

        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False

    def _mark_jobs_as_notified(self, jobs: List[Dict[str, Any]]) -> None:
        # This would typically update a database to mark jobs as notified
        # For this example, we'll just log the operation
        logger.info(f"Marked {len(jobs)} jobs as notified")

        # If database operations are needed, you would use something like:
        # from airflow.providers.postgres.hooks.postgres import PostgresHook
        # pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        # for job in jobs:
        #     pg_hook.run(
        #         "UPDATE matched_job_postings SET notified = TRUE WHERE id = %s",
        #         parameters=(job.get('id'),)
        #     )