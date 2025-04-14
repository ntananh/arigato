import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import base64
from io import BytesIO
from typing import Optional, List, Dict, Any
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

class HtmlReportTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        output_dir: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        airflow_home = os.environ.get('AIRFLOW_HOME', '')
        self.output_dir = output_dir or os.path.join(airflow_home, 'data', 'reports')

    def execute(self, context):
        ti = context['ti']
        matched_jobs = ti.xcom_pull(task_ids='match_jobs')

        if not matched_jobs:
            logger.warning("No matched jobs found from upstream task")
            return None

        logger.info(f"Generating HTML report for {len(matched_jobs)} jobs")
        os.makedirs(self.output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(self.output_dir, f"job_report_{timestamp}.html")

        html_content = self._generate_html_report(matched_jobs)

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"HTML report generated at: {report_file}")

        ti.xcom_push(key='report_path', value=report_file)
        return report_file

    def _generate_html_report(self, jobs: List[Dict[str, Any]]) -> str:
        df = pd.DataFrame(jobs)

        score_chart = self._create_score_distribution_chart(df)
        skills_chart = self._create_skills_chart(df)
        sources_chart = self._create_sources_chart(df)

        avg_match_score = df['match_score'].mean() if 'match_score' in df.columns else 0
        source_counts = df['source'].value_counts().to_dict() if 'source' in df.columns else {}
        source_stats_html = ''.join([f"<li>{source.title()}: {count} jobs</li>" for source, count in source_counts.items()])

        job_cards_html = self._generate_job_cards(df.sort_values(by='match_score', ascending=False).head(20))

        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Job Match Report</title>
    <style>
        :root {{
            --primary-color: #2563eb;
            --secondary-color: #3b82f6;
            --accent-color: #93c5fd;
            --background-color: #f8fafc;
            --card-color: #ffffff;
            --text-color: #1e293b;
            --text-light: #64748b;
            --border-color: #e2e8f0;
            --success-color: #10b981;
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: var(--text-color);
            background-color: var(--background-color);
            padding: 0;
            margin: 0;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }}

        header {{
            background-color: var(--primary-color);
            color: white;
            padding: 2rem 0;
            margin-bottom: 2rem;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}

        header .container {{
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        h1, h2, h3, h4 {{
            color: var(--primary-color);
            margin-bottom: 1rem;
        }}

        header h1 {{
            color: white;
            margin-bottom: 0.5rem;
        }}

        header p {{
            color: var(--accent-color);
            font-size: 1.1rem;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .stat-card {{
            background-color: var(--card-color);
            border-radius: 0.5rem;
            padding: 1.5rem;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            border: 1px solid var(--border-color);
        }}

        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
            color: var(--primary-color);
            margin: 0.5rem 0;
        }}

        .visualizations {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 2rem;
            margin-bottom: 3rem;
        }}

        .chart-container {{
            background-color: var(--card-color);
            border-radius: 0.5rem;
            padding: 1.5rem;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            border: 1px solid var(--border-color);
        }}

        .chart-img {{
            width: 100%;
            height: auto;
            margin-top: 1rem;
        }}

        .job-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 1.5rem;
            margin-bottom: 3rem;
        }}

        .job-card {{
            background-color: var(--card-color);
            border-radius: 0.5rem;
            padding: 1.5rem;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            border: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
        }}

        .job-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 1rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1rem;
        }}

        .job-title {{
            font-size: 1.25rem;
            font-weight: bold;
            color: var(--primary-color);
            margin: 0;
        }}

        .match-score {{
            background-color: var(--primary-color);
            color: white;
            padding: 0.25rem 0.5rem;
            border-radius: 1rem;
            font-size: 0.85rem;
            font-weight: bold;
        }}

        .job-details {{
            flex-grow: 1;
        }}

        .job-detail {{
            margin-bottom: 0.75rem;
        }}

        .job-label {{
            font-weight: bold;
            margin-right: 0.5rem;
        }}

        .skill-tag {{
            display: inline-block;
            background-color: var(--accent-color);
            color: var(--primary-color);
            padding: 0.25rem 0.5rem;
            border-radius: 1rem;
            font-size: 0.85rem;
            margin: 0.25rem 0.5rem 0.25rem 0;
        }}

        .job-footer {{
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid var(--border-color);
            text-align: right;
        }}

        .view-button {{
            display: inline-block;
            background-color: var(--primary-color);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 0.25rem;
            text-decoration: none;
            font-weight: 500;
            transition: background-color 0.2s;
        }}

        .view-button:hover {{
            background-color: var(--secondary-color);
        }}

        .footer {{
            background-color: var(--primary-color);
            color: white;
            padding: 2rem 0;
            margin-top: 3rem;
            text-align: center;
        }}
    </style>
</head>
<body>
    <header>
        <div class="container">
            <div>
                <h1>Job Match Report</h1>
                <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
            </div>
        </div>
    </header>

    <div class="container">
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Jobs</h3>
                <div class="stat-value">{len(jobs)}</div>
            </div>
            <div class="stat-card">
                <h3>Average Match Score</h3>
                <div class="stat-value">{avg_match_score * 100:.1f}%</div>
            </div>
            <div class="stat-card">
                <h3>Job Sources</h3>
                <ul>
                    {source_stats_html}
                </ul>
            </div>
        </div>

        <h2>Visualizations</h2>
        <div class="visualizations">
            <div class="chart-container">
                <h3>Match Score Distribution</h3>
                <img src="data:image/png;base64,{score_chart}" alt="Match Score Distribution" class="chart-img">
            </div>
            <div class="chart-container">
                <h3>Top Skills</h3>
                <img src="data:image/png;base64,{skills_chart}" alt="Top Skills" class="chart-img">
            </div>
            <div class="chart-container">
                <h3>Job Sources</h3>
                <img src="data:image/png;base64,{sources_chart}" alt="Job Sources" class="chart-img">
            </div>
        </div>

        <h2>Top Job Matches</h2>
        <div class="job-cards">
            {job_cards_html}
        </div>
    </div>

    <footer class="footer">
        <div class="container">
            <p>This report was generated automatically by the Job Search Pipeline.</p>
        </div>
    </footer>
</body>
</html>
        """
        return html

    def _create_score_distribution_chart(self, df):
        plt.figure(figsize=(10, 6))

        scores = df['match_score'].fillna(0) * 100

        plt.hist(scores, bins=10, color='#3b82f6', alpha=0.7, edgecolor='black')
        plt.xlabel('Match Score (%)')
        plt.ylabel('Number of Jobs')
        plt.title('Distribution of Job Match Scores')
        plt.grid(axis='y', alpha=0.3)
        plt.xlim(0, 100)
        plt.xticks(range(0, 101, 10))

        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close()
        buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode('utf-8')

    def _create_skills_chart(self, df):
        plt.figure(figsize=(10, 6))

        all_skills = []
        for skills_list in df['skills']:
            if isinstance(skills_list, list):
                all_skills.extend(skills_list)
            elif isinstance(skills_list, str):
                all_skills.extend([s.strip() for s in skills_list.split(',')])

        skill_counts = pd.Series(all_skills).value_counts().sort_values(ascending=True).tail(15)

        plt.barh(skill_counts.index, skill_counts.values, color='#10b981', alpha=0.7, edgecolor='black')
        plt.xlabel('Frequency')
        plt.ylabel('Skill')
        plt.title('Top Skills in Matched Jobs')
        plt.grid(axis='x', alpha=0.3)
        plt.tight_layout()

        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close()
        buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode('utf-8')

    def _create_sources_chart(self, df):
        plt.figure(figsize=(8, 8))

        source_counts = df['source'].value_counts()
        plt.pie(source_counts, labels=source_counts.index, autopct='%1.1f%%',
                startangle=90, colors=plt.cm.tab10.colors)
        plt.axis('equal')
        plt.title('Job Sources Distribution')

        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        plt.close()
        buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode('utf-8')

    def _generate_job_cards(self, df):
        job_cards = []

        for _, job in df.iterrows():
            match_score = job.get('match_score', 0)
            match_percentage = f"{match_score * 100:.1f}%"

            skills = job.get('skills', [])
            if isinstance(skills, str):
                skills = [s.strip() for s in skills.split(',') if s.strip()]

            skills_html = ''.join([f'<span class="skill-tag">{skill}</span>' for skill in skills[:5]])
            if len(skills) > 5:
                skills_html += f'<span class="skill-tag">+{len(skills) - 5} more</span>'

            location = "Remote" if job.get('remote', False) else job.get('location', 'Not specified')

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

            job_card = f"""
                <div class="job-card">
                    <div class="job-header">
                        <h3 class="job-title">{job.get('title', 'Untitled Job')}</h3>
                        <span class="match-score">{match_percentage}</span>
                    </div>
                    <div class="job-details">
                        <div class="job-detail">
                            <span class="job-label">Company:</span>
                            {job.get('company', 'Unknown')}
                        </div>
                        <div class="job-detail">
                            <span class="job-label">Location:</span>
                            {location}
                        </div>
                        <div class="job-detail">
                            <span class="job-label">Source:</span>
                            {job.get('source', 'Unknown')}
                        </div>
                        {f'<div class="job-detail"><span class="job-label">Salary:</span>{salary_info}</div>' if salary_info else ''}
                        <div class="job-detail">
                            <span class="job-label">Skills:</span><br>
                            {skills_html}
                        </div>
                    </div>
                    <div class="job-footer">
                        <a href="{job.get('url', '#')}" class="view-button" target="_blank">View Job</a>
                    </div>
                </div>
            """
            job_cards.append(job_card)

        return ''.join(job_cards)