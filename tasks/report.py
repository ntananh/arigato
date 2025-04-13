import os
import json
import logging
import datetime
import csv
from typing import Dict, Any, List, Optional
import matplotlib.pyplot as plt
import numpy as np

logger = logging.getLogger(__name__)

class ReportTask:

    def generate_report(
        self,
        matched_jobs: List[Dict[str, Any]],
        report_format: str = 'html',
        output_file: Optional[str] = None
    ) -> str:
        if not matched_jobs:
            logger.warning("No jobs to include in report")
            return ""

        logger.info(f"Generating {report_format} report for {len(matched_jobs)} jobs")

        # Create report directory
        data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
        report_dir = os.path.join(data_dir, 'reports')
        os.makedirs(report_dir, exist_ok=True)

        # Generate timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        # Generate report based on format
        if report_format.lower() == 'html':
            return self._generate_html_report(matched_jobs, report_dir, output_file, timestamp)
        elif report_format.lower() == 'csv':
            return self._generate_csv_report(matched_jobs, report_dir, output_file, timestamp)
        elif report_format.lower() == 'json':
            return self._generate_json_report(matched_jobs, report_dir, output_file, timestamp)
        else:
            logger.error(f"Unsupported report format: {report_format}")
            return ""

    def _generate_html_report(
        self,
        matched_jobs: List[Dict[str, Any]],
        report_dir: str,
        output_file: Optional[str],
        timestamp: str
    ) -> str:
        if not output_file:
            output_file = f"job_report_{timestamp}.html"

        report_path = os.path.join(report_dir, output_file)

        charts_path = os.path.join(report_dir, f"charts_{timestamp}")
        os.makedirs(charts_path, exist_ok=True)

        score_dist_chart = self._generate_score_distribution_chart(matched_jobs, charts_path)
        skills_chart = self._generate_skills_frequency_chart(matched_jobs, charts_path)
        sources_chart = self._generate_sources_chart(matched_jobs, charts_path)

        job_details = []
        for i, job in enumerate(matched_jobs[:20], 1):
            match_score = job.get('match_score', 0)
            match_percentage = f"{match_score * 100:.1f}%"

            skills = job.get('skills', [])
            if isinstance(skills, str):
                skills = [s.strip() for s in skills.split(',') if s.strip()]
            skills_text = ", ".join(skills)

            job_details.append(f"""
            <div class="job-card">
                <h3 class="job-title">{i}. {job.get('title', '')} <span class="match-score">{match_percentage}</span></h3>
                <div class="job-details">
                    <p><strong>Company:</strong> {job.get('company', '')}</p>
                    <p><strong>Location:</strong> {"Remote" if job.get('remote', False) else job.get('location', '')}</p>
                    <p><strong>Skills:</strong> {skills_text}</p>
                    <p><strong>Source:</strong> {job.get('source', '').title()}</p>
                    <p><a href="{job.get('url', '')}" target="_blank" class="view-button">View Job</a></p>
                </div>
            </div>
            """)

        avg_match_score = sum(job.get('match_score', 0) for job in matched_jobs) / max(1, len(matched_jobs))
        sources = {}
        for job in matched_jobs:
            source = job.get('source', 'unknown')
            sources[source] = sources.get(source, 0) + 1

        source_stats = []
        for source, count in sources.items():
            source_stats.append(f"<li>{source.title()}: {count} jobs</li>")

        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Job Match Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 0; }}
                .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
                h1, h2 {{ color: #0066cc; }}
                .stats-cards {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }}
                .stat-card {{ background-color: #f5f5f5; border-radius: 5px; padding: 15px; flex: 1; min-width: 200px; }}
                .stat-value {{ font-size: 24px; font-weight: bold; color: #0066cc; margin: 10px 0; }}
                .visualizations {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 30px; }}
                .chart-container {{ flex: 1; min-width: 300px; background-color: #fff; border: 1px solid #ddd; border-radius: 5px; padding: 15px; }}
                .chart-img {{ width: 100%; max-width: 100%; height: auto; }}
                .job-card {{ background-color: #fff; border: 1px solid #ddd; border-radius: 5px; padding: 15px; margin-bottom: 20px; }}
                .job-title {{ margin-top: 0; display: flex; justify-content: space-between; align-items: center; }}
                .match-score {{ background-color: #0066cc; color: white; padding: 5px 10px; border-radius: 15px; font-size: 14px; }}
                .view-button {{ display: inline-block; background-color: #0066cc; color: white; padding: 8px 15px; text-decoration: none; border-radius: 5px; }}
                .view-button:hover {{ background-color: #004c99; }}
                .footer {{ margin-top: 30px; font-size: 0.8em; color: #666; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Job Match Report</h1>
                <p>Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

                <div class="stats-cards">
                    <div class="stat-card">
                        <h3>Total Jobs</h3>
                        <div class="stat-value">{len(matched_jobs)}</div>
                    </div>
                    <div class="stat-card">
                        <h3>Average Match Score</h3>
                        <div class="stat-value">{avg_match_score * 100:.1f}%</div>
                    </div>
                    <div class="stat-card">
                        <h3>Sources</h3>
                        <ul>
                            {"".join(source_stats)}
                        </ul>
                    </div>
                </div>

                <h2>Visualizations</h2>
                <div class="visualizations">
                    <div class="chart-container">
                        <h3>Match Score Distribution</h3>
                        <img src="{os.path.basename(score_dist_chart)}" alt="Match Score Distribution" class="chart-img">
                    </div>
                    <div class="chart-container">
                        <h3>Top Skills</h3>
                        <img src="{os.path.basename(skills_chart)}" alt="Top Skills" class="chart-img">
                    </div>
                    <div class="chart-container">
                        <h3>Job Sources</h3>
                        <img src="{os.path.basename(sources_chart)}" alt="Job Sources" class="chart-img">
                    </div>
                </div>

                <h2>Top Matched Jobs</h2>
                {"".join(job_details)}

                <div class="footer">
                    <p>This report was generated automatically by the Job Search Pipeline.</p>
                </div>
            </div>
        </body>
        </html>
        """

        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"HTML report generated at {report_path}")
            return report_path
        except Exception as e:
            logger.error(f"Error generating HTML report: {str(e)}")
            return ""

    def _generate_csv_report(
        self,
        matched_jobs: List[Dict[str, Any]],
        report_dir: str,
        output_file: Optional[str],
        timestamp: str
    ) -> str:
        if not output_file:
            output_file = f"job_report_{timestamp}.csv"

        report_path = os.path.join(report_dir, output_file)

        try:
            fieldnames = set()
            for job in matched_jobs:
                fieldnames.update(job.keys())
            fieldnames = sorted(list(fieldnames))

            with open(report_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for job in matched_jobs:
                    # Handle skills list
                    if 'skills' in job and isinstance(job['skills'], list):
                        job['skills'] = ','.join(job['skills'])

                    writer.writerow(job)

            logger.info(f"CSV report generated at {report_path}")
            return report_path

        except Exception as e:
            logger.error(f"Error generating CSV report: {str(e)}")
            return ""

    def _generate_json_report(
        self,
        matched_jobs: List[Dict[str, Any]],
        report_dir: str,
        output_file: Optional[str],
        timestamp: str
    ) -> str:
        if not output_file:
            output_file = f"job_report_{timestamp}.json"

        report_path = os.path.join(report_dir, output_file)

        try:
            jobs_for_json = []
            for job in matched_jobs:
                job_copy = job.copy()
                if 'skills' in job_copy and isinstance(job_copy['skills'], list):
                    job_copy['skills'] = ','.join(job_copy['skills'])
                jobs_for_json.append(job_copy)

            # Create report data structure
            report_data = {
                "generated_at": datetime.datetime.now().isoformat(),
                "total_jobs": len(matched_jobs),
                "avg_match_score": sum(job.get('match_score', 0) for job in matched_jobs) / max(1, len(matched_jobs)),
                "jobs": jobs_for_json
            }

            # Write to JSON file
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2)

            logger.info(f"JSON report generated at {report_path}")
            return report_path

        except Exception as e:
            logger.error(f"Error generating JSON report: {str(e)}")
            return ""

    def _generate_score_distribution_chart(
        self,
        matched_jobs: List[Dict[str, Any]],
        chart_dir: str
    ) -> str:
        try:
            scores = [job.get('match_score', 0) * 100 for job in matched_jobs]

            plt.figure(figsize=(10, 6))

            # Create histogram
            plt.hist(scores, bins=10, color='#0066cc', alpha=0.7, edgecolor='black')

            # Add labels and title
            plt.xlabel('Match Score (%)')
            plt.ylabel('Number of Jobs')
            plt.title('Distribution of Job Match Scores')

            # Add grid
            plt.grid(axis='y', alpha=0.3)

            # Set x-axis limits and ticks
            plt.xlim(0, 100)
            plt.xticks(range(0, 101, 10))

            # Save figure
            output_path = os.path.join(chart_dir, 'score_distribution.png')
            plt.savefig(output_path, bbox_inches='tight')
            plt.close()

            return output_path

        except Exception as e:
            logger.error(f"Error generating score distribution chart: {str(e)}")
            return ""

    def _generate_skills_frequency_chart(
        self,
        matched_jobs: List[Dict[str, Any]],
        chart_dir: str
    ) -> str:
        """
        Generate skills frequency chart.

        Parameters:
        -----------
        matched_jobs : List[Dict[str, Any]]
            List of matched jobs
        chart_dir : str
            Directory to save chart

        Returns:
        --------
        str
            Path to the generated chart
        """
        try:
            # Extract skills and count frequency
            skill_counts = {}
            for job in matched_jobs:
                skills = job.get('skills', [])
                if isinstance(skills, str):
                    skills = [s.strip() for s in skills.split(',') if s.strip()]

                for skill in skills:
                    skill_counts[skill] = skill_counts.get(skill, 0) + 1

            # Sort by frequency and get top 10
            top_skills = sorted(skill_counts.items(), key=lambda x: x[1], reverse=True)[:10]

            # Extract names and counts
            skill_names = [skill for skill, _ in top_skills]
            skill_freqs = [count for _, count in top_skills]

            # Create figure
            plt.figure(figsize=(10, 6))

            # Create horizontal bar chart
            plt.barh(skill_names, skill_freqs, color='#2ca02c', alpha=0.7, edgecolor='black')

            # Add labels and title
            plt.xlabel('Frequency')
            plt.ylabel('Skill')
            plt.title('Top 10 Skills in Matched Jobs')

            # Add counts as text
            for i, v in enumerate(skill_freqs):
                plt.text(v + 0.5, i, str(v), va='center')

            # Adjust layout
            plt.tight_layout()

            # Save figure
            output_path = os.path.join(chart_dir, 'skills_frequency.png')
            plt.savefig(output_path, bbox_inches='tight')
            plt.close()

            return output_path

        except Exception as e:
            logger.error(f"Error generating skills frequency chart: {str(e)}")
            return ""

    def _generate_sources_chart(
        self,
        matched_jobs: List[Dict[str, Any]],
        chart_dir: str
    ) -> str:
        """
        Generate job sources pie chart.

        Parameters:
        -----------
        matched_jobs : List[Dict[str, Any]]
            List of matched jobs
        chart_dir : str
            Directory to save chart

        Returns:
        --------
        str
            Path to the generated chart
        """
        try:
            # Count jobs by source
            source_counts = {}
            for job in matched_jobs:
                source = job.get('source', 'unknown')
                source_counts[source] = source_counts.get(source, 0) + 1

            # Extract sources and counts
            sources = list(source_counts.keys())
            counts = list(source_counts.values())

            # Create figure
            plt.figure(figsize=(8, 8))

            # Create pie chart
            plt.pie(counts, labels=sources, autopct='%1.1f%%', startangle=90,
                    shadow=True, colors=plt.cm.Paired(np.linspace(0, 1, len(sources))))

            # Equal aspect ratio ensures that pie is drawn as a circle
            plt.axis('equal')

            # Add title
            plt.title('Job Sources Distribution')

            # Save figure
            output_path = os.path.join(chart_dir, 'sources_pie.png')
            plt.savefig(output_path, bbox_inches='tight')
            plt.close()

            return output_path

        except Exception as e:
            logger.error(f"Error generating sources chart: {str(e)}")
            return ""