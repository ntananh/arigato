"""
JobMatchingOperator - Processes and matches jobs with user preferences
"""
import os
import json
import yaml
import logging
from typing import Dict, Any, List, Tuple
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

class JobMatchingOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        preferences_path: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.preferences_path = preferences_path
        self.preferences = self._load_preferences()
        self.stop_words = set(stopwords.words('english'))

    def _load_preferences(self) -> Dict[str, Any]:
        with open(self.preferences_path, 'r') as pref_file:
            return yaml.safe_load(pref_file)

    def execute(self, context: Dict[str, Any]) -> None:
        self.log.info("Starting job processing and matching...")

        # Get unprocessed jobs
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        raw_jobs = pg_hook.get_records(
            """
            SELECT r.id, r.source, r.source_id, r.raw_data
            FROM raw_job_postings r
            LEFT JOIN processed_job_postings p ON r.id = p.raw_job_id
            WHERE p.id IS NULL
            ORDER BY r.collected_at DESC
            """
        )

        self.log.info(f"Found {len(raw_jobs)} unprocessed job postings")

        # Process each job
        for job_id, source, source_id, raw_data in raw_jobs:
            try:
                # Parse the raw data
                job_data = json.loads(raw_data)

                # Process the job
                processed_job = self._process_job(job_id, source, job_data)

                # Store processed job
                processed_id = self._store_processed_job(processed_job)

                # Match against preferences
                match_scores = self._match_job(processed_job)

                # Store match results
                self._store_match_results(processed_id, match_scores)

            except Exception as e:
                self.log.error(f"Error processing job {job_id}: {str(e)}")

        self.log.info("Job processing and matching completed")

    def _process_job(self, job_id: int, source: str, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a raw job posting into a standardized format"""
        # Extract job details differently based on source
        if source == 'linkedin':
            return self._process_linkedin_job(job_id, job_data)
        elif source == 'upwork':
            return self._process_upwork_job(job_id, job_data)
        else:
            raise ValueError(f"Unsupported source: {source}")

    def _process_linkedin_job(self, job_id: int, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a LinkedIn job posting"""
        description = job_data.get('description', '')

        # Extract salary range using regex if available
        salary_min, salary_max, currency = self._extract_salary(description)

        # Extract skills from description
        skills = self._extract_skills(description)

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

    def _process_upwork_job(self, job_id: int, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an Upwork job posting"""
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
        skills = self._extract_skills(description)

        return {
            'raw_job_id': job_id,
            'job_title': job_data.get('title', ''),
            'company_name': job_data.get('client', {}).get('name', ''),
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

    def _extract_salary(self, text: str) -> Tuple[float, float, str]:
        """Extract salary information from text using regex patterns"""
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

    def _extract_skills(self, text: str) -> List[str]:
        """Extract potential skills from job description"""
        # Create a list of common tech skills to match against
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

        # Add the skills from preferences
        must_have_skills = self.preferences.get('skills', {}).get('must_have', [])
        nice_to_have_skills = self.preferences.get('skills', {}).get('nice_to_have', [])
        tech_skills.extend(must_have_skills)
        tech_skills.extend(nice_to_have_skills)

        # Lowercase text for case-insensitive matching
        text_lower = text.lower()

        # Find skills in text
        found_skills = []
        for skill in tech_skills:
            # Use word boundary to avoid partial matches
            pattern = r'\b' + re.escape(skill.lower()) + r'\b'
            if re.search(pattern, text_lower):
                found_skills.append(skill)

        return list(set(found_skills))  # Remove duplicates

    def _store_processed_job(self, job_data: Dict[str, Any]) -> int:
        """Store processed job in database and return its ID"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Insert into processed_job_postings table
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

    def _match_job(self, job_data: Dict[str, Any]) -> Dict[str, float]:
        """Match job against user preferences and calculate scores"""
        # Get weights from preferences
        weights = self.preferences.get('scoring', {})
        skill_weight = weights.get('skill_match_weight', 0.4)
        location_weight = weights.get('location_weight', 0.2)
        salary_weight = weights.get('salary_weight', 0.2)
        company_weight = weights.get('company_weight', 0.1)
        description_weight = weights.get('job_description_weight', 0.1)

        # Calculate skill score
        skill_score = self._calculate_skill_score(job_data['skills'])

        # Calculate location score
        location_score = self._calculate_location_score(job_data['location'], job_data['remote'])

        # Calculate salary score
        salary_score = self._calculate_salary_score(job_data['salary_min'], job_data['salary_max'], job_data['salary_currency'])

        # Calculate company score
        company_score = self._calculate_company_score(job_data['company_name'])

        # Calculate description score
        description_score = self._calculate_description_score(job_data['job_description'])

        # Calculate overall score (weighted average)
        overall_score = (
            skill_score * skill_weight +
            location_score * location_weight +
            salary_score * salary_weight +
            company_score * company_weight +
            description_score * description_weight
        )

        return {
            'match_score': overall_score,
            'skill_score': skill_score,
            'location_score': location_score,
            'salary_score': salary_score,
            'company_score': company_score,
            'description_score': description_score
        }

    def _calculate_skill_score(self, job_skills: List[str]) -> float:
        """Calculate match score for job skills"""
        must_have_skills = self.preferences.get('skills', {}).get('must_have', [])
        nice_to_have_skills = self.preferences.get('skills', {}).get('nice_to_have', [])

        # Count matches
        must_have_matches = sum(1 for skill in must_have_skills if skill.lower() in [s.lower() for s in job_skills])
        nice_to_have_matches = sum(1 for skill in nice_to_have_skills if skill.lower() in [s.lower() for s in job_skills])

        # Calculate score
        if not must_have_skills:
            must_have_score = 1.0  # If no must-have skills specified, give full score
        else:
            must_have_score = must_have_matches / len(must_have_skills)

        if not nice_to_have_skills:
            nice_to_have_score = 1.0  # If no nice-to-have skills specified, give full score
        else:
            nice_to_have_score = nice_to_have_matches / len(nice_to_have_skills)

        # Weighted combination (must-have skills are more important)
        return 0.7 * must_have_score + 0.3 * nice_to_have_score

    def _calculate_location_score(self, location: str, is_remote: bool) -> float:
        """Calculate match score for job location"""
        preferred_remote = self.preferences.get('locations', {}).get('remote', False)
        preferred_countries = self.preferences.get('locations', {}).get('countries', [])
        excluded_regions = self.preferences.get('locations', {}).get('exclude_regions', [])

        # If job is remote and user prefers remote, it's a perfect match
        if is_remote and preferred_remote:
            return 1.0

        # Check if job location contains any preferred countries
        location_lower = location.lower()
        country_match = any(country.lower() in location_lower for country in preferred_countries)

        # Check if job location contains any excluded regions
        region_exclude = any(region.lower() in location_lower for region in excluded_regions)

        if region_exclude:
            return 0.0
        elif country_match:
            return 0.8  # Good match but not perfect (not remote)
        elif is_remote and not preferred_remote:
            return 0.5  # Job is remote but user doesn't prefer remote
        else:
            return 0.2  # No match but not completely excluded

    def _calculate_salary_score(self, min_salary: float, max_salary: float, currency: str) -> float:
        """Calculate match score for salary"""
        # Get user's minimum salary preference
        min_preferred_salary = self.preferences.get('min_salary', 0)

        # If no salary information, give a neutral score
        if min_salary == 0 and max_salary == 0:
            return 0.5

        # Convert salary to same currency if needed (simplified)
        # In a real implementation, use a currency conversion API
        converted_min_salary = min_salary
        if currency == 'EUR' and min_salary > 0:
            converted_min_salary = min_salary * 1.1  # Approximate EUR to USD
        elif currency == 'GBP' and min_salary > 0:
            converted_min_salary = min_salary * 1.3  # Approximate GBP to USD

        # Calculate score based on how much the salary exceeds minimum preferred
        if converted_min_salary >= min_preferred_salary * 1.5:
            return 1.0  # Excellent
        elif converted_min_salary >= min_preferred_salary * 1.2:
            return 0.9  # Very good
        elif converted_min_salary >= min_preferred_salary:
            return 0.8  # Good
        elif converted_min_salary >= min_preferred_salary * 0.8:
            return 0.6  # Acceptable
        elif converted_min_salary > 0:
            return 0.4  # Below preference but not terrible
        else:
            return 0.5  # No salary info, neutral score

    def _calculate_company_score(self, company_name: str) -> float:
        """Calculate match score for company"""
        company_types = self.preferences.get('company_types', [])

        # This would ideally use a company database or API to get company type
        # For now, use simple heuristics based on company name
        company_lower = company_name.lower()

        # Simple heuristic checks
        is_startup = any(term in company_lower for term in ['startup', 'labs', 'technologies', 'tech'])
        is_tech = any(term in company_lower for term in ['tech', 'digital', 'software', 'solutions', 'systems'])
        is_established = any(term in company_lower for term in ['inc', 'corp', 'ltd', 'limited', 'group'])

        # Count matches with preferences
        matches = 0
        if 'startup' in company_types and is_startup:
            matches += 1
        if 'tech' in company_types and is_tech:
            matches += 1
        if 'established' in company_types and is_established:
            matches += 1

        # Calculate score
        if not company_types:
            return 0.7  # Neutral if no preferences
        else:
            score = matches / len(company_types)
            return max(0.5, score)  # Minimum 0.5 as company is less important

    def _calculate_description_score(self, description: str) -> float:
        """Calculate match score for job description"""
        keywords = self.preferences.get('keywords', [])
        excluded_terms = self.preferences.get('excluded_terms', [])

        description_lower = description.lower()

        # Check for excluded terms - if any found, score is lower
        exclusion_matches = sum(1 for term in excluded_terms if term.lower() in description_lower)

        # Check for keyword matches
        keyword_matches = sum(1 for keyword in keywords if keyword.lower() in description_lower)

        # Calculate keyword score
        if not keywords:
            keyword_score = 0.7  # Neutral if no keywords specified
        else:
            keyword_score = min(1.0, keyword_matches / len(keywords))

        # Calculate exclusion penalty
        if excluded_terms:
            exclusion_penalty = exclusion_matches / len(excluded_terms)
        else:
            exclusion_penalty = 0

        # Final score with penalty
        score = keyword_score * (1 - exclusion_penalty * 0.8)
        return max(0.1, score)  # Minimum score of 0.1

    def _store_match_results(self, processed_job_id: int, scores: Dict[str, float]) -> None:
        """Store job match results in database"""
        if not processed_job_id:
            self.log.warning("Cannot store match results: processed job ID is missing")
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Insert into matched_job_postings table
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