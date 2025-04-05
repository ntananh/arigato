import os
import json
import yaml
import pickle
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')


class JobMatcherModel:
    """
    ML model for matching jobs with user preferences.

    This model uses TF-IDF and cosine similarity to match job descriptions
    with user preferences.
    """

    def __init__(self, preferences_path: str):
        """
        Initialize the JobMatcherModel.

        Parameters:
        -----------
        preferences_path : str
            Path to the YAML file containing user preferences
        """
        self.preferences = self._load_preferences(preferences_path)
        self.stop_words = set(stopwords.words('english'))
        self.vectorizer = TfidfVectorizer(
            stop_words='english',
            ngram_range=(1, 2),
            max_features=10000
        )
        self.model_path = os.path.join(os.environ.get(
            'AIRFLOW_HOME', ''), 'include', 'ml_models', 'job_matcher_model.pkl')

        if os.path.exists(self.model_path):
            self._load_model()
        else:
            self._initialize_model()

    def _load_preferences(self, path: str) -> Dict[str, Any]:
        """
        Load user preferences from YAML file.

        Parameters:
        -----------
        path : str
            Path to the YAML file

        Returns:
        --------
        Dict[str, Any]
            User preferences
        """
        try:
            with open(path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            print(f"Error loading preferences: {str(e)}")
            return {}

    def _initialize_model(self) -> None:
        """Initialize a new model with user preferences"""
        # Create preference vectors
        must_have_skills = self.preferences.get(
            'skills', {}).get('must_have', [])
        nice_to_have_skills = self.preferences.get(
            'skills', {}).get('nice_to_have', [])
        all_skills = must_have_skills + nice_to_have_skills

        # Create a preference document
        skill_text = ' '.join(all_skills)
        keyword_text = ' '.join(self.preferences.get('keywords', []))
        preference_doc = f"{skill_text} {keyword_text}"

        # Fit vectorizer
        self.vectorizer.fit([preference_doc])

        # Create model data
        self.model_data = {
            'vectorizer': self.vectorizer,
            'preference_vector': self.vectorizer.transform([preference_doc])
        }

        # Save model
        self._save_model()

    def _load_model(self) -> None:
        """Load model from file"""
        try:
            with open(self.model_path, 'rb') as file:
                self.model_data = pickle.load(file)
                self.vectorizer = self.model_data.get('vectorizer')
        except Exception as e:
            print(f"Error loading model: {str(e)}")
            self._initialize_model()

    def _save_model(self) -> None:
        """Save model to file"""
        try:
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            with open(self.model_path, 'wb') as file:
                pickle.dump(self.model_data, file)
        except Exception as e:
            print(f"Error saving model: {str(e)}")

    def match_job(self, job_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Match a job with user preferences.

        Parameters:
        -----------
        job_data : Dict[str, Any]
            Job data to match

        Returns:
        --------
        Dict[str, float]
            Match scores
        """
        # Calculate text similarity score
        description = job_data.get('job_description', '')
        title = job_data.get('job_title', '')
        text_score = self._calculate_text_similarity(f"{title} {description}")

        # Calculate skill match score
        skills = job_data.get('skills', [])
        skill_score = self._calculate_skill_score(skills)

        # Calculate location score
        location = job_data.get('location', '')
        remote = job_data.get('remote', False)
        location_score = self._calculate_location_score(location, remote)

        # Calculate salary score
        salary_min = job_data.get('salary_min', 0)
        salary_max = job_data.get('salary_max', 0)
        salary_currency = job_data.get('salary_currency', '')
        salary_score = self._calculate_salary_score(
            salary_min, salary_max, salary_currency)

        # Calculate company score
        company_name = job_data.get('company_name', '')
        company_score = self._calculate_company_score(company_name)

        # Calculate overall score (weighted average)
        weights = self.preferences.get('scoring', {})
        skill_weight = weights.get('skill_match_weight', 0.4)
        location_weight = weights.get('location_weight', 0.2)
        salary_weight = weights.get('salary_weight', 0.2)
        company_weight = weights.get('company_weight', 0.1)
        description_weight = weights.get('job_description_weight', 0.1)

        overall_score = (
            skill_score * skill_weight +
            location_score * location_weight +
            salary_score * salary_weight +
            company_score * company_weight +
            text_score * description_weight
        )

        return {
            'match_score': overall_score,
            'skill_score': skill_score,
            'location_score': location_score,
            'salary_score': salary_score,
            'company_score': company_score,
            'description_score': text_score
        }

    def _calculate_text_similarity(self, text: str) -> float:
        """
        Calculate text similarity score between job text and preference text.

        Parameters:
        -----------
        text : str
            Job text

        Returns:
        --------
        float
            Similarity score (0-1)
        """
        # Clean text - remove special characters, lowercase
        clean_text = re.sub(r'[^\w\s]', ' ', text.lower())

        # Calculate similarity using TF-IDF and cosine similarity
        job_vector = self.vectorizer.transform([clean_text])
        preference_vector = self.model_data.get('preference_vector')

        # Calculate cosine similarity
        similarity = cosine_similarity(job_vector, preference_vector)[0][0]

        # Calculate keyword matches
        keywords = self.preferences.get('keywords', [])
        excluded_terms = self.preferences.get('excluded_terms', [])

        keyword_matches = sum(1 for keyword in keywords if keyword.lower(
        ) in clean_text) / max(1, len(keywords))
        excluded_matches = sum(1 for term in excluded_terms if term.lower(
        ) in clean_text) / max(1, len(excluded_terms))

        # Adjust similarity score based on keyword and excluded term matches
        adjusted_score = similarity * 0.6 + keyword_matches * 0.4

        # Apply penalty for excluded terms
        final_score = adjusted_score * (1 - excluded_matches * 0.7)

        return max(0, min(final_score, 1.0))

    def _calculate_skill_score(self, job_skills: List[str]) -> float:
        """
        Calculate match score for job skills against preferences.

        Parameters:
        -----------
        job_skills : List[str]
            List of skills from the job

        Returns:
        --------
        float
            Skill match score (0-1)
        """
        must_have_skills = self.preferences.get(
            'skills', {}).get('must_have', [])
        nice_to_have_skills = self.preferences.get(
            'skills', {}).get('nice_to_have', [])

        # Count matches
        must_have_matches = sum(1 for skill in must_have_skills if any(
            skill.lower() in job_skill.lower() for job_skill in job_skills))
        nice_to_have_matches = sum(1 for skill in nice_to_have_skills if any(
            skill.lower() in job_skill.lower() for job_skill in job_skills))

        # Calculate score
        if not must_have_skills:
            must_have_score = 1.0  # If no must-have skills specified, give full score
        else:
            must_have_score = must_have_matches / len(must_have_skills)

        if not nice_to_have_skills:
            nice_to_have_score = 1.0  # If no nice-to-have skills specified, give full score
        else:
            nice_to_have_score = nice_to_have_matches / \
                len(nice_to_have_skills)

        # Weighted combination (must-have skills are more important)
        return 0.7 * must_have_score + 0.3 * nice_to_have_score

    def _calculate_location_score(self, location: str, is_remote: bool) -> float:
        """
        Calculate match score for job location against preferences.

        Parameters:
        -----------
        location : str
            Job location
        is_remote : bool
            Whether the job is remote

        Returns:
        --------
        float
            Location match score (0-1)
        """
        preferred_remote = self.preferences.get(
            'locations', {}).get('remote', False)
        preferred_countries = self.preferences.get(
            'locations', {}).get('countries', [])
        excluded_regions = self.preferences.get(
            'locations', {}).get('exclude_regions', [])

        # If job is remote and user prefers remote, it's a perfect match
        if is_remote and preferred_remote:
            return 1.0

        # Check if job location contains any preferred countries
        location_lower = location.lower()
        country_match = any(
            country.lower() in location_lower for country in preferred_countries)

        # Check if job location contains any excluded regions
        region_exclude = any(
            region.lower() in location_lower for region in excluded_regions)

        if region_exclude:
            return 0.0
        elif country_match:
            return 0.8  # Good match but not perfect (not remote)
        elif is_remote and not preferred_remote:
            return 0.5  # Job is remote but user doesn't prefer remote
        else:
            return 0.2  # No match but not completely excluded

    def _calculate_salary_score(self, min_salary: float, max_salary: float, currency: str) -> float:
        """
        Calculate match score for salary against preferences.

        Parameters:
        -----------
        min_salary : float
            Minimum salary
        max_salary : float
            Maximum salary
        currency : str
            Salary currency

        Returns:
        --------
        float
            Salary match score (0-1)
        """
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
        """
        Calculate match score for company against preferences.

        Parameters:
        -----------
        company_name : str
            Company name

        Returns:
        --------
        float
            Company match score (0-1)
        """
        company_types = self.preferences.get('company_types', [])

        # This would ideally use a company database or API to get company type
        # For now, use simple heuristics based on company name
        company_lower = company_name.lower()

        # Simple heuristic checks
        is_startup = any(term in company_lower for term in [
                         'startup', 'labs', 'technologies', 'tech'])
        is_tech = any(term in company_lower for term in [
                      'tech', 'digital', 'software', 'solutions', 'systems'])
        is_established = any(term in company_lower for term in [
                             'inc', 'corp', 'ltd', 'limited', 'group'])

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
            score = 0.5 + (matches / max(1, len(company_types))) * 0.5
            return score
