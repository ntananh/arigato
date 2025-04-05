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