-- Enum for job sources
CREATE TYPE job_source AS ENUM ('linkedin', 'upwork');

-- Enum for job types
CREATE TYPE job_type AS ENUM ('full_time', 'part_time', 'contract', 'freelance', 'internship', 'temporary');

-- Main job listings table
CREATE TABLE job_listings (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(255) NOT NULL,
    title VARCHAR(512) NOT NULL,
    description TEXT,
    source job_source NOT NULL,
    url TEXT,
    posted_date DATE,
    application_deadline DATE,

    -- Job characteristics
    job_type job_type,
    employment_type VARCHAR(100),
    remote BOOLEAN DEFAULT FALSE,

    -- Location details
    location VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),

    -- Salary information
    salary_min NUMERIC(12,2),
    salary_max NUMERIC(12,2),
    salary_currency VARCHAR(10),
    salary_period VARCHAR(50),

    -- Matching scores (from recommendation system)
    match_score FLOAT,
    skill_score FLOAT,
    location_score FLOAT,
    salary_score FLOAT,
    company_score FLOAT,
    description_score FLOAT,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Company information table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    industry VARCHAR(255),
    company_size VARCHAR(100),
    website VARCHAR(255),
    linkedin_url VARCHAR(255),

    -- Source-specific identifiers
    linkedin_org_id VARCHAR(255),
    upwork_company_id VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Skills table (many-to-many relationship with job listings)
CREATE TABLE skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

-- Job-Skills junction table
CREATE TABLE job_skills (
    job_id INTEGER REFERENCES job_listings(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES skills(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);

-- Client information for Upwork-specific details
CREATE TABLE upwork_client_details (
    id SERIAL PRIMARY KEY,
    job_id INTEGER UNIQUE REFERENCES job_listings(id) ON DELETE CASCADE,
    client_total_jobs_posted INTEGER,
    client_rating NUMERIC(4,2),
    client_country VARCHAR(100),
    engagement_duration VARCHAR(100),
    weekly_hours VARCHAR(50),
    project_type VARCHAR(100)
);

-- LinkedIn-specific job details
CREATE TABLE linkedin_job_details (
    id SERIAL PRIMARY KEY,
    job_id INTEGER UNIQUE REFERENCES job_listings(id) ON DELETE CASCADE,
    linkedin_job_id VARCHAR(255),
    linkedin_company_id VARCHAR(255),
    applicants_count INTEGER,
    seniority_level VARCHAR(100),
    required_credentials TEXT[]
);

-- Indexes for performance
CREATE INDEX idx_job_listings_title ON job_listings(title);
CREATE INDEX idx_job_listings_location ON job_listings(location);
CREATE INDEX idx_job_listings_posted_date ON job_listings(posted_date);
CREATE INDEX idx_job_listings_match_score ON job_listings(match_score);
CREATE INDEX idx_job_skills_job_id ON job_skills(job_id);
CREATE INDEX idx_job_skills_skill_id ON job_skills(skill_id);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_job_listings_modtime
    BEFORE UPDATE ON job_listings
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_companies_modtime
    BEFORE UPDATE ON companies
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();