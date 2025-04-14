-- Job Listings Table
CREATE TABLE job_listings (
    id SERIAL PRIMARY KEY,
    external_id TEXT,
    title TEXT,
    description TEXT,
    source TEXT DEFAULT 'linkedin',
    url TEXT,

    -- Job Attributes
    posted_date DATE,
    job_type TEXT,
    employment_type TEXT,
    remote BOOLEAN DEFAULT FALSE,

    -- Location Details
    location TEXT,
    city TEXT,
    country TEXT,

    -- Salary Information
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_currency TEXT,
    salary_period TEXT,

    -- Scoring Metrics
    match_score NUMERIC DEFAULT 0,
    skill_score NUMERIC DEFAULT 0,
    location_score NUMERIC DEFAULT 0,
    salary_score NUMERIC DEFAULT 0,
    company_score NUMERIC DEFAULT 0,
    description_score NUMERIC DEFAULT 0,

    -- Company Details
    company TEXT,
    company_description TEXT,
    company_industry TEXT,
    company_size TEXT,

    -- Additional Details
    category TEXT,
    category_group TEXT,
    client_jobs_posted INTEGER,
    client_rating NUMERIC,
    engagement_duration TEXT,
    weekly_hours TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster searching
CREATE INDEX idx_job_listings_source ON job_listings(source);
CREATE INDEX idx_job_listings_title ON job_listings(title);
CREATE INDEX idx_job_listings_location ON job_listings(location);
CREATE INDEX idx_job_listings_match_score ON job_listings(match_score);

-- Skills Table
CREATE TABLE skills (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Job Skills Junction Table (Many-to-Many Relationship)
CREATE TABLE job_skills (
    job_id INTEGER REFERENCES job_listings(id) ON DELETE CASCADE,
    skill_id INTEGER REFERENCES skills(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);

-- Optional: Trigger to update 'updated_at' timestamp
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