-- Bank Reviews Data Warehouse Schema
-- Star Schema design for analyzing bank agency reviews

-- Create dimension tables
CREATE TABLE IF NOT EXISTS dim_bank (
    bank_id SERIAL PRIMARY KEY,
    bank_name VARCHAR(100) NOT NULL,
    bank_code VARCHAR(10),
    headquarters VARCHAR(100),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_branch (
    branch_id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES dim_bank(bank_id),
    branch_name VARCHAR(200) NOT NULL,
    google_place_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    district VARCHAR(100),
    address TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_sentiment (
    sentiment_id SERIAL PRIMARY KEY,
    sentiment_category VARCHAR(20) NOT NULL, -- 'Positive', 'Negative', 'Neutral'
    sentiment_score DECIMAL(4, 3), -- Score from -1 to 1
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create fact table for reviews
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES dim_bank(bank_id),
    branch_id INTEGER NOT NULL REFERENCES dim_branch(branch_id),
    location_id INTEGER NOT NULL REFERENCES dim_location(location_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    sentiment_id INTEGER REFERENCES dim_sentiment(sentiment_id),
    rating DECIMAL(2, 1) NOT NULL,
    review_text TEXT,
    language VARCHAR(10),
    topics JSONB, -- Store extracted topics from NLP
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a view for aggregated branch performance
CREATE OR REPLACE VIEW branch_performance AS
SELECT 
    b.bank_name,
    br.branch_name,
    l.city,
    COUNT(fr.review_id) AS total_reviews,
    AVG(fr.rating) AS avg_rating,
    COUNT(CASE WHEN s.sentiment_category = 'Positive' THEN 1 END) AS positive_reviews,
    COUNT(CASE WHEN s.sentiment_category = 'Negative' THEN 1 END) AS negative_reviews,
    COUNT(CASE WHEN s.sentiment_category = 'Neutral' THEN 1 END) AS neutral_reviews
FROM fact_reviews fr
JOIN dim_bank b ON fr.bank_id = b.bank_id
JOIN dim_branch br ON fr.branch_id = br.branch_id
JOIN dim_location l ON fr.location_id = l.location_id
JOIN dim_sentiment s ON fr.sentiment_id = s.sentiment_id
GROUP BY b.bank_name, br.branch_name, l.city
ORDER BY avg_rating DESC;

-- Create a view for topic analysis
CREATE OR REPLACE VIEW topic_analysis AS
SELECT 
    b.bank_name,
    s.sentiment_category,
    jsonb_object_keys(fr.topics) AS topic,
    COUNT(*) AS topic_count
FROM fact_reviews fr
JOIN dim_bank b ON fr.bank_id = b.bank_id
JOIN dim_sentiment s ON fr.sentiment_id = s.sentiment_id
WHERE fr.topics IS NOT NULL
GROUP BY b.bank_name, s.sentiment_category, jsonb_object_keys(fr.topics)
ORDER BY topic_count DESC;