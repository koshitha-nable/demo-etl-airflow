CREATE TABLE IF NOT EXISTS dim_review(
        review_id VARCHAR(100) NOT NULL,
        product_id INTEGER NOT NULL,
        review_score INTEGER NOT NULL,
        review_date DATE NOT NULL,
        review_year INTEGER NOT NULL,
        review_month INTEGER NOT NULL
        
    );