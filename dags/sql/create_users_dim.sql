CREATE TABLE IF NOT EXISTS dim_user(
        user_id INTEGER PRIMARY KEY NOT NULL,
        name VARCHAR(200) NOT NULL,
        email VARCHAR(200) NOT NULL,
        location VARCHAR(200) NOT NULL,
        state VARCHAR(200) NOT NULL,
        zip_code INTEGER NOT NULL
        
    );