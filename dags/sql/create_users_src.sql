CREATE TABLE IF NOT EXISTS stg_users(
        user_id INTEGER PRIMARY KEY not null,
        name VARCHAR(200) not null,
        email VARCHAR(200) not null,
        city VARCHAR(200) not null,
        state VARCHAR(200) not null,
        zip_code INTEGER not null
       
        
    );