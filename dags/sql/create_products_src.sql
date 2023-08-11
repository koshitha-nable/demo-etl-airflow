CREATE TABLE IF NOT EXISTS stg_products(
        product_id INTEGER PRIMARY KEY not null,
        product_name VARCHAR(200) not null,
        email VARCHAR(200) not null,
        product_description VARCHAR(200) not null,
        price FLOAT not null
        
    );