CREATE TABLE IF NOT EXISTS dim_product(
        product_id INTEGER PRIMARY KEY NOT NULL,
        product_name VARCHAR(200) NOT NULL,
        product_description VARCHAR(200) NOT NULL,
        price FLOAT NOT NULL,
        discounted_price FLOAT NOT NULL
        
    );