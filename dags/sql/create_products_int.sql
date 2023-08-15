CREATE TABLE IF NOT EXISTS int_products(
        product_id INTEGER PRIMARY KEY not null,
        product_name VARCHAR(200),
        product_description VARCHAR(200) ,
        price FLOAT ,
        discounted_price FLOAT
        
    );