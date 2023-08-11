CREATE TABLE IF NOT EXISTS stg_transactions(
        purchase_id INTEGER PRIMARY KEY not null,
        product_id INTEGER not null,
        user_id INTEGER not null,
        quantity INTEGER not null
        
    );