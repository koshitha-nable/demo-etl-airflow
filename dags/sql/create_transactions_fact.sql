CREATE TABLE IF NOT EXISTS fact_transaction(
        purchase_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        total_amount FLOAT NOT NULL

    );