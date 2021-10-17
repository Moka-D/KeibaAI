CREATE TABLE horse (
    horse_id INT UNIQUE NOT NULL PRIMARY KEY,
    horse_name VARCHAR(9) UNIQUE,
    horse_owner VARCHAR(20),
    trainer VARCHAR(15)
);