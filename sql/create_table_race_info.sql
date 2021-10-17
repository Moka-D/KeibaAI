CREATE TABLE race_info (
    race_id INT NOT NULL PRIMARY KEY,
    race_name VARCHAR(30),
    event_date DATE,
    place_id SMALLINT FOREIGN KEY,
    hold_no SMALLINT,
    hold_day SMALLINT,
    race_no SMALLINT,
    race_grade SMALLINT FOREIGN KEY,
    distance SMALLINT,
    race_type VARCHAR(2),
    turn VARCHAR(1),
    ground VARCHAR(2),
    weather VARCHAR(2)
);