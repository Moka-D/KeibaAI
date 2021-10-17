CREATE TABLE result (
    race_id INT NOT NULL FOREIGN KEY,
    horse_no SMALLINT NOT NULL,
    frame_no SMALLINT,
    horse_id INT FOREIGN KEY,
    arriving_order SMALLINT,
    popular_order SMALLINT,
    win_odds FLOAT,
    jockey_id INT FOREIGN KEY,
    impost FLOAT,
    horse_sex VARCHAR(1),
    horse_age SMALLINT,
    horse_weight SMALLINT,
    horse_weight_change SMALLINT,
    PRIMARY KEY(race_id, horse_no)
);