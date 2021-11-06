CREATE TABLE "results" (
	"race_id"	INTEGER NOT NULL,
	"horse_no"	INTEGER NOT NULL,
	"frame_no"	INTEGER,
	"arriving_order"	TEXT,
	"horse_id"	TEXT,
	"sex_age"	TEXT,
	"impost"	REAL,
	"jockey_id"	TEXT,
	"goal_time"	TEXT,
	"margin_length"	TEXT,
	"corner_pass"	TEXT,
	"last_three_furlong"	REAL,
	"win_odds"	REAL,
	"popular_order"	INTEGER,
	"horse_weight"	TEXT,
	"trainer_id"	TEXT,
	"owner_name"	TEXT,
	"prise"	REAL,
	PRIMARY KEY("race_id","horse_no"),
	FOREIGN KEY("horse_id") REFERENCES "horse"("id"),
	FOREIGN KEY("jockey_id") REFERENCES "jockey"("id"),
	FOREIGN KEY("trainer_id") REFERENCES "trainer"("id"),
	FOREIGN KEY("race_id") REFERENCES "race_info"("race_id")
)