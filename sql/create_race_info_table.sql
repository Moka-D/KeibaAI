CREATE TABLE if not exists "race_info" (
	"race_id"		INTEGER NOT NULL UNIQUE,
	"race_title"	TEXT,
	"date"			INTEGER,
	"place_id"		TEXT,
	"hold_no"		INTEGER,
	"hold_day"		INTEGER,
	"race_no"		INTEGER,
	"distance"		INTEGER,
	"race_type"		TEXT,
	"turn"			TEXT,
	"ground"		TEXT,
	"weather"		TEXT,
	PRIMARY KEY("race_id")
)