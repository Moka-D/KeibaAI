CREATE TABLE if not exists "horse" (
	"id"	TEXT NOT NULL UNIQUE,
	"name"	TEXT NOT NULL,
	"father"	TEXT,
	"mother"	TEXT,
	"fathers_father"	TEXT,
	"fathers_mother"	TEXT,
	"mothers_father"	TEXT,
	"mothers_mother"	TEXT,
	PRIMARY KEY("id")
)