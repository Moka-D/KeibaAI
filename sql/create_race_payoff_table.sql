CREATE TABLE if not exists "race_payoff" (
	"race_id"		TEXT NOT NULL,
	"ticket_type"	TEXT NOT NULL,
	"pattern"		TEXT NOT NULL,
	"payoff"		INTEGER,
	"popularity"	INTEGER,
	FOREIGN KEY("race_id") REFERENCES "race_info"("race_id"),
	PRIMARY KEY("race_id","ticket_type","pattern")
)