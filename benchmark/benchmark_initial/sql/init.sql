CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

INSERT INTO users (name)
SELECT 'Oyleli' || i
FROM generate_series(1, 10000000) AS i;