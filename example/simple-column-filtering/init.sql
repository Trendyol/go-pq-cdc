CREATE TABLE users (
   id serial PRIMARY KEY,
   name text NOT NULL,
   secret_id text NOT NULL,
   created_on timestamptz
);
