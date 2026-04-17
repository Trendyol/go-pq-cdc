CREATE TABLE users (
   id serial PRIMARY KEY,
   email text NOT NULL,
   name text NOT NULL
);

CREATE UNIQUE INDEX users_email_unique_idx ON users (email);
