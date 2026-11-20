CREATE TABLE users (
   id serial PRIMARY KEY,
   name text NOT NULL,
   email text,
   password text
);

CREATE TABLE orders (
   id serial PRIMARY KEY,
   user_id int NOT NULL,
   status text NOT NULL,
   amount numeric(10, 2)
);
