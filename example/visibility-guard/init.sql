CREATE TABLE orders (
   id serial PRIMARY KEY,
   note text NOT NULL,
   created_at timestamptz DEFAULT now()
);
