CREATE TABLE events (
   id serial PRIMARY KEY,
   event_key text NOT NULL,
   payload jsonb NOT NULL
);
