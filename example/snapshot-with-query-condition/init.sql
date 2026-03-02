CREATE TABLE users (
   id serial PRIMARY KEY,
   name text NOT NULL,
   status text NOT NULL DEFAULT 'active',
   deleted_at timestamptz,
   created_on timestamptz DEFAULT now()
);

CREATE TABLE orders (
   id serial PRIMARY KEY,
   user_id int NOT NULL,
   status text NOT NULL DEFAULT 'pending',
   created_on timestamptz DEFAULT now()
);

INSERT INTO users (name, status, deleted_at)
SELECT
    'User' || i,
    CASE WHEN i % 3 = 0 THEN 'inactive' ELSE 'active' END,
    CASE WHEN i % 5 = 0 THEN now() ELSE NULL END
FROM generate_series(1, 100) AS i;

-- Update some users to have status = 'active' but also deleted_at set
-- This helps demonstrate the combined condition: status = 'active' AND deleted_at IS NULL
UPDATE users SET status = 'active' WHERE id % 7 = 0;

INSERT INTO orders (user_id, status)
SELECT
    (i % 10) + 1,
    CASE 
        WHEN i % 3 = 0 THEN 'cancelled'
        WHEN i % 3 = 1 THEN 'completed'
        ELSE 'pending'
    END
FROM generate_series(1, 200) AS i;

