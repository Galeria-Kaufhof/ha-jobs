-- description: create lock
-- authoredAt: 20140924114900
-- up:

CREATE TABLE lock (
  lock_type text PRIMARY KEY,
  job_lock timeuuid
);

-- down:

DROP TABLE lock;