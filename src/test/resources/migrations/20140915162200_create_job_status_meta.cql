-- description: create job status meta
-- authoredAt: 20140915162200
-- up:

CREATE TABLE job_status_meta (
  job_type text,
  job_id timeuuid,
  trigger_id timeuuid,
  job_status_ts timestamp,
  job_state text,
  job_result text,
  PRIMARY KEY (job_type, job_id)
) WITH CLUSTERING ORDER BY (job_id DESC);

-- down:

drop table job_status_meta;
