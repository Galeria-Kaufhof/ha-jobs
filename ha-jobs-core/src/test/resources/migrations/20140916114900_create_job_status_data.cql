-- description: create job status data
-- authoredAt: 20140916114900
-- up:

CREATE TABLE job_status_data (
  job_type text,
  job_id timeuuid,
  trigger_id timeuuid,
  job_status_ts timestamp,
  job_state text,
  job_result text,
  content text,
  PRIMARY KEY ((job_type, job_id), job_status_ts, job_state, job_result)
) WITH CLUSTERING ORDER BY (job_status_ts DESC);

-- down:

drop table job_status_data;
