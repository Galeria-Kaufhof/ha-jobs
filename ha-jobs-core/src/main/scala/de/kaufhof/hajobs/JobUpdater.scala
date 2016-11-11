package de.kaufhof.hajobs

import org.slf4j.LoggerFactory.getLogger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * JobUpdater is responsible finding running/pending jobs that have lost its lock
 * and set them to status failed/dead
 * JobUpdater tries to get latest JobStatusData to update status, so one can
 * see the latest content of the dead job.
 * @param lockRepository see which jobs actually have a lock
 * @param jobStatusRepository find all the jobStatus
 */
class JobUpdater(lockRepository: LockRepository,
                 jobStatusRepository: JobStatusRepository,
                 limitByJobType: JobType => Int = JobStatusRepository.defaultLimitByJobType) {

  private val logger = getLogger(getClass)

  def updateJobs(): Future[List[JobStatus]] = {

    for {
      // we *really* want sequential execution here: first read the locks,
      // and only after that is finished, read the jobs status (to ensure
      // consistency between job state and lock state). So please do not try to
      // optimize by moving this code out of the for comprehension.
      // we also need to read with quorom to ensure we get the most current
      // (and consistent) data
      locks <- lockRepository.getAll()
      jobs <- jobStatusRepository.getMetadata(readwithQuorum = true, limitByJobType = limitByJobType)

      runningJobs = jobs.flatMap(_._2).toList.filter(_.jobResult == JobResult.Pending)
      deadJobs = runningJobs.filterNot(job => locks.exists(_.jobId == job.jobId))
      updatedJobs <- updateDeadJobState(deadJobs)

    } yield {
      updatedJobs
    }
  }

  private[hajobs] def updateDeadJobState(deadJobs: List[JobStatus]): Future[List[JobStatus]] = {
    Future.traverse(deadJobs) { jobMeta =>
      logger.info("Detected dead job, changing state from {} to DEAD for: {} ({})", jobMeta.jobState, jobMeta.jobId, jobMeta.jobType)
      jobStatusRepository.get(jobMeta.jobType, jobMeta.jobId).flatMap {
          case Some(data) => jobStatusRepository.updateJobState(data, JobState.Dead)
          // if no latest JobStatusData is found update JobStatusMeta instead
          case None => jobStatusRepository.updateJobState(jobMeta, JobState.Dead)
        }
      }
    }

}
