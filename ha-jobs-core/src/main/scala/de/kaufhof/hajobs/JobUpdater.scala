package de.kaufhof.hajobs

import org.slf4j.LoggerFactory.getLogger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * JobUpdater is responsible finding running/pending jobs that have lost its lock
 * and set them to status failed/dead
 * @param lockRepository see which jobs actually have a lock
 * @param jobStatusRepository find all the jobStatus
 */
class JobUpdater(lockRepository: LockRepository, jobStatusRepository: JobStatusRepository) {

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
      jobs <- jobStatusRepository.getAllMetadata(readwithQuorum = true)

      runningJobs = jobs.filter(_.jobResult == JobResult.Pending)
      deadJobs = runningJobs.filterNot(job => locks.exists(_.jobId == job.jobId))
      updatedJobs <- updateDeadJobState(deadJobs)

    } yield {
      updatedJobs
    }
  }

  private[hajobs] def updateDeadJobState(deadJobs: List[JobStatus]): Future[List[JobStatus]] = {
    if (deadJobs.isEmpty) {
      Future.successful(List.empty)
    } else {
      logger.info("Detected dead jobs, changing state to DEAD for: {}", deadJobs.map(_.jobId).mkString(","))
      val updateResults = deadJobs.map(job =>
        jobStatusRepository.updateJobState(job, JobState.Dead)
      )
      Future.sequence(updateResults)
    }
  }

}
