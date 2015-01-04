package de.kaufhof.hajobs

import java.util.UUID

import org.slf4j.LoggerFactory._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}

/**
 * Checks if for jobs in state RUNNING there's a lock as well. If this is not the case,
 * the job is considered to be crashed (because a job must regularly update the corresponding
 * lock).
 */
class JobSupervisor(jobManager: => JobManager,
                    jobUpdater: JobUpdater,
                    jobStatusRepository: JobStatusRepository,
                    cronExpression: Option[String]) extends Job(JobTypes.JobSupervisor, 0, cronExpression) {

  def this(jobManager: => JobManager,
           lockRepository: LockRepository,
           jobStatusRepository: JobStatusRepository,
           cronExpression: Option[String] = None) = this(jobManager, new JobUpdater(lockRepository, jobStatusRepository), jobStatusRepository, cronExpression)

  private val logger = getLogger(getClass)

  /**
   * Starts dead job detection. The start status is returned as soon as we know if we have
   * the lock or not (actual job detection is running in the background after that).
   */
  def run()(implicit jobContext: JobContext): JobExecution = new JobExecution() {

    @volatile
    private var isCancelled = false

    private val jobId = jobContext.jobId
    logger.info("Starting dead job detection...")

    private val promise = Promise[Unit]()
    override val result: Future[Unit] = promise.future

    private val updatedJobs = jobUpdater.updateJobs()

    updatedJobs.onComplete {
      case Success(jobs) =>
        if(jobs.isEmpty) {
          logger.info("Finished dead job detection, no dead jobs found.")
        } else {
          logger.info("Dead job detection finished, changed jobs state to DEAD for {} jobs.", jobs.length)
        }
      case Failure(e) =>
        logger.error("Error during dead job detection.", e)
    }

    updatedJobs.onComplete { res =>
      if (!isCancelled) {
        retriggerJobs().onComplete {
          case Success(retriggeredJobStatus) =>
            if (retriggeredJobStatus.isEmpty) {
              logger.info("Finished retriggering jobs, no jobs to retrigger found.")
            } else {
              logger.info("Retriggering jobs finished, retriggered {} jobs.", retriggeredJobStatus.length)
            }
            promise.success(())
          case Failure(e) =>
            logger.error("Error during dead job detection.", e)
            promise.failure(e)
        }
      } else {
        Future.successful(Nil)
      }
    }

    override def cancel(): Unit = isCancelled = true
  }


  /**
   * Retrigger all failed jobs. A job is considered failed if for the latest trigger id there is no succeded job
   * and no job is running. Failed job can only be retriggered a limited number of times.
   * @return
   */
  private[hajobs] def retriggerJobs(): Future[Seq[JobStartStatus]] = {
    jobStatusRepository.getAllMetadata().flatMap { allJobs =>
      val a = allJobs.groupBy(_.jobType).flatMap { case (jobType, jobStatus) =>
        triggerIdToRetrigger(jobType, jobStatus).map { triggerId =>
          logger.info(s"Retriggering job of type $jobType with triggerid $triggerId")
          jobManager.retriggerJob(jobType, triggerId)
        }
      }.toSeq
      Future.sequence(a)
    }
  }

  private def triggerIdToRetrigger(jobType: JobType, jobStatus: List[JobStatus]): Option[UUID] = {
    val job = jobManager.getJob(jobType)
    val latestTriggerId = jobStatus.sortBy(_.jobStatusTs.getMillis).last.triggerId
    val jobsOfLatestTrigger = jobStatus.filter(_.triggerId == latestTriggerId)

    // we don't need to restart a job that already succeeded
    // or thats pending, cause we don't know the reuslt of that job
    val someJobIsRunningOrPending = jobsOfLatestTrigger.exists(js => js.jobResult == JobResult.Pending
      || js.jobResult == JobResult.Success)

    if (!someJobIsRunningOrPending && jobsOfLatestTrigger.size <= job.retriggerCount) {
      Some(latestTriggerId)
    } else {
      None
    }
  }
}
