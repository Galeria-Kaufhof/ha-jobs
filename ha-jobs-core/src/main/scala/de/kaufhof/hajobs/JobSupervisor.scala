package de.kaufhof.hajobs

import java.util.UUID

import org.slf4j.LoggerFactory._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
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

    logger.info("Starting dead job detection...")

    override val result: Future[Unit] = jobUpdater.updateJobs().flatMap { case jobs =>
      if (jobs.isEmpty) {
          logger.info("Finished dead job detection, no dead jobs found.")
        } else {
          logger.info("Dead job detection finished, changed jobs state to DEAD for {} jobs.", jobs.length)
        }

      if (!isCancelled) {
        retriggerJobs().andThen {
          case Success(retriggeredJobStatus) =>
            if (retriggeredJobStatus.isEmpty) {
              logger.info("Finished retriggering jobs, no jobs to retrigger found.")
            } else {
              logger.info("Retriggering jobs finished, retriggered {} jobs.", retriggeredJobStatus.length)
            }
          case Failure(e) =>
            logger.error("Error during retriggering failed jobs.", e)
        }.map(_ => ())
      } else {
        Future.successful(())
        }
    }.recover {
      case NonFatal(e) =>
        logger.error("Error in JobSupervisor during job updating.", e)
        throw e
    }

    override def cancel(): Unit = isCancelled = true
  }


  /**
   * Retrigger all failed jobs. A job is considered failed if for the latest trigger id there is no succeded job
   * and no job is running. Failed job can only be retriggered a limited number of times.
    *
    * @return
   */
  private[hajobs] def retriggerJobs(): Future[Seq[JobStartStatus]] = {
    def retriggerCount: JobType => Int = jobType => jobManager.retriggerCounts.getOrElse(jobType, 10)
    def triggerIds(metadataList: List[(JobType, List[JobStatus])]): List[(JobType, UUID)] = for {
      (jobType, jobStatusList) <- metadataList
      triggerId <- triggerIdToRetrigger(jobType, jobStatusList)
    } yield jobType -> triggerId

    for {
      metadataList <- jobStatusRepository.getMetadata(limitByJobType = retriggerCount).map(_.toList)
      triggerIdList = triggerIds(metadataList)
      jobStartStatusList <- Future.traverse(triggerIdList) { case (jobTypeToRetrigger, triggerId) =>
        jobManager.retriggerJob(jobTypeToRetrigger, triggerId)
      }
    } yield jobStartStatusList
  }

  private def triggerIdToRetrigger(jobType: JobType, jobStatusList: List[JobStatus]): Option[UUID] = {
    val job = jobManager.getJob(jobType)
    val maybeLatestTriggerId = jobStatusList.sortBy(_.jobStatusTs.getMillis).lastOption.map(_.triggerId)

    maybeLatestTriggerId flatMap { latestTriggerId =>
        val jobsOfLatestTrigger = jobStatusList.filter(_.triggerId == latestTriggerId)

        // we don't need to restart a job that already succeeded
        // or thats pending, cause we don't know the reuslt of that job
        val someJobIsRunningOrPending = jobsOfLatestTrigger.exists(js => js.jobResult == JobResult.Pending
          || js.jobResult == JobResult.Success)

        if (!someJobIsRunningOrPending && jobsOfLatestTrigger.size <= job.retriggerCount) {
          maybeLatestTriggerId
        } else {
          None
        }
    }

  }
}
