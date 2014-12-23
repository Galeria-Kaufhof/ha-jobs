package de.kaufhof.hajobs

import java.util.UUID

import de.kaufhof.hajobs.JobState.JobState
import org.joda.time.DateTime
import play.api.libs.json.JsValue

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


/**
 * JobStartStatus classes are used to communicate the result of a job start request.
 */
abstract class JobStartStatus

case class Started(jobId: UUID, details: Option[JsValue] = None) extends JobStartStatus

case class LockedStatus(runningId: Option[UUID] = None) extends JobStartStatus

case class Error(details: String) extends JobStartStatus

case class JobContext(jobId: UUID, triggerId: UUID, finishCallback: () => Unit)


/**
 * Base class for jobs, wraps job execution and status.
 */
abstract class Job(val jobType: JobType,
                   jobStatusRepository: JobStatusRepository,
                   val retriggerCount: Int,
                   val cronExpression: Option[String] = None,
                   val lockTimeout: FiniteDuration = 60 seconds) {

  /**
   * Starts a new job. The returned future should be completed once the job was started so that
   * we know it's running.
   *
   * This method should only be called by job manager. That one is responsibly for lock management and
   * creation of JobContext.
   *
   * ATTENTION: For job manager to work probably it needs to be informed when job execution finished. For that
   * a method finishCallback is provided in JobContext. That needs to be called when job execution is finished.
   * Otherwise the job will fall in undefined state.
   */
  def run()(implicit context: JobContext): Future[JobStartStatus]

  /**
   * Cancels a running job, jobs need to check the [isCancelled] property accordingly.
   * For now, there is no return value provided to identify if cancelation was successful.
   */
  def cancel()

  def allJobStatus: Future[List[JobStatus]] = jobStatusRepository.list(jobType)

  def jobStatus(jobId: UUID): Future[Option[JobStatus]] = jobStatusRepository.get(jobType, jobId)

  protected def writeStatus(jobState: JobState, content: Option[JsValue] = None)(implicit jobContext: JobContext): Future[JobStatus] = {
    val status = JobStatus(jobContext.triggerId, jobType, jobContext.jobId, jobState, JobStatus.stateToResult(jobState), DateTime.now(), content)
    jobStatusRepository.save(status)
  }
}
