package de.kaufhof.hajobs

import JobState.JobState
import org.joda.time.DateTime
import play.api.libs.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

/**
 * Supports shortcut to store the job status, can be mixed into Jobs.
 */
trait WriteStatus {

  def jobStatusRepository: JobStatusRepository
  def jobType: JobType

  protected def writeStatus(jobState: JobState, content: Option[JsValue] = None)
                           (implicit jobContext: JobContext, ec: ExecutionContext): Future[JobStatus] = {
    val status = JobStatus(jobContext.triggerId, jobType, jobContext.jobId, jobState, JobStatus.stateToResult(jobState), DateTime.now(), content)
    jobStatusRepository.save(status)
  }

}
