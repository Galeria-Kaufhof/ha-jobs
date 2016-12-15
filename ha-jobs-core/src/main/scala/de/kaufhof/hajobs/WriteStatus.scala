package de.kaufhof.hajobs

import JobState.JobState
import org.joda.time.DateTime
import play.api.libs.json.{JsValue, Json, Writes}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Supports shortcut to store the job status, can be mixed into Jobs.
 */
trait WriteStatus {

  def jobStatusRepository: StatusWriter

  def writeStatus(jobState: JobState, content: Option[JsValue] = None)
                           (implicit jobContext: JobContext, ec: ExecutionContext): Future[JobStatus] = {
    val status = JobStatus(jobContext.triggerId, jobContext.jobType, jobContext.jobId,
      jobState, JobStatus.stateToResult(jobState), DateTime.now(), content)
    jobStatusRepository.save(status)
  }

  def writeStatus(jobState: JobState, content: JsValue)
                           (implicit jobContext: JobContext, ec: ExecutionContext): Future[JobStatus] = {
    writeStatus(jobState, Some(content))
  }

  /**
    * Converts the given content to json and writes it as content of JobStatus to the StatusWriter.
    */
  def writeStatusAsJson[T](jobState: JobState, content: T)
                       (implicit writes: Writes[T], jobContext: JobContext, ec: ExecutionContext): Future[JobStatus] = {
    writeStatus(jobState, Json.toJson(content))
  }

}
