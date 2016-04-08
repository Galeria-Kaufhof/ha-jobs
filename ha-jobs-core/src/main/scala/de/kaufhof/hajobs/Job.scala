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

/**
 * Details regarding a job execution.
 */
case class JobContext(jobType: JobType, jobId: UUID, triggerId: UUID)


/**
 * Base class for jobs, job execution is started via `run()`.
 * For actor jobs you can just use the `ActorJob`.
 */
abstract class Job(val jobType: JobType,
                   val retriggerCount: Int = 1,
                   val cronExpression: Option[String] = None,
                   val lockTimeout: FiniteDuration = 60 seconds) {

  /**
   * Starts a new job and returns a [[de.kaufhof.hajobs.JobExecution JobExecution]].
   * The [[de.kaufhof.hajobs.JobExecution#result JobExecution.result]] future must be
   * completed when the job execution is finished.
   *
   * This method (`run`) should not "block", all the work must be performed
   * by the returned `JobExecution`.
   */
  def run()(implicit context: JobContext): JobExecution

}

/**
 * The actual execution of a certain Job.
 * @param context the job context, is made implicitely available to the JobExecution
 *                (e.g. useful in combination with WriteStatus).
 */
abstract class JobExecution(implicit val context: JobContext) {

  /**
   * The result of this job execution. Once completed, the lock will be released.
   */
  def result: Future[Unit]

  /**
   * Cancels this job execution. Cancellation must complete the [[result]] Future at some point.
   */
  def cancel(): Unit

}