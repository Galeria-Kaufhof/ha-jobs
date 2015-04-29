package de.kaufhof.hajobs

import java.util.UUID

import de.kaufhof.hajobs.JobResult.JobResult
import de.kaufhof.hajobs.JobState.JobState
import de.kaufhof.hajobs.utils.EnumJsonSupport
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json.{JsValue, _}

import scala.language.implicitConversions
import scala.util.control.NonFatal

/**
 * Represents Status of Import Jobs
 */
case class JobStatus(triggerId: UUID, jobType: JobType, jobId: UUID, jobState: JobState, jobResult: JobResult, jobStatusTs: DateTime,
                     content: Option[JsValue] = None)

object JobState extends Enumeration {
  type JobState = Value

  // remember to add new values to stateResultMapping as well
  val Running = Value("RUNNING")
  val Preparing = Value("PREPARING")
  val Finished = Value("FINISHED")
  val Failed = Value("FAILED")
  val Canceled = Value("CANCELED")
  val Dead = Value("DEAD")

  implicit val enumRead: Reads[JobState] = EnumJsonSupport.enumReads(JobState)
  implicit val enumWrite: Writes[JobState] = EnumJsonSupport.enumWrites

}

object JobResult extends Enumeration {
  type JobResult = Value
  val Pending = Value("PENDING")
  val Success = Value("SUCCESS")
  val Failed = Value("FAILED")

  implicit val enumRead: Reads[JobResult] = EnumJsonSupport.enumReads(JobResult)
  implicit val enumWrite: Writes[JobResult] = EnumJsonSupport.enumWrites
}

/**
 * The job type, identified by its name, specifies a LockType.
 *
 * JobTypes do not override <code>toString</code> so that there can more useful log output when
 * a jobType is just printed. When storing a reference to a JobType e.g. in C*, the name property
 * must be used instead of toString (like it's done for Enumerations).
 */
case class JobType(name: String, lockType: LockType)

object JobType {

  implicit def jobTypeReads(implicit jobTypes: JobTypes): Reads[JobType] = new Reads[JobType] {
    def reads(json: JsValue): JsResult[JobType] = json match {
      case JsString(s) => jobTypes(s).map(JsSuccess(_)).getOrElse(JsError(s"No JobType found with name '$s'"))
      case _ => JsError("String value expected")
    }
  }

  implicit val jobTypeWrites: Writes[JobType] = new Writes[JobType] {
    def writes(v: JobType): JsValue = JsString(v.name)
  }

}

class JobTypes(jobTypeList: List[JobType]) {

  /**
   * Resolves a JobType by name. Compares built in JobType and given JobTypes.
   */
  def apply(name: String): Option[JobType] = {
    (jobTypeList :+ JobTypes.JobSupervisor).find(_.name == name)
  }

  def all: List[JobType] = jobTypeList

}

object JobTypes {

  object JobSupervisor extends JobType("supervisor", lockType = LockTypes.JobSupervisorLock)

  def apply(jobTypes: JobType*): JobTypes = new JobTypes(jobTypes.toList)

}

object JobStatus {

  /**
   * Override the default DateTime json Format (just prints the unix timestamp)
   * with a one that uses a more readable form (ISO8601).
   * The Reads also supports the former timestamp (millis) based format, so that
   * old values read from the storage don't fail the JobStatus Reads.
   */
  private implicit val iso8601DateTimeFormat = new Format[DateTime] {
    override def writes(o: DateTime): JsValue = JsString(o.toString(ISODateTimeFormat.dateTime()))
    override def reads(json: JsValue): JsResult[DateTime] = json match {
      case JsString(value) =>
        try { JsSuccess(ISODateTimeFormat.dateTime().parseDateTime(json.as[String])) }
        catch { case NonFatal(e) => JsError(s"Could not parse jobStatusTs: $e") }
      case JsNumber(value) => JsSuccess(new DateTime(value.toLong))
      case default =>
        JsError(s"Unexpected value for jobStatusTs: $json")
    }
  }

  implicit def jobStatusReads(implicit jobTypes: JobTypes): Reads[JobStatus] = Json.reads[JobStatus]

  implicit val jobStatusWrites = Json.writes[JobStatus]

  private val stateResultMapping = Map[JobState, JobResult](
    JobState.Running -> JobResult.Pending,
    JobState.Preparing -> JobResult.Pending,
    JobState.Finished -> JobResult.Success,
    JobState.Failed -> JobResult.Failed,
    JobState.Canceled -> JobResult.Failed,
    JobState.Dead -> JobResult.Failed
  )

  def stateToResult(state: JobState): JobResult = stateResultMapping(state)
}