package de.kaufhof.hajobs

import java.util.{NoSuchElementException, UUID}

import de.kaufhof.hajobs.utils.EnumJsonSupport
import JobResult.JobResult
import JobState.JobState
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.Logger
import play.api.libs.json.{JsValue, _}

import scala.util.Try
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
sealed trait JobType {
  def name: String
  def lockType: LockType
}

object JobType {

  private val logger = Logger(classOf[JobType])

  abstract class ConfiguredJobType(override val name: String, override val lockType: LockType) extends JobType
  case object JobSupervisor extends ConfiguredJobType("supervisor", LockType.JobSupervisor)
  case object ProductImporter extends ConfiguredJobType("products", LockType.Product)
  case object StoererImporter extends ConfiguredJobType("stoerer", LockType.Stoerer)
  case object KpiImporter extends ConfiguredJobType("kpi", LockType.Kpi)
  case object DictionaryImporter extends ConfiguredJobType("dictionary", LockType.Dictionary)
  case object StockFeedImporter extends ConfiguredJobType("stockFeed", LockType.Stock)
  case object StockSnapshotImporter extends ConfiguredJobType("stockSnapshot", LockType.Stock)

  /** The values of this enumeration as a set.
   */
  val values: Seq[JobType] = Seq(JobSupervisor, ProductImporter, StoererImporter, KpiImporter, DictionaryImporter,
    StockFeedImporter, StockSnapshotImporter)

  /**
   * Resolves a JobType by name. Throws a NoSuchElementException if there's no JobType
   * with the given name. In this case exception is preferred over returning an Option to be
   * more conformant with the other JobStatus enums.
   */
  @throws[NoSuchElementException]
  def withName(name: String): JobType = name match {
    case JobSupervisor.name => JobSupervisor
    case ProductImporter.name => ProductImporter
    case StoererImporter.name => StoererImporter
    case KpiImporter.name => KpiImporter
    case DictionaryImporter.name => DictionaryImporter
    case StockFeedImporter.name => StockFeedImporter
    case StockSnapshotImporter.name => StockSnapshotImporter
    case _ =>
      logger.warn(s"Could not find JobType with name '$name' (known names: ${values.map(_.name).mkString(",")}).")
      throw new NoSuchElementException(s"Could not find JobType with name '$name'.")
  }

  implicit val jobTypeReads = new Reads[JobType] {
    def reads(json: JsValue): JsResult[JobType] = json match {
      case JsString(s) => Try(withName(s)).map(JsSuccess(_)).getOrElse(JsError(s"No JobType found with name '$s'"))
      case _ => JsError("String value expected")
    }
  }

  implicit def enumWrites: Writes[JobType] = new Writes[JobType] {
    def writes(v: JobType): JsValue = JsString(v.name)
  }
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

  implicit val jobStatusFormat = Json.format[JobStatus]

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