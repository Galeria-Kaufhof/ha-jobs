package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel.QUORUM
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder._
import de.kaufhof.hajobs.utils.CassandraUtils
import CassandraUtils._
import com.datastax.driver.core._
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs.utils.CassandraUtils
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
 * Repository to manage job status in the database.
 * Write and Read Methods can be called with Consitency Level Quorum
 * to get consistent Job data from Cassandra. This is necessary to prevent
 * the JobSupervisor from setting Finished Jobs to Dead Jobs
 */
class JobStatusRepository(session: Session, ttl: FiniteDuration = 14.days) {

  private val logger = Logger(getClass)

  private val MetaTable = "job_status_meta"
  private val DataTable = "job_status_data"

  private val JobTypeColumn = "job_type"
  private val JobIdColumn = "job_id"
  private val JobStateColumn = "job_state"
  private val JobResultColumn = "job_result"
  private val TimestampColumn = "job_status_ts"
  private val ContentColumn = "content"
  private val TriggerIdColumn = "trigger_id"

  private def insertMetaQuery(jobStatus: JobStatus) = {
    val stmt = insertInto(MetaTable)
      .value(JobTypeColumn, jobStatus.jobType.name)
      .value(JobIdColumn, jobStatus.jobId)
      .value(TimestampColumn, jobStatus.jobStatusTs.toDate)
      .value(JobStateColumn, jobStatus.jobState.toString)
      .value(JobResultColumn, jobStatus.jobResult.toString)
      .value(TriggerIdColumn, jobStatus.triggerId)
      .using(QueryBuilder.ttl(ttl.toSeconds.toInt))
    stmt.setConsistencyLevel(QUORUM)
    stmt
  }

  private def insertDataQuery(jobStatus: JobStatus) = {
    val stmt = insertInto(DataTable)
      .value(JobTypeColumn, jobStatus.jobType.name)
      .value(JobIdColumn, jobStatus.jobId)
      .value(JobStateColumn, jobStatus.jobState.toString)
      .value(JobResultColumn, jobStatus.jobResult.toString)
      .value(TimestampColumn, jobStatus.jobStatusTs.toDate)
      .value(ContentColumn, jobStatus.content.map(_.toString()).orNull)
      .value(TriggerIdColumn, jobStatus.triggerId)
      .using(QueryBuilder.ttl(ttl.toSeconds.toInt))
    stmt.setConsistencyLevel(QUORUM)
    stmt
  }

  def save(jobStatus: JobStatus): Future[JobStatus] = {
    val batchStmt = batch(insertMetaQuery(jobStatus), insertDataQuery(jobStatus))
    session.executeAsync(batchStmt).map(_ => jobStatus)
  }

  def updateJobState(jobStatus: JobStatus, newState: JobState): Future[JobStatus] = {
    save(jobStatus.copy(jobState = newState, jobResult = JobStatus.stateToResult(newState), jobStatusTs = DateTime.now()))
  }

  /**
   * Finds the latest job status entries for all jobs.
   */
  def getAllMetadata(readWithQuorum: Boolean = false): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._

    val selectStmt = select().all().from(MetaTable)

    if (readWithQuorum) {
      // setConsistencyLevel returns "this", we do not need to reassign
      selectStmt.setConsistencyLevel(ConsistencyLevel.QUORUM)
    }

    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map( res =>
      res.all().toList.flatMap( result =>
        rowToStatus(result, isMeta = true)
      ))
  }

  /**
   * Returns the history of saved JobStatus for a single job (each save/update for a job
   * is a separate entry).
   */
  def getJobHistory(jobType: JobType, jobId: UUID, withQuorum: Boolean = false): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._

    val selectStmt = select().all()
      .from(DataTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .and(QueryBuilder.eq(JobIdColumn, jobId))

    if (withQuorum) {
      selectStmt.setConsistencyLevel(QUORUM)
    }

    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map( res =>
      res.all.toList.flatMap( row =>
        rowToStatus(row, isMeta = false)
      ))
  }

  def list(jobType: JobType, limit: Int = 20, withQuorum: Boolean = false): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._
    val selectStmt = select().all()
      .from(MetaTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .limit(limit)

    if (withQuorum) {
      selectStmt.setConsistencyLevel(QUORUM)
    }

    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map ( res =>
      res.all().toList.flatMap( result =>
        rowToStatus(result, isMeta = true)
      ))
  }

  def get(jobType: JobType, jobId: UUID, withQuorum: Boolean = false): Future[Option[JobStatus]] = {
    val selectStmt = select().all()
      .from(DataTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .and(QueryBuilder.eq(JobIdColumn, jobId))
      .limit(1)

    if (withQuorum) {
      selectStmt.setConsistencyLevel(QUORUM)
    }


    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map( res =>
      Option(res.one).flatMap( result =>
        rowToStatus(result, isMeta = false)
      ))
  }

  private def rowToStatus(row: Row, isMeta: Boolean): Option[JobStatus] = {
    try {
      Some(JobStatus(
        row.getUUID(TriggerIdColumn),
        JobType.withName(row.getString(JobTypeColumn)),
        row.getUUID(JobIdColumn),
        JobState.withName(row.getString(JobStateColumn)),
        JobResult.withName(row.getString(JobResultColumn)),
        new DateTime(row.getDate(TimestampColumn).getTime),
        if (!isMeta) {
          readContent(row)
        } else {
          None
        }
      ))
    } catch {
      case NonFatal(e) =>
        Logger.error("error mapping a JobStatus datarow to JobStatus object", e)
        None
    }
  }

  private def readContent(row: Row): Option[JsValue] = {
    Option(row.getString(ContentColumn)).flatMap(content =>
      try {
        Some(Json.parse(content))
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Could not parse content for ${row.getString(JobTypeColumn)} job ${row.getUUID(JobIdColumn)}: $e (content: $content).")
          None
      }
    )
  }

  def clear(): Future[Unit] = {
    val metaResFuture = session.executeAsync(truncate(MetaTable))
    val dataResFuture = session.executeAsync(truncate(DataTable))
    for(
      meta <- metaResFuture;
      data <- dataResFuture
    ) yield ()
  }
}
