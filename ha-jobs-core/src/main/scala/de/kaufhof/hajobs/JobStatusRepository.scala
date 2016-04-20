package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder._
import de.kaufhof.hajobs.utils.CassandraUtils
import CassandraUtils._
import com.datastax.driver.core._
import de.kaufhof.hajobs.JobState._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory.getLogger

import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

object JobStatusRepository {

  /**
    * Returns a default limit of 10 for any JobType.
    *
    * @return the maximum number of job statuses that are returned.
    */
  val defaultLimitByJobType: JobType => Int = _ => 10
}

/**
 * Repository to manage job status in the database.
 * Write and Read Methods can be called with Consitency Level LOCAL_QUORUM
 * to get consistent Job data from Cassandra. This is necessary to prevent
 * the JobSupervisor from setting Finished Jobs to Dead Jobs
 */
class JobStatusRepository(session: Session,
                          ttl: FiniteDuration = 14.days,
                          jobTypes: JobTypes) {

  private val logger = getLogger(getClass)

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
    stmt.setConsistencyLevel(LOCAL_QUORUM)
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
    stmt.setConsistencyLevel(LOCAL_QUORUM)
    stmt
  }

  def save(jobStatus: JobStatus)(implicit ec: ExecutionContext): Future[JobStatus] = for {
    _ <- session.executeAsync(insertDataQuery(jobStatus))
    _ <- session.executeAsync(insertMetaQuery(jobStatus))
  } yield jobStatus

  def updateJobState(jobStatus: JobStatus, newState: JobState)(implicit ec: ExecutionContext): Future[JobStatus] = {
    save(jobStatus.copy(jobState = newState, jobResult = JobStatus.stateToResult(newState), jobStatusTs = DateTime.now()))
  }

  /**
   * Finds the latest job status entries for all jobs.
   * Every JobState is loaded with a single select statement
   * It is recommened to use partition keys if possible and avoid
   * IN statement in WHERE clauses, therefore we prefer to execute
   * more than one select statement
   */
  def getLatestMetadata(readwithQuorum: Boolean = false)(implicit ec: ExecutionContext): Future[List[JobStatus]] = {

    def getLatestMetadata(jobType: JobType): Future[Option[JobStatus]] = {
      val selectMetadata = select().all().from(MetaTable).where(QueryBuilder.eq(JobTypeColumn, jobType.name)).limit(1)
      if (readwithQuorum) {
        // setConsistencyLevel returns "this", we do not need to reassign
        selectMetadata.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      }
      session.executeAsync(selectMetadata).map(res =>
        Option(res.one).flatMap(result => rowToStatus(result, isMeta = true))
      )
    }

    Future.traverse(jobTypes.all.toList){ getLatestMetadata }.map(_.flatten)
  }

  /**
   * Finds all job status entries for all jobs.
   * Every JobState is loaded with a single select statement
   * It is recommened to use partition keys if possible and avoid
   * IN statement in WHERE clauses, therefore we prefer to execute
   * more than one select statement
   */
  def getMetadata(readwithQuorum: Boolean = false,
                  limitByJobType: JobType => Int = JobStatusRepository.defaultLimitByJobType)
                 (implicit ec: ExecutionContext): Future[Map[JobType, List[JobStatus]]] = {
    def getAllMetadata(jobType: JobType): Future[(JobType, List[JobStatus])] = {
      import scala.collection.JavaConversions._
      val queryLimit = Option(limitByJobType(jobType)).filter(_ > 0).getOrElse(JobStatusRepository.defaultLimitByJobType(jobType))
      val selectMetadata = select().all().from(MetaTable).where(QueryBuilder.eq(JobTypeColumn, jobType.name)).limit(queryLimit)
      if (readwithQuorum) {
        // setConsistencyLevel returns "this", we do not need to reassign
        selectMetadata.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      }
      session.executeAsync(selectMetadata).map(res => {
        val jobStatusList: List[JobStatus] = res.all.toList.flatMap(row => rowToStatus(row, isMeta = true))
        jobType -> jobStatusList
      }
      )
    }

    Future.traverse(jobTypes.all.toList){getAllMetadata}.map(_.toMap)
  }

  /**
   * Returns the history of saved JobStatus for a single job (each save/update for a job
   * is a separate entry).
   */
  def getJobHistory(jobType: JobType, jobId: UUID, withQuorum: Boolean = false)
                   (implicit ec: ExecutionContext): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._

    val selectStmt = select().all()
      .from(DataTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .and(QueryBuilder.eq(JobIdColumn, jobId))

    if (withQuorum) {
      selectStmt.setConsistencyLevel(LOCAL_QUORUM)
    }

    session.executeAsync(selectStmt).map(res =>
      res.all.toList.flatMap( row =>
        rowToStatus(row, isMeta = false)
      ))
  }

  def list(jobType: JobType, limit: Int = 20, withQuorum: Boolean = false)
          (implicit ec: ExecutionContext): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._
    val selectStmt = select().all()
      .from(MetaTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .limit(limit)

    if (withQuorum) {
      selectStmt.setConsistencyLevel(LOCAL_QUORUM)
    }

    session.executeAsync(selectStmt).map(res =>
      res.all().toList.flatMap( result =>
        rowToStatus(result, isMeta = true)
      ))
  }

  def get(jobType: JobType, jobId: UUID, withQuorum: Boolean = false)
         (implicit ec: ExecutionContext): Future[Option[JobStatus]] = {
    val selectStmt = select().all()
      .from(DataTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .and(QueryBuilder.eq(JobIdColumn, jobId))
      .limit(1)

    if (withQuorum) {
      selectStmt.setConsistencyLevel(LOCAL_QUORUM)
    }


    session.executeAsync(selectStmt).map(res =>
      Option(res.one).flatMap( result =>
        rowToStatus(result, isMeta = false)
      ))
  }

  private def rowToStatus(row: Row, isMeta: Boolean): Option[JobStatus] = {

    def table = if(isMeta) "job_status_meta" else "job_status_data"

    val jobTypeName = row.getString(JobTypeColumn)
    val jobId = row.getUUID(JobIdColumn)
    try {
      jobTypes(jobTypeName) match {
        case Some(jobType) =>
          Some(JobStatus(
            row.getUUID(TriggerIdColumn),
            jobType,
            jobId,
            JobState.withName(row.getString(JobStateColumn)),
            JobResult.withName(row.getString(JobResultColumn)),
            new DateTime(row.getTimestamp(TimestampColumn)),
            if (!isMeta) {
              readContent(row)
            } else {
              None
            }
          ))
        case None => logger.error(s"Could not find JobType for name: $jobTypeName (table $table)")
          None
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Could not map $table row to JobStatus object for $jobTypeName job $jobId.", e)
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

  def clear()(implicit ec: ExecutionContext): Future[Unit] = {
    for(
      meta <- session.executeAsync(truncate(MetaTable));
      data <- session.executeAsync(truncate(DataTable))
    ) yield ()
  }
}
