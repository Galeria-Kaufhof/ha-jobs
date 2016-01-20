package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel.{LOCAL_QUORUM, LOCAL_SERIAL}
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder._
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder}
import de.kaufhof.hajobs.utils.CassandraUtils._
import org.slf4j.LoggerFactory._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/**
 * This repository manages locks for jobs syncronization in distributed environments.
 * To work every kind of job needs an unique identifier (jobType) and every job run an unique identifier (jobId)
 * If a job acquires a lock, it will only get it, if there isn't already a jobId saved for that jobType. The jobId
 * of the running job will be saved with a ttl to the lock table. A running job needs to renew its lock to show it's
 * still active and not died.
 *
 * We use Consistency Level Quorum to ensure that a Job is Locked or is not Locked. The CL makes the Lock mechanism
 * more deterministic.
 *
 * @param session
 */
class LockRepository(session: Session, lockTypes: LockTypes) {

  private val logger = getLogger(getClass)

  private val Table = "lock"

  private val LockTypeCol = "lock_type"

  private val LockCol = "job_lock"

  private def rowToJobId(row: Row) = row.getUUID(LockCol)

  private def buildInsertStatement(lockType: LockType): Insert = {
    // we do not write the LOCK here because we want to touch it with a "ttl" with the "updateLock" method
    val insertStmt: Insert = insertInto(Table)
      .value(LockTypeCol, lockType.name)
      .ifNotExists()
    insertStmt.using(QueryBuilder.ttl((14 days).toSeconds.toInt))
    insertStmt.setConsistencyLevel(LOCAL_QUORUM)
    insertStmt.setSerialConsistencyLevel(LOCAL_SERIAL)

    insertStmt
  }

  //scalastyle:off null - we need null value for our expression
  private def getLockStatement(lockType: LockType, jobId: UUID, ttl: Duration): RegularStatement = {
    val stmt = update(Table)
      .`with`(QueryBuilder.set(LockCol, jobId))
      .using(QueryBuilder.ttl(ttl.toSeconds.toInt))
      .where(QueryBuilder.eq(LockTypeCol, lockType.name))
      .onlyIf(QueryBuilder.eq(LockCol, null))

    stmt.setConsistencyLevel(LOCAL_QUORUM)
    stmt.setSerialConsistencyLevel(LOCAL_SERIAL)
    stmt
  }
  //scalastyle:on

  private def updateLockStatement(lockType: LockType, jobId: UUID, ttl: Duration): RegularStatement = {
    val stmt = update(Table)
      .`with`(QueryBuilder.set(LockCol, jobId))
      .using(QueryBuilder.ttl(ttl.toSeconds.toInt))
      .where(QueryBuilder.eq(LockTypeCol, lockType.name))
      .onlyIf(QueryBuilder.eq(LockCol, jobId))
    stmt.setConsistencyLevel(LOCAL_QUORUM)
    stmt.setSerialConsistencyLevel(LOCAL_SERIAL)
    stmt
  }

  private def unlockStatement(lockType: LockType, jobId: UUID): RegularStatement = {
    val stmt = update(Table)
      .`with`(QueryBuilder.set(LockCol, null))
      .where(QueryBuilder.eq(LockTypeCol, lockType.name))
      .onlyIf(QueryBuilder.eq(LockCol, jobId))
    stmt.setConsistencyLevel(LOCAL_QUORUM)
    stmt.setSerialConsistencyLevel(LOCAL_SERIAL)
    stmt
  }

  /**
   * Delete lock for the given job type.
   */
  def delete(jobType: JobType): Future[ResultSet] =
    session.executeAsync(QueryBuilder.delete().all().from(Table).where(QueryBuilder.eq(LockTypeCol, jobType.lockType.name))
      .setConsistencyLevel(LOCAL_QUORUM))


  def save(jobType: JobType): Future[ResultSet] =
    session.executeAsync(buildInsertStatement(jobType.lockType))

  /**
   * we use the concept of lightweight transactions (or compare and set, CAS) here as implemented by C* with
   * the PAXOS consensus protocol. see http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_ltwt_transaction_c.html
   * for more info
   */
  def updateLock(jobType: JobType, jobId: UUID, ttl: Duration = 60 seconds)
                (implicit ec: ExecutionContext): Future[Boolean] =
    session.executeAsync(updateLockStatement(jobType.lockType, jobId, ttl)).map { resultSet =>
      resultSet.one.getBool(0)
      // 0 == applied col; get column by name is not available as it is not defined in the schema
    }

  def acquireLock(jobType: JobType, jobId: UUID, ttl: Duration = 60 seconds)
                 (implicit ec: ExecutionContext): Future[Boolean] = {
    for {
      insert <- save(jobType)
      resultSet <- session.executeAsync(getLockStatement(jobType.lockType, jobId, ttl))
    } yield {
      resultSet.one.getBool(0)
    }
  }

  def releaseLock(jobType: JobType, jobId: UUID)(implicit ec: ExecutionContext): Future[Boolean] =
    session.executeAsync(unlockStatement(jobType.lockType, jobId)).map { resultSet =>
      resultSet.one.getBool(0)
      // 0 == applied col; get column by name is not available as it is not defined in the schema
    }

  /**
   * @param jobType the job type of the job, e.g. "product_full_import"
   */
  def getIdForType(jobType: JobType)(implicit ec: ExecutionContext): Future[Option[UUID]] = {
    val query = select().all().from(Table).where(QueryBuilder.eq(LockTypeCol, jobType.lockType.name)).setConsistencyLevel(LOCAL_QUORUM)
    session.executeAsync(query).map(rs => Option(rs.one)).map(_.map(rowToJobId))
  }

  /**
   * Returns a list of Locks (jobType + jobId).
   * To avoid select * statement without key in where clause,
   * we do multiple select statements with given primary key
   */
  def getAll()(implicit ec: ExecutionContext): Future[Seq[Lock]] = {

    def getLock(lockType: LockType) = {
      val selectLock = select().all().from(Table)
        .where(QueryBuilder.eq(LockTypeCol, lockType.name))
        .setConsistencyLevel(LOCAL_QUORUM)
      session.executeAsync(selectLock).map(rs =>
        Option(rs.one).flatMap { row =>
          val lockTypeName = row.getString(LockTypeCol)
          lockTypes(lockTypeName) match {
            case Some(lockType) => Some(Lock(lockType, row.getUUID(LockCol)))
            case None => logger.error(s"Could not find matching lock type for name: $lockTypeName")
              None
          }
        }
      )
    }

    val res = lockTypes.all.toList.map(getLock)
    Future.sequence(res).map(_.flatten)
  }

  def clear(): Future[ResultSet] = {
    /* no consistency level LOCAL_QUORUM on truncate, quick googling seemed to imply it's a bad idea to set a consistency level
    on truncate */
    session.executeAsync(truncate(Table))
  }

}