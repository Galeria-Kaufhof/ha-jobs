package de.kaufhof.hajobs.examples

import akka.actor.ActorSystem
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs._
import de.kaufhof.hajobs.testutils.TestCassandraConnection
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success, Try}

object Ex1ScheduledJob extends App with TestCassandraConnection {

  // == Product Import Job
  val ProductImportLockType = LockType("ProductImportLock")
  val ProductImportJobType = JobType("ProductImport", ProductImportLockType)

  /**
   * A job that normally would import products, but now only prints "importing" some times.
   */
  class ProductImport(override val jobStatusRepository: JobStatusRepository,
                      cronExpression: Option[String]) extends Job(
    ProductImportJobType, retriggerCount = 3, cronExpression = cronExpression) with WriteStatus {

    override def run()(implicit context: JobContext): JobExecution = new JobExecution() {

      private val promise = Promise[Unit]()
      override val result = promise.future

      override def cancel(): Unit = {
        // We might update some flag that could be checked by `importProducts()`
      }

      writeStatus(Running)

      // onComplete: after updating our status we must complete the result. This will
      // release the lock and stop the lock keeper actor.
      importProducts().onComplete(updateStatus.andThen(_ => promise.success(())))

      // A not so long running operation, but still producing some side effect
      private def importProducts(): Future[Int] = {
        Future.successful {
          println("Importing products ... done.")
          42 // products imported
        }
      }

      private def updateStatus(implicit context: JobContext): Try[Int] => Future[JobStatus] = {
        case Success(count) =>
          writeStatus(Finished, Some(Json.obj("count" -> count)))
        case Failure(e) =>
          writeStatus(Failed, Some(Json.obj("error" -> e.getMessage)))
      }

    }

  }

  // Setup repos needed for jobs + job manager
  // session: the Cassandra Session (com.datastax.driver.core.Session)
  val statusRepo = new JobStatusRepository(session, jobTypes = JobTypes(ProductImportJobType))
  val lockRepo = new LockRepository(session, LockTypes(ProductImportLockType))

  // Setup jobs
  val productImporter = new ProductImport(statusRepo, Some("0/10 * * * * ?"))
  val jobSupervisor = new JobSupervisor(manager, lockRepo, statusRepo, Some("0 * * * * ?"))

  val system = ActorSystem("example1")

  // Setup the JobManager
  lazy val manager: JobManager = new JobManager(Seq(productImporter, jobSupervisor), lockRepo, statusRepo, system)

  println("Sleeping")
  Thread.sleep(20000)


  println("Stopping")
  manager.shutdown()
  system.terminate()
  session.getCluster.close()

}
