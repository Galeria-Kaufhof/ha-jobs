package de.kaufhof.hajobs.examples

import akka.actor.{Props, Actor, ActorSystem}
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs._
import de.kaufhof.hajobs.testutils.TestCassandraConnection
import play.api.libs.json.Json

object Ex2ScheduledActorJob extends App with TestCassandraConnection {

  // == Product Import Job
  val ProductImportLockType = LockType("ProductImportLock")
  val ProductImportJobType = JobType("ProductImport", ProductImportLockType)

  class ProductImportActor(override val jobStatusRepository: JobStatusRepository)
                          (implicit jobContext: JobContext) extends Actor with WriteStatus {

    import context.dispatcher // implicit EC, for writeStatus

    override def jobType = ProductImportJobType

    writeStatus(Running)

    self ! "go"

    override def receive: Receive = {
      case "go" =>
        println("Importing products ... done.")
        writeStatus(Finished, Some(Json.obj("count" -> 42)))
        // no need to tell the context that we're finished, this will be done by ActorJob when we're stopped.
        context.stop(self)
      case ActorJob.Cancel =>
        // We should support ActorJob.Cancel and stop() processing.
        writeStatus(Canceled)
        context.stop(self)
    }

  }

  object ProductImportActor {
    def props(statusRepo: JobStatusRepository)(jobContext: JobContext) = Props(new ProductImportActor(statusRepo)(jobContext))
  }

  // Setup jobs + job manager
  val statusRepo = new JobStatusRepository(session, jobTypes = JobTypes(ProductImportJobType))
  val lockRepo = new LockRepository(session, LockTypes(ProductImportLockType))

  val system = ActorSystem("system")

  val productImporter = new ActorJob(ProductImportJobType, ProductImportActor.props(statusRepo), system, cronExpression = Some("0/10 * * * * ?"))
  val jobSupervisor = new JobSupervisor(manager, lockRepo, statusRepo, Some("0 * * * * ?"))


  val manager: JobManager = new JobManager(Seq(productImporter, jobSupervisor), lockRepo, statusRepo, system)

  println("Sleeping")
  Thread.sleep(20000)


  println("Stopping")
  manager.shutdown()
  system.shutdown()
  session.getCluster.close()

}
