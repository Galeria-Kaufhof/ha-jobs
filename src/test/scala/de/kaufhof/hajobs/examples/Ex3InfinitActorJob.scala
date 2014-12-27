package de.kaufhof.hajobs.examples

import akka.actor.{Actor, ActorSystem, Props}
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs._
import de.kaufhof.hajobs.testutils.TestCassandraConnection
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Ex3InfinitActorJob extends App with TestCassandraConnection {

  // == Product Import Job
  val ConsumerLockType = LockType("ConsumerLock")
  val ConsumerJobType = JobType("Consumer", ConsumerLockType)

  class QueueConsumerActor(interval: FiniteDuration,
                           override val jobStatusRepository: JobStatusRepository)
                          (implicit jobContext: JobContext) extends Actor with WriteStatus {

    override def jobType = ConsumerJobType

    writeStatus(Running)

    self ! "consume"
    
    override def receive: Receive = consuming(0)

    private def consuming(count: Int): Receive = {
      case "consume" =>
        println(s"Consuming, until now consumed $count items...")
        writeStatus(Running, Some(Json.obj("round" -> count)))
        context.system.scheduler.scheduleOnce(interval, self, "consume")
        context.become(consuming(count + 42))
      case ActorJob.Cancel =>
        // We should support ActorJob.Cancel and stop() processing.
        writeStatus(Canceled)
        context.stop(self)
    }

  }

  object QueueConsumerActor {
    def props(interval: FiniteDuration, statusRepo: JobStatusRepository)(jobContext: JobContext) =
      Props(new QueueConsumerActor(interval, statusRepo)(jobContext))
  }

  // Setup jobs + job manager
  val statusRepo = new JobStatusRepository(session, jobTypes = JobTypes(ConsumerJobType))
  val lockRepo = new LockRepository(session, LockTypes(ConsumerLockType))

  val system = ActorSystem("system")

  // The ActorJob does not define a `cronExpression`
  val queueConsumer = new ActorJob(ConsumerJobType, QueueConsumerActor.props(2 seconds, statusRepo), system, cronExpression = None)
  val jobSupervisor = new JobSupervisor(manager, lockRepo, statusRepo, Some("0 * * * * ?"))


  val manager: JobManager = new JobManager(Seq(queueConsumer, jobSupervisor), lockRepo, statusRepo, system)

  // manually trigger the job
  manager.triggerJob(ConsumerJobType) onComplete {
    case Success(Started(jobId, _)) => println(s"Started queue consumer job $jobId")
    // The Success case can also carry LockedStatus or Error
    case Success(els) => println(s"Could not start queue consumer: $els")
    case Failure(e) => println(s"An exception occurred when trying to start queue consumer: $e")
  }

  println("Sleeping")
  Thread.sleep(20000)


  println("Stopping")
  manager.shutdown()
  system.shutdown()
  session.getCluster.close()

}
