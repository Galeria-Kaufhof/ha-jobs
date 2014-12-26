package de.kaufhof.hajobs

import akka.actor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * A [[de.kaufhof.hajobs.Job Job]] that runs the given actor (creates a new actor instance).
 *
 * The actor must:
 * $ - `stop()` itself (e.g. via `ActorContext.stop` once it's finished
 * $ - handle [[de.kaufhof.hajobs.ActorJob.Cancel ActorJob.Cancel]] and `stop` itself when it received this message
 */
class ActorJob(jobType: JobType,
               props: JobContext => Props,
               system: ActorSystem,
               jobStatusRepository: JobStatusRepository,
               retriggerCount: Int = 0,
               cronExpression: Option[String] = None,
               lockTimeout: FiniteDuration = 60 seconds)
  extends Job(jobType, jobStatusRepository, retriggerCount, cronExpression, lockTimeout) {

  private var actor: Option[ActorRef] = None

  override def run()(implicit ctxt: JobContext): Future[JobStartStatus] = {

    actor = Some(system.actorOf(props(ctxt), "myActorJob"))

    val watcher = system.actorOf(Props(new Actor {
      context.watch(actor.get)
      override def receive: Actor.Receive = {
        case Terminated(actorRef) =>
          context.unwatch(actorRef)
          ctxt.finishCallback()
          actor = None
          context.stop(self)
      }
    }), "ActorJobWatcher")

    Future.successful(Started(ctxt.jobId))
  }

  override def cancel(): Unit = actor.foreach(_ ! ActorJob.Cancel)
}

object ActorJob {
  case object Cancel
}
