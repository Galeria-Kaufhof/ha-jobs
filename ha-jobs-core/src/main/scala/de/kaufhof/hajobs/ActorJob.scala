package de.kaufhof.hajobs

import akka.actor._

import scala.concurrent.{Promise, Future}

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
               retriggerCount: Int = 1,
               cronExpression: Option[String] = None,
               lockTimeout: FiniteDuration = 60 seconds)
  extends Job(jobType, retriggerCount, cronExpression, lockTimeout) {

  override def run()(implicit ctxt: JobContext): JobExecution = new JobExecution() {

    private val actorName = s"${jobType.name}-${ctxt.jobId}"
    private val actor = system.actorOf(props(ctxt), actorName)
    private val promise = Promise[Unit]()
    override val result: Future[Unit] = promise.future

    private val watcher = system.actorOf(Props(new Actor {
      context.watch(actor)
      override def receive: Actor.Receive = {
        case Terminated(actorRef) =>
          context.unwatch(actorRef)
          promise.success(())
          context.stop(self)
      }
    }), s"${actorName}_watcher")

    override def cancel(): Unit = actor ! ActorJob.Cancel

  }

}

object ActorJob {
  case object Cancel
}
