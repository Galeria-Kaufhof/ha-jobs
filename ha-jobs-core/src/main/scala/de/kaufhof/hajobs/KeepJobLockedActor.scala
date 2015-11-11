package de.kaufhof.hajobs

import java.util.UUID

import akka.actor.{Cancellable, Actor, ActorLogging, Props}
import org.joda.time.{DateTime, Seconds}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

/**
 * An Actor which helps to keep long running sychronous job tasks alive by updating the job lock periodically.
 */
class KeepJobLockedActor(lockRepository: LockRepository, jobType: JobType, jobId: UUID, lockTtl: FiniteDuration, onLockLost: () => Unit)
  extends Actor with ActorLogging {

  import context._
  import KeepJobLockedActor._

  log.info("Started KeepJobLockedActor, jobType: {}, jobId: {}", jobType, jobId)
  @volatile
  private var schedule = scheduleTick()
  private var lastSuccess: DateTime = DateTime.now()
  private var isCanceled = false

  private def scheduleTick(): Cancellable = {
    system.scheduler.scheduleOnce(lockTtl / 2, self, Tick)
  }

  def receive: Receive = {
    case InfoRequest =>
      sender() ! InfoResponse(jobId, lastSuccess, isCanceled)
    case Cancel =>
      if (!isCanceled) {
        log.info("Cancelling lock keeping because of lost lock for job {} / {}", jobType.name, jobId)
        onLockLost()
        isCanceled = true
      }
    case Tick =>
      try {
        log.debug("KeepJobLockedActor: updated lock for job {}", jobType.name)
        lockRepository.updateLock(jobType, jobId, lockTtl).map { res =>
          if (!res) {
            self ! Cancel
          } else {
            lastSuccess = DateTime.now
            // calling system throws NPE when Actor is shut down
            // dont know how to decide if system is callable
            Try(schedule = scheduleTick())
          }
        }.recover {
          case NonFatal(e) =>
            if (Seconds.secondsBetween(lastSuccess, DateTime.now()).getSeconds > lockTtl.toSeconds) {
              self ! Cancel
            } else {
              val retryTimeout = lockTtl / 10
              log.warning("error when tried to update lock for job {} / {}, error: {}, retrying in {}", jobType.name, jobId, e, retryTimeout)
              schedule = system.scheduler.scheduleOnce(retryTimeout, self, Tick)
            }
        }
      } catch {
        case NonFatal(e) =>
          log.error(e, "could not update lock for job {} / {}", jobType.name, jobId)
          self ! Cancel
      }
  }

  override def postStop(): Unit = {
    schedule.cancel()
    log.info("Stopped KeepJobLockedActor for job {} / {}", jobType.name, jobId)
  }
}

object KeepJobLockedActor {
  def props(lockRepository: LockRepository, jobType: JobType, uuid: UUID, lockTtl: FiniteDuration, cancel: () => Unit): Props =
    Props(new KeepJobLockedActor(lockRepository, jobType, uuid: UUID, lockTtl, cancel))

  case object Tick
  case object Cancel
  case object InfoRequest
  case class InfoResponse(jobId: UUID, lastSuccess: DateTime, isCanceled: Boolean)
}
