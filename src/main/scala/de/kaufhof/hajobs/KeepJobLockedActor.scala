package de.kaufhof.hajobs

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props}
import org.joda.time.{DateTime, Seconds}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

/**
 * An Actor which helps to keep long running sychronous job tasks alive by updating the job lock periodically.
 */
class KeepJobLockedActor(lockRepository: LockRepository, jobType: JobType, jobId: UUID, lockTtl: FiniteDuration, cancel: () => Unit)
  extends Actor with ActorLogging {

  import context._
  import KeepJobLockedActor._

  log.info("Started KeepJobLockedActor, jobType: {}, jobId: {}", jobType, jobId)
  private var schedule = system.scheduler.scheduleOnce(0 millis, self, Tick)
  private var lastSuccess: DateTime = DateTime.now()
  private var isCanceled = false

  def receive: Receive = {

    case Tick =>
      try {
        log.debug("KeepJobLockedActor: updated lock for job {}", jobType)
        lockRepository.updateLock(jobType, jobId, lockTtl).map { res =>
          if (!res) {
            doCancel()
          }
          lastSuccess = DateTime.now

          // calling system throws NPE when Actor is shut down
          // dont know how to decide if system is callable
          Try(schedule = system.scheduler.scheduleOnce(lockTtl / 2, self, Tick))
        }.recover {
          case NonFatal(e) =>
            if (Seconds.secondsBetween(lastSuccess, DateTime.now()).getSeconds > lockTtl.toSeconds) {
              doCancel()
            } else {
              val retryTimeout = lockTtl / 10
              log.warning("error when tried to update lock for jobType: {}, jobId: {}, error: {}, retrying in {}", jobType, jobId, e, retryTimeout)
              schedule = system.scheduler.scheduleOnce(retryTimeout, self, Tick)
            }
        }
      } catch {
        case NonFatal(e) =>
          log.error(e, "could not update lock for jobType: {}, jobId: {}", jobType, jobId)
          doCancel()
      }
  }

  private def doCancel() = {
    if (!isCanceled) {
      log.info("Job cancelled because of lost lock for jobType: {}, jobId: {}", jobType, jobId)
      cancel()
      isCanceled = true
    }
  }

  override def postStop(): Unit = {
    schedule.cancel()
    log.info("Stopped KeepJobLockedActor, jobType: {}, jobId: {}", jobType, jobId)
  }
}

object KeepJobLockedActor {
  def props(lockRepository: LockRepository, jobType: JobType, uuid: UUID, lockTtl: FiniteDuration, cancel: () => Unit): Props =
    Props(new KeepJobLockedActor(lockRepository, jobType, uuid: UUID, lockTtl, cancel))

  case object Tick
}
