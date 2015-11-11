package de.kaufhof.hajobs

import java.util.UUID

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.KeepJobLockedActor.InfoResponse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Executes jobs, manages currently running jobs.
 */
class JobExecutor(lockRepo: LockRepository) extends Actor with ActorLogging {

  import de.kaufhof.hajobs.JobExecutor._

  override def receive: Receive = running(Map.empty)

  private def running(jobs: Map[UUID, Running]): Receive = {
    case Execute(job, triggerId) =>
      val origSender = sender()
      val startStatus = run(job, triggerId, jobs)
      startStatus onComplete {
        case Success(Running(job, execution, lockKeeper)) =>
          log.info("Job {} / {} successfully started", job.jobType.name, execution.context.jobId)
          origSender ! Started(execution.context.jobId)
          self ! Running(job, execution, lockKeeper)
          // We must register the onComplete callback after the Running msg,
          // otherwise the Completed msg might be received before the was registered.
          execution.result.onComplete { _ =>
            self ! Completed(execution.context)
          }
        case Success(r) => origSender ! r
        case Failure(e) => origSender ! Status.Failure(e)
      }
    case msg@Running(job, execution, lockKeeper) =>
      context.become(running(jobs + (execution.context.jobId -> msg)))
    case Completed(ctxt) =>
      jobs.get(ctxt.jobId) match {
        case Some(running) =>
          log.info("Job {} / {} completed, cleaning up...", ctxt.jobType.name, ctxt.jobId)
          running.lockKeeper.foreach { lockKeeper =>
            context.stop(lockKeeper)
            lockRepo.releaseLock(ctxt.jobType, ctxt.jobId)
          }
        case None =>
          log.warning("On Completed: Found no running job {} with id {}", ctxt.jobType.name, ctxt.jobId)
      }
      context.become(running(jobs - ctxt.jobId))
    case LostLock(ctxt) =>
      val newState = jobs.get(ctxt.jobId) match {
        case Some(running) =>
          log.info("Lost lock for job {} / {}, asking job to cancel", ctxt.jobType.name, ctxt.jobId)
          // We can already stop the lock keeper, and by setting it to None on the running job we know later (on Completed),
          // that we don't have to releaseLock
          running.lockKeeper.foreach(context.stop)
          // jobExecution.cancel() should complete() the jobExecution.result, which should trigger a Completed msg
          running.jobExecution.cancel()
          // we must keep the job so that we can handle Completed accordingly
          jobs.updated(ctxt.jobId, running.copy(lockKeeper = None))
        case None =>
          log.warning("On LostLock: Found no running job {} / {}", ctxt.jobType.name, ctxt.jobId)
          jobs - ctxt.jobId
      }
      context.become(running(newState))
    case Cancel(jobType) =>
      jobs.values.filter(_.job.jobType == jobType).foreach { running =>
        log.info("Canceling job {} / {}", jobType.name, running.jobExecution.context.jobId)
        // jobExecution.cancel() should complete() the jobExecution.result, which should trigger a Completed msg
        running.jobExecution.cancel()
      }
  }

  /**
   * Restarts a job for specified trigger id. Part of (re-)starting a job is locking for that job and managing
   * the lifecycle of the KeepLockActor. So the this method does the following:
   * - Acquire and release the lock for the job
   * - Trigger in case of success the job
   * - Starting and stopping the KeepJobLockActor
   *
   * @return StartStatus, f.e. Started if job could be started or LockedStatus if job is already running.
   */
  private def run(job: Job, triggerId: UUID, runningJobs: Map[UUID, Running]): Future[JobStartStatus] = {
    val jobId = UUIDs.timeBased()

    lockRepo.acquireLock(job.jobType, jobId, job.lockTimeout).flatMap { haveLock =>
      if (haveLock) {
        
        implicit val jobContext = JobContext(job.jobType, jobId, triggerId)
        val lockKeeperName = job.jobType.name + "_LOCK"

        try {
          val execution = job.run()

          val lockKeeper = context.actorOf(
            KeepJobLockedActor.props(lockRepo, job.jobType, jobId, job.lockTimeout, () => self ! LostLock(jobContext)), lockKeeperName
          )

          Future.successful(Running(job, execution, Some(lockKeeper)))
        } catch {
          case e: InvalidActorNameException =>
            logInvalidActorNameException(e, job, triggerId, runningJobs, lockKeeperName)
            lockRepo.releaseLock(job.jobType, jobContext.jobId)
            Future.failed(e)
          case e: Throwable =>
            log.error(e, "Error starting job {} / {}, releasing lock.", job.jobType.name, jobId)
            lockRepo.releaseLock(job.jobType, jobContext.jobId)
            Future.failed(e)
        }
      } else {
        lockRepo.getIdForType(job.jobType).map { uuid =>
          val id = uuid.map(id => Some(id)).getOrElse(None)
          log.info("Could not run job {} because it's locked by id {}", job.jobType.name, id)
          LockedStatus(id)
        }
      }
    }
  }

  private def logInvalidActorNameException(e: InvalidActorNameException, job: Job, triggerId: UUID, runningJobs: Map[UUID, Running], lockKeeperName: String) {

    implicit val timeout = Timeout(10 seconds)

    def logLockKeeperInfo(lockKeeper: ActorRef): Future[Unit] = {
      (lockKeeper ? KeepJobLockedActor.InfoRequest).mapTo[InfoResponse].map(info =>
        log.info("Info from lock keeper for aborted job {} (triggerId {}): " +
          s"running for jobId ${info.jobId}, lastSuccess ${info.lastSuccess}, canceled ${info.isCanceled}.",
          job.jobType.name, triggerId)
      )
    }

    def lookupAndLogInfoFromLockKeeper() {
      context.actorSelection(lockKeeperName).resolveOne().onComplete {
        case Success(lockKeeper) =>
          logLockKeeperInfo(lockKeeper)
        case Failure(e2) =>
          log.info("Could not find lock keeper for aborted job {} (triggerId {}).", job.jobType.name, triggerId)
      }
    }
    runningJobs.collectFirst { case (runningJobId, running) if running.job.jobType == job.jobType => (running.jobExecution.context, running.lockKeeper) } match {
      case Some((jobContext, Some(lockKeeper))) =>
        log.error(e, s"Error starting job {} (triggerId {}), releasing lock. Found running job with jobId ${jobContext.jobId}, triggerId ${jobContext.triggerId}." +
          " Requesting info from lock keeper, stay tuned.",
          job.jobType.name, triggerId)
        logLockKeeperInfo(lockKeeper)
      case Some((jobContext, None)) =>
        log.error(e, s"Error starting job {} (triggerId {}), releasing lock. Found running job with jobId ${jobContext.jobId}, triggerId ${jobContext.triggerId}." +
          " Looking up lock keeper actor to get more info, stay tuned",
          job.jobType.name, triggerId)
        lookupAndLogInfoFromLockKeeper()
      case None =>
        log.error(e, "Error starting job {} (triggerId {}), releasing lock. No running job found, looking up lock keeper actor, stay tuned.", job.jobType.name, triggerId)
        lookupAndLogInfoFromLockKeeper()
    }
  }

}

object JobExecutor {

  def props(lockRepo: LockRepository): Props = Props(new JobExecutor(lockRepo))

  /**
   * Runs the given job, a JobStartStatus is sent to the sender.
   */
  case class Execute(job: Job, triggerId: UUID)

  /**
   * Cancel any job execution started by the job of the given JobType.
   */
  case class Cancel(jobType: JobType)

  /**
   * Sent by the JobExecutor actor to itself when the job was successfully started. Also
   * used to store job execution related info for a jobId.
   */
  private[JobExecutor] case class Running(job: Job, jobExecution: JobExecution, lockKeeper: Option[ActorRef]) extends JobStartStatus

  /**
   * Sent by the onLockLost callback invoked by the KeepJobLockedActor to the JobExecutor actor
   * to trigger cancellation of the job execution.
   */
  private[JobExecutor] case class LostLock(jobContext: JobContext)

  /**
   * Sent by the JobExecution.result.onComplete callback to the JobExecutor actor
   * to trigger cleanup (e.g. lock release).
   */
  private[JobExecutor] case class Completed(jobContext: JobContext)

}