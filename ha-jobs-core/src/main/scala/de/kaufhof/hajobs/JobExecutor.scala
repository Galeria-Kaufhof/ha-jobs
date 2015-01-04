package de.kaufhof.hajobs

import java.util.UUID

import akka.actor._
import com.datastax.driver.core.utils.UUIDs

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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
      val startStatus = run(job, triggerId)
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
          lockRepo.releaseLock(ctxt.jobType, ctxt.jobId)
          context.stop(running.lockKeeper)
        case None =>
          log.warning("On Completed: Found no running job {} with id {}", ctxt.jobType.name, ctxt.jobId)
      }
      context.become(running(jobs - ctxt.jobId))
    case LostLock(ctxt) =>
      jobs.get(ctxt.jobId) match {
        case Some(running) =>
          log.info("Lost lock for job {} / {}, asking job to cancel", ctxt.jobType.name, ctxt.jobId)
          // jobExecution.cancel() should complete() the jobExecution.result, which should trigger a Completed msg
          running.jobExecution.cancel()
        case None =>
          log.warning("On LostLock: Found no running job {} / {}", ctxt.jobType.name, ctxt.jobId)
      }
      context.become(running(jobs - ctxt.jobId))
    case Cancel(jobType) =>
      jobs.values.filter(_.job.jobType == jobType).map { running =>
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
  private def run(job: Job, triggerId: UUID): Future[JobStartStatus] = {
    val jobId = UUIDs.timeBased()

    lockRepo.acquireLock(job.jobType, jobId, job.lockTimeout).flatMap { haveLock =>
      if (haveLock) {
        
        implicit val jobContext = JobContext(job.jobType, jobId, triggerId)

        try {
          val execution = job.run()

          val lockKeeper = context.actorOf(
            KeepJobLockedActor.props(lockRepo, job.jobType, jobId, job.lockTimeout, () => self ! LostLock(jobContext)),
            job.jobType.name + "_LOCK")

          Future.successful(Running(job, execution, lockKeeper))
        } catch {
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
  private[JobExecutor] case class Running(job: Job, jobExecution: JobExecution, lockKeeper: ActorRef) extends JobStartStatus

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