package de.kaufhof.hajobs

import java.util.{TimeZone, UUID}

import akka.actor.ActorSystem
import com.datastax.driver.core.utils.UUIDs
import org.quartz._
import org.quartz.impl.DirectSchedulerFactory
import org.quartz.simpl.{RAMJobStore, SimpleThreadPool}
import play.api.Logger

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
 * A JobManager is responsible for running jobs. It creates the infrastructure, checks locking  etc. for job running
 * @param managedJobs all the jobs the manager knows and may manage. Jobs are defined lazy, so that the jobs map can
 *                    contain jobs that themselves need the JobManager (allow cyclic dependencies)
 */
class JobManager(managedJobs: => Jobs,
                 lockRepo: LockRepository,
                 actorSystem: ActorSystem,
                 scheduler: Scheduler,
                 enableJobScheduling: Boolean) {

  def this(jobs: Seq[Job], lockRepo: LockRepository, actorSystem: ActorSystem, sched: Scheduler = JobManager.createScheduler, enableJobScheduling: Boolean = true) =
    this(Jobs(jobs), lockRepo, actorSystem, sched, enableJobScheduling)

  init()

  protected def init(): Unit = {
    if (enableJobScheduling) {
      Logger.debug("JobManager: Starting scheduler")
      scheduler.start
      scheduleJobs
    } else {
      Logger.warn("CRON JOBS ARE GLOBALLY DISABLED via config, will not schedule jobs!")
    }
  }

  def shutdown(): Unit = {
    Logger.debug("JobManager: Shutting down scheduler")
    scheduler.shutdown(false)
    managedJobs.keys.foreach(cancelJob)
  }

  protected def scheduleJob(jobToSchedule: Job) = {
    val jobType = jobToSchedule.jobType
    try {
      jobToSchedule.cronExpression.map { cronExpression =>
        Logger.debug(s"scheduling job for $jobType")
        val job = JobBuilder
          .newJob(classOf[TriggerPuller])
          .usingJobData(TPData(jobType, this).asDataMap)
          .build()

        val jobName = jobType.name

        val trigger = TriggerBuilder
          .newTrigger
          .withIdentity(s"$jobName-trigger")
          .withSchedule(CronScheduleBuilder
          // this will throw an exception if the expression is incorrect
          .cronSchedule(cronExpression)
          .inTimeZone(TimeZone.getTimeZone("UTC"))
          ).build()

        scheduler.scheduleJob(job, trigger)

        Logger.info(s"job '$jobType' has cron expression '${trigger.getCronExpression}', first execution at ${trigger.getNextFireTime}")
      }.getOrElse(
          Logger.info(s"no cronExpression defined for job $jobType")
        )
    } catch {
      case NonFatal(e) =>
        Logger.error(s"could not start scheduler for job type=$jobType", e)
        throw e
    }
  }

  protected def scheduleJobs: Unit = {
    managedJobs.values.foreach(scheduleJob)
  }

  /**
   * Start a job
   * @param jobType JobType of the job to start
   * @return StartStatus, f.e. Started if job could be started or LockedStatus if job is already running
   */
  def triggerJob(jobType: JobType): Future[JobStartStatus] = {
    val triggerId = UUIDs.timeBased()
    Logger.info(s"triggering job of type $jobType with triggerid $triggerId")
    retriggerJob(jobType, triggerId)
  }

  /**
   * Restarts a job for specified trigger id. Part of (re-)starting a job is locking for that job and managing
   * the lifecycle of the KeepLockActor. So the this method does the following:
   * - Acquire and release the lock for the job
   * - Trigger in case of success the job
   * - Starting and stopping the KeepJobLockActor
   *
   * @param jobType JobType of the job to start
   * @param triggerId Trigger Id to restart
   * @return StartStatus, f.e. Started if job could be started or LockedStatus if job is already running.
   */
  def retriggerJob(jobType: JobType, triggerId: UUID): Future[JobStartStatus] = {
    val jobId = UUIDs.timeBased()
    val job = managedJobs(jobType)
    lockRepo.acquireLock(jobType, jobId, job.lockTimeout).flatMap { haveLock =>
      if (haveLock) {
        val lockKeeper = actorSystem.actorOf(KeepJobLockedActor.props(lockRepo, jobType, jobId, job.lockTimeout, job.cancel _), jobType.name + "_LOCK")
        def finishCallback(): Unit = {
          Logger.info(s"Job with type $jobType and id $jobId finished, cleaning up...")
          lockRepo.releaseLock(jobType, jobId)
          actorSystem.stop(lockKeeper)
        }

        implicit val context = JobContext(jobId, triggerId, finishCallback _)

        // before starting new Job, update old pending jobs to status dead
        job.run().map {
          case res@Started(jobId, _) =>
            Logger.info(s"Job with type $jobType and id $jobId successfully started")
            res

          case res@default =>
            finishCallback()
            res
        }.recover {
          case NonFatal(e) =>
            Logger.error(s"error starting job with type $jobType and id $jobId")
            finishCallback()
            Error(e.getMessage)
        }
      } else {
        lockRepo.getIdForType(jobType).map { uuid =>
          val id = uuid.map(id => Some(id)).getOrElse(None)
          LockedStatus(id)
        }
      }
    }
  }

  def cancelJob(jobType: JobType): Unit = {
    Logger.info(s"cancelling job for job type $jobType")
    managedJobs(jobType).cancel
  }

  def getJob(jobType: JobType): Job = managedJobs(jobType)

  def allJobStatus(jobType: JobType): Future[List[JobStatus]] = managedJobs.get(jobType).get.allJobStatus

  def jobStatus(jobType: JobType, jobId: UUID): Future[Option[JobStatus]] = managedJobs.get(jobType).get.jobStatus(jobId)
}

object JobManager {
  def createScheduler: Scheduler = {
    Logger.info("creating quartz scheduler")

    // ensure scheduler is only registered once
    if (!Option(DirectSchedulerFactory.getInstance.getScheduler("JobManagerScheduler")).isDefined) {
      // we could use createVolatileScheduler, but we want to set the name of the thread pool
      val pool = new SimpleThreadPool()
      pool.setThreadNamePrefix("quartz-")
      pool.setThreadCount(1)
      pool.setMakeThreadsDaemons(true)

      DirectSchedulerFactory.getInstance.createScheduler("JobManagerScheduler", "Single", pool, new RAMJobStore)
    }
    DirectSchedulerFactory.getInstance.getScheduler("JobManagerScheduler")
  }
}

private[hajobs] class TriggerPuller extends org.quartz.Job {

  override def execute(context: JobExecutionContext): Unit = {
    val data = TPData(context.getJobDetail.getJobDataMap)

    Logger.info(s"It's ${context.getFireTime}, triggering job '${data.jobType}' next call at ${context.getNextFireTime}")

    data.manager.triggerJob(data.jobType)
  }
}

private[hajobs] case class TPData(jobType: JobType, manager: JobManager) {

  def asDataMap: JobDataMap = {
    new JobDataMap(Map(
      "jobType" -> jobType,
      "manager" -> manager
    ).asJava)
  }
}

private[hajobs] object TPData {
  def apply(dataMap: JobDataMap): TPData = {

    val data = dataMap.asScala

    def get[T](key: String): T = {
      data.get(key) match {
        case Some(x) =>
          x.asInstanceOf[T]
        case None =>
          throw new IllegalStateException(s"missing required key '${key}' in job data")
      }
    }
    new TPData(get[JobType]("jobType"), get[JobManager]("manager"))
  }
}

