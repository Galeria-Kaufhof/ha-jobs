package de.kaufhof.hajobs

import akka.actor.ActorSystem
import org.quartz.Scheduler
import play.api.Configuration
import scaldi.Module

abstract class JobManagerModule extends Module {

  protected def jobs: Seq[Job]

  protected def actorSystem: ActorSystem

  protected def scheduler: Scheduler = JobManager.createScheduler

  protected def jobUpdater: JobUpdater = new JobUpdater(inject[LockRepository], inject[JobStatusRepository])

  private lazy val jobMap = jobs.map(s => s.jobType -> s).toMap

  binding to scheduler

  // Non-lazy because the job manager should start cron schedules
  binding toNonLazy new JobManager(jobMap,
    inject[LockRepository],
    inject[JobStatusRepository],
    actorSystem,
    scheduler,
    inject[Configuration].getBoolean("job.import.enableJobTriggering").getOrElse(true)
  ) destroyWith(_.shutdown)

  binding to new JobSupervisor(inject[JobManager],
    jobUpdater,
    inject[JobStatusRepository],
    inject[Configuration].getString(s"job.import.${JobTypes.JobSupervisor.name}.cron")
  )

}