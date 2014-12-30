package de.kaufhof.hajobs

import akka.actor.ActorSystem
import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.JobManagerSpec._
import de.kaufhof.hajobs.testutils.MockInitializers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.quartz.Scheduler
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import play.api.Application
import de.kaufhof.hajobs.testutils.StandardSpec

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class JobManagerSpec extends StandardSpec {

  private val lockRepository = mock[LockRepository]

  private val jobStatusRepository = mock[JobStatusRepository]

  private var scheduler: Scheduler = _

  private val actorSystem = mock[ActorSystem]

  private val jobUpdater = mock[JobUpdater]

  val app = mock[Application]

  override def beforeEach() {
    MockInitializers.initializeLockRepo(lockRepository)
    reset(jobStatusRepository)
    reset(jobUpdater)
    when(jobUpdater.updateJobs()).thenReturn(Future.successful(Nil))
    reset(actorSystem)
    scheduler = JobManager.createScheduler
  }

  override def afterEach() {
    scheduler.shutdown(true)
  }

  "JobManager scheduler" should {
    "trigger a job with a cronExpression defined" in {
      val job = mock[Job]
      when(job.jobType).thenReturn(JobType1)
      when(job.cronExpression).thenReturn(Some("* * * * * ?"))


      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, scheduler, true)

      eventually(Timeout(scaled(3 seconds))) {
        verify(job, atLeastOnce()).run()(any[JobContext])
      }
    }

    "not trigger a job with no cronExpression defined" in {
      val mockedScheduler = mock[Scheduler]
      val job = mock[Job]
      when(job.jobType).thenReturn(JobType1)
      when(job.cronExpression).thenReturn(None)

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, true)
      verify(mockedScheduler, times(1)).start()
      verifyNoMoreInteractions(mockedScheduler)
    }

    "not trigger a job with a cronExpression defined, if scheduling is disabled" in {
      val mockedScheduler = mock[Scheduler]
      val job = mock[Job]
      when(job.jobType).thenReturn(JobType1)
      when(job.cronExpression).thenReturn(Some("* * * * * ?"))

      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, false)
      verifyNoMoreInteractions(mockedScheduler)
    }
  }

  "JobManager retrigger job" should {
    "release lock after a synchronous job finished" in {
      val job = new TestJob(jobStatusRepository)

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, scheduler, false)
      await(manager.retriggerJob(JobType1, UUIDs.timeBased()))

      verify(lockRepository, times(1)).acquireLock(any(), any(), any())(any())
      verify(lockRepository, times(1)).releaseLock(any(), any())(any())
      verify(actorSystem, times(1)).actorOf(any(), any())
      verify(actorSystem, times(1)).stop(any())
    }

    "release lock after a job failed on start" in {
      val mockedScheduler = mock[Scheduler]
      val job = mock[Job]
      when(job.jobType).thenReturn(JobType1)
      when(job.run()(any())).thenReturn(Future.failed(new RuntimeException("test exception") {
        // suppress the stacktrace to reduce log spam
        override def fillInStackTrace(): Throwable = this
      }))

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, false)
      await(manager.retriggerJob(JobType1, UUIDs.timeBased()))
      verify(lockRepository, times(1)).acquireLock(any(), any(), any())(any())
      verify(lockRepository, times(1)).releaseLock(any(), any())(any())
      verify(actorSystem, times(1)).actorOf(any(), any())
      verify(actorSystem, times(1)).stop(any())
    }
  }
}

object JobManagerSpec {
  class TestJob(jobStatusRepository: JobStatusRepository) extends Job(JobType1, 0) {
    override def run()(implicit context: JobContext): Future[JobStartStatus] = {
      context.finishCallback()
      Future.successful(Started(context.jobId))
    }

    override def cancel: Unit = {}
  }
}