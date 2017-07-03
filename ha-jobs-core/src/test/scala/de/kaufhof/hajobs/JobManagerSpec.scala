package de.kaufhof.hajobs

import akka.actor.{ActorNotFound, ActorSystem}
import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.JobManagerSpec._
import de.kaufhof.hajobs.testutils.{MockInitializers, StandardSpec}
import org.joda.time.{DateTime, DateTimeConstants, DateTimeZone}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.quartz.Scheduler
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import play.api.Application

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, blocking}
import scala.language.postfixOps

class JobManagerSpec extends StandardSpec {

  private val lockRepository = mock[LockRepository]

  private val jobStatusRepository = mock[JobStatusRepository]

  private val jobUpdater = mock[JobUpdater]

  private var actorSystem: ActorSystem = _

  private var manager: JobManager = _

  val app = mock[Application]

  override def beforeEach() {
    MockInitializers.initializeLockRepo(lockRepository)
    reset(jobStatusRepository)
    when(jobStatusRepository.list(any(), any(), any())(any())).thenReturn(Future.successful(Nil))
    reset(jobUpdater)
    when(jobUpdater.updateJobs()).thenReturn(Future.successful(Nil))
    actorSystem = ActorSystem("JobManagerSpec")
  }

  override def afterEach() {
    manager.shutdown()
    actorSystem.terminate()
  }

  "JobManager scheduler" should {
    "trigger a job with a cronExpression defined" in {
      val job = spy(new TestJob(Some("* * * * * ?")))

      manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem)
      await(manager.allJobsScheduled)

      eventually(Timeout(scaled(3 seconds))) {
        verify(job, atLeastOnce()).run()(any[JobContext])
      }
    }

    "not trigger a job with no cronExpression defined" in {
      val mockedScheduler = mock[Scheduler]
      val job = new TestJob(cronExpression = None)

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, true)
      await(manager.allJobsScheduled)
      verify(mockedScheduler, times(1)).start()
      verifyNoMoreInteractions(mockedScheduler)
    }

    "trigger a job immediately if the previous run was during a downtime" in {
      val dayOfWeekMapping = Map(DateTimeConstants.MONDAY -> "MON", DateTimeConstants.TUESDAY -> "TUE", DateTimeConstants.WEDNESDAY -> "WED",
        DateTimeConstants.THURSDAY -> "THU", DateTimeConstants.FRIDAY -> "FRI" -> DateTimeConstants.SATURDAY -> "SAT", DateTimeConstants.SUNDAY -> "SUN")
      val yesterday = dayOfWeekMapping(DateTime.now().minusDays(1).getDayOfWeek)
      val job = spy(new TestJob(Some(s"0 0 0 ? * $yesterday")))

      val mockedScheduler = mock[Scheduler]
      val lastJobStatus = JobStatus(UUIDs.timeBased(), job.jobType, UUIDs.timeBased(),
        JobState.Finished, JobResult.Success, DateTime.now().minusDays(2))
      when(jobStatusRepository.list(any(), any(), any())(any())).thenReturn(Future.successful(List(lastJobStatus)))
      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, true)
      await(manager.allJobsScheduled)
      verify(mockedScheduler, times(1)).start()
      verify(job, atLeastOnce()).run()(any())
    }

    "not trigger a missed job immediately if the next run is only some minutes in the future" in {
      val testDate = new DateTime(2017, 5, 16, 15, 14, DateTimeZone.UTC)
      val fiveMinutesAgo = testDate.minusMinutes(5)
      val job = spy(new TestJob(Some("0 10/20 * * * ?")))

      val mockedScheduler = mock[Scheduler]
      val lastJobStatus = JobStatus(UUIDs.timeBased(), job.jobType, UUIDs.timeBased(),
        JobState.Finished, JobResult.Success, testDate.minusDays(1))
      when(jobStatusRepository.list(any(), any(), any())(any())).thenReturn(Future.successful(List(lastJobStatus)))
      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, true) {
        override protected def now = testDate
      }
      await(manager.allJobsScheduled)

      verify(mockedScheduler, times(1)).start()
      verify(job, times(0)).run()(any())
    }

    "not trigger a job with a cronExpression defined, if scheduling is disabled" in {
      val mockedScheduler = mock[Scheduler]
      val job = new TestJob(Some("* * * * * ?"))

      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, false)
      await(manager.allJobsScheduled)
      verifyNoMoreInteractions(mockedScheduler)
    }
  }

  "JobManager retrigger job" should {
    "release lock after a synchronous job finished" in {
      val job = new TestJob()

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, enableJobScheduling = false)
      await(manager.allJobsScheduled)
      await(manager.retriggerJob(JobType1, UUIDs.timeBased()))

      verify(lockRepository, times(1)).acquireLock(any(), any(), any())(any())

      eventually { verify(lockRepository, times(1)).releaseLock(any(), any())(any()) }
      eventually {
        // KeepJobLockedActor path looks like this: "akka://system/user/JobExecutor/ProductImport_LOCK"
        an[ActorNotFound] shouldBe thrownBy(await(actorSystem.actorSelection(".*_LOCK").resolveOne()))
      }
    }

    "release lock after a job failed on start" in {
      val mockedScheduler = mock[Scheduler]
      val job = mock[Job]
      when(job.jobType).thenReturn(JobType1)
      when(job.run()(any())).thenThrow(newTestException)

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, false)
      await(manager.allJobsScheduled)
      a[RuntimeException] should be thrownBy await(manager.retriggerJob(JobType1, UUIDs.timeBased()))

      verify(lockRepository, times(3)).acquireLock(any(), any(), any())(any())
      verify(lockRepository, times(3)).releaseLock(any(), any())(any())
      an[ActorNotFound] shouldBe thrownBy(await(actorSystem.actorSelection(".*_LOCK").resolveOne()))
    }

    "release lock after a job failed result" in {
      val mockedScheduler = mock[Scheduler]
      val job = new TestJob() {
        override def run()(implicit context: JobContext): JobExecution = new JobExecution() {
          override def result = Future.failed(newTestException)
          override def cancel: Unit = ()
        }
      }

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, false)
      await(manager.allJobsScheduled)
      await(manager.retriggerJob(JobType1, UUIDs.timeBased()))

      verify(lockRepository, times(1)).acquireLock(any(), any(), any())(any())

      eventually(verify(lockRepository, times(1)).releaseLock(any(), any())(any()))
      eventually(an[ActorNotFound] shouldBe thrownBy(await(actorSystem.actorSelection(".*_LOCK").resolveOne())))
    }

    "set job to failed if job failed on start" in {
      val mockedScheduler = mock[Scheduler]
      val job = new TestJob() {
        override def run()(implicit context: JobContext): JobExecution = throw newTestException
      }
      var jobStatus: List[JobStatus] = Nil
      when(jobStatusRepository.save(any())(any())).thenAnswer(new Answer[Future[JobStatus]] {
        override def answer(invocation: InvocationOnMock): Future[JobStatus] = {
          jobStatus = List(invocation.getArguments.head.asInstanceOf[JobStatus])
          Future.successful(jobStatus.head)
        }

      })
      when(jobStatusRepository.getLatestMetadata(any())(any())).thenAnswer(new Answer[Future[List[JobStatus]]] {
        override def answer(invocation: InvocationOnMock): Future[List[JobStatus]] = Future.successful(jobStatus)
      })

      val manager = new JobManager(Seq(job), lockRepository, jobStatusRepository, actorSystem, mockedScheduler, false)
      await(manager.allJobsScheduled)
      await(manager.retriggerJob(JobType1, UUIDs.timeBased()))

      verify(jobStatusRepository, times(1)).save(any())(any())
      await(jobStatusRepository.getLatestMetadata()).head.jobResult should be(JobResult.Failed)

    }
  }
}

object JobManagerSpec {
  class TestJob(cronExpression: Option[String] = None) extends Job(JobType1, 0, cronExpression) {
    override def run()(implicit context: JobContext): JobExecution = new JobExecution() {
      override def result = Future {
        // just wait a bit...
        blocking(Thread.sleep(50))
      }
      override def cancel: Unit = ()
    }
  }

  private[JobManagerSpec] def newTestException = new RuntimeException("test exception") {
    // suppress the stacktrace to reduce log spam
    override def fillInStackTrace(): Throwable = this
  }
}
