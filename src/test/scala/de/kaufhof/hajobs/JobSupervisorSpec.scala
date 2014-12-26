package de.kaufhof.hajobs

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.{StandardSpec, MockInitializers}
import org.joda.time.DateTime
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar.mock
import MockInitializers._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class JobSupervisorSpec extends StandardSpec {

  private val lockRepository = mock[LockRepository]

  private val jobStatusRepository = mock[JobStatusRepository]

  override def beforeEach() {
    initializeLockRepo(lockRepository)
    reset(jobStatusRepository)
  }

  "update job state in JobSupervisor" should {
    val jobStatus = JobStatus(UUIDs.timeBased(), JobType1, UUIDs.timeBased(), JobState.Running, JobResult.Pending, DateTime.now(), None)
    val jobManager = mock[JobManager]

    "change the state of failed jobs to FAILED" in {
      when(lockRepository.getAll()).thenReturn(Future.successful(Seq.empty))
      when(jobStatusRepository.getAllMetadata(anyBoolean())).thenReturn(Future.successful(List(jobStatus)))
      when(jobStatusRepository.updateJobState(any(), any())).thenAnswer(futureIdentityAnswer())

      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val sut = new JobSupervisor(jobManager, jobUpdater, jobStatusRepository)
      var finishCalled = false

      implicit val context = JobContext(UUIDs.timeBased(), UUIDs.timeBased(), () => (finishCalled = true))
      val Started(jobId, _) = await(sut.run())

      // In theory everything is running sequentially (in a single thread), so that when `sut.run()` returns
      // everything else is done, but who knows, perhaps some things (e.g. in MockInitializers) might be
      // changed at some time so that we would get flaky tests.
      // So let's make sure that the last action of the job is done, which is the lock release.
      eventually {
        finishCalled should be (true)
      }
      verify(jobStatusRepository, times(1)).updateJobState(jobStatus, JobState.Dead)
    }

    "not change the state of still running jobs" in {
      when(lockRepository.getAll()).thenReturn(Future.successful(Seq(Lock(jobStatus.jobType.lockType, jobStatus.jobId))))
      when(jobStatusRepository.getAllMetadata()).thenReturn(Future.successful(List(jobStatus)))

      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val sut = new JobSupervisor(jobManager, jobUpdater, jobStatusRepository)
      var finishCalled = false

      implicit val context = JobContext(UUIDs.timeBased(), UUIDs.timeBased(), () => (finishCalled = true))
      val Started(jobId, _) = await(sut.run())

      // We detect the job end based on lock release
      eventually {
        finishCalled should be (true)
      }
      verify(jobStatusRepository, times(0)).updateJobState(any[JobStatus], any())
    }
  }

  "retrigger job in JobSupervisor" should {
    val someJob = mock[Job]
    when(someJob.jobType).thenReturn(JobType1)
    when(someJob.retriggerCount).thenReturn(2)

    val jobManager = mock[JobManager]
    when(jobManager.getJob(any[JobType])).thenReturn(someJob)
    when(jobManager.retriggerJob(any(), any())).thenReturn(Future.successful(Started(UUIDs.timeBased())))

    "do nothing if no JobStatus exist" in {
      when(jobStatusRepository.getAllMetadata()).thenReturn(Future.successful(Nil))
      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val sut = new JobSupervisor(jobManager, jobUpdater, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(0)).retriggerJob(any(), any())
    }

    "do nothing if one job of the last trigger id ended successfully (even if a trigger id earlier failed))" in {
      val job1 = JobStatus(UUIDs.timeBased(), JobType1, UUIDs.timeBased(), JobState.Canceled, JobResult.Failed, DateTime.now.minusMillis(1))
      val job2 = JobStatus(UUIDs.timeBased(), JobType1, UUIDs.timeBased(), JobState.Finished, JobResult.Success, DateTime.now)
      when(jobStatusRepository.getAllMetadata()).thenReturn(Future.successful(List(job1, job2)))
      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val sut = new JobSupervisor(jobManager, jobUpdater, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(0)).retriggerJob(any(), any())
    }

    "retrigger a job if no job of the last trigger was successful and retrigger size is not reached" in {
      val job1 = JobStatus(UUIDs.timeBased(), JobType1, UUIDs.timeBased(), JobState.Canceled, JobResult.Failed, DateTime.now.minusMillis(1))
      when(jobStatusRepository.getAllMetadata()).thenReturn(Future.successful(List(job1)))
      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val sut = new JobSupervisor(jobManager, jobUpdater, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(1)).retriggerJob(job1.jobType, job1.triggerId)
    }

    "do nothing if no job of the last trigger was successful and retrigger size is reached" in {
      val job1 = JobStatus(UUIDs.timeBased(), JobType1, UUIDs.timeBased(), JobState.Canceled, JobResult.Failed, DateTime.now.minusMillis(1))
      val job2 = job1.copy(jobStatusTs = DateTime.now.minusMillis(1))
      val job3 = job1.copy(jobStatusTs = DateTime.now.minusMillis(2))
      val job4 = job1.copy(jobStatusTs = DateTime.now.minusMillis(2))
      when(jobStatusRepository.getAllMetadata()).thenReturn(Future.successful(List(job1, job2, job3, job4)))
      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)
      val sut = new JobSupervisor(jobManager, jobUpdater, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(0)).retriggerJob(job1.jobType, job1.triggerId)
    }
  }
}
