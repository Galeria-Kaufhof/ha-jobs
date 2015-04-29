package de.kaufhof.hajobs

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.{StandardSpec, MockInitializers}
import org.joda.time.DateTime
import org.mockito.Matchers._
import org.mockito.Mockito._
import MockInitializers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JobSupervisorSpec extends StandardSpec {

  private val lockRepository = mock[LockRepository]

  private val jobStatusRepository = mock[JobStatusRepository]

  override def beforeEach() {
    initializeLockRepo(lockRepository)
    reset(jobStatusRepository)
  }

  "update job state in JobSupervisor" should {
    val jobStatus = JobStatus(UUIDs.timeBased(), JobTypes.JobSupervisor, UUIDs.timeBased(), JobState.Running, JobResult.Pending, DateTime.now(), None)
    val jobManager = mock[JobManager]

    "change the state of failed jobs to FAILED" in {
      when(lockRepository.getAll()(any())).thenReturn(Future.successful(Seq.empty))
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(List(jobStatus)))
      when(jobStatusRepository.updateJobState(any(), any())(any())).thenAnswer(futureIdentityAnswer())

      val sut = new JobSupervisor(jobManager, lockRepository, jobStatusRepository)

      implicit val context = JobContext(JobTypes.JobSupervisor, UUIDs.timeBased(), UUIDs.timeBased())
      val jobExecution = sut.run()
      await(jobExecution.result)

      verify(jobStatusRepository, times(1)).updateJobState(jobStatus, JobState.Dead)
    }

    "not change the state of still running jobs" in {
      when(lockRepository.getAll()(any())).thenReturn(Future.successful(Seq(Lock(jobStatus.jobType.lockType, jobStatus.jobId))))
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(List(jobStatus)))

      val sut = new JobSupervisor(jobManager, lockRepository, jobStatusRepository)

      implicit val context = JobContext(JobTypes.JobSupervisor, UUIDs.timeBased(), UUIDs.timeBased())
      val jobExecution = sut.run()
      await(jobExecution.result)

      verify(jobStatusRepository, times(0)).updateJobState(any[JobStatus], any())(any())
    }
  }

  "retrigger job in JobSupervisor" should {
    val someJob = mock[Job]
    when(someJob.jobType).thenReturn(JobTypes.JobSupervisor)
    when(someJob.retriggerCount).thenReturn(2)

    val jobManager = mock[JobManager]
    when(jobManager.getJob(any[JobType])).thenReturn(someJob)
    when(jobManager.retriggerJob(any(), any())).thenReturn(Future.successful(Started(UUIDs.timeBased())))

    "do nothing if no JobStatus exist" in {
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(Nil))
      val sut = new JobSupervisor(jobManager, lockRepository, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(0)).retriggerJob(any(), any())
    }

    "do nothing if one job of the last trigger id ended successfully (even if a trigger id earlier failed))" in {
      val job1 = JobStatus(UUIDs.timeBased(), JobTypes.JobSupervisor, UUIDs.timeBased(), JobState.Canceled, JobResult.Failed, DateTime.now.minusMillis(1))
      val job2 = JobStatus(UUIDs.timeBased(), JobTypes.JobSupervisor, UUIDs.timeBased(), JobState.Finished, JobResult.Success, DateTime.now)
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(List(job1, job2)))
      val sut = new JobSupervisor(jobManager, lockRepository, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(0)).retriggerJob(any(), any())
    }

    "retrigger a job if no job of the last trigger was successful and retrigger size is not reached" in {
      val job1 = JobStatus(UUIDs.timeBased(), JobTypes.JobSupervisor, UUIDs.timeBased(), JobState.Canceled, JobResult.Failed, DateTime.now.minusMillis(1))
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(List(job1)))
      val sut = new JobSupervisor(jobManager, lockRepository, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(1)).retriggerJob(job1.jobType, job1.triggerId)
    }

    "do nothing if no job of the last trigger was successful and retrigger size is reached" in {
      val job1 = JobStatus(UUIDs.timeBased(), JobTypes.JobSupervisor, UUIDs.timeBased(), JobState.Canceled, JobResult.Failed, DateTime.now.minusMillis(1))
      val job2 = job1.copy(jobStatusTs = DateTime.now.minusMillis(1))
      val job3 = job1.copy(jobStatusTs = DateTime.now.minusMillis(2))
      val job4 = job1.copy(jobStatusTs = DateTime.now.minusMillis(2))
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(List(job1, job2, job3, job4)))
      val sut = new JobSupervisor(jobManager, lockRepository, jobStatusRepository)
      await(sut.retriggerJobs())
      verify(jobManager, times(0)).retriggerJob(job1.jobType, job1.triggerId)
    }
  }
}
