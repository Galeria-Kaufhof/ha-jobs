package de.kaufhof.hajobs

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.StandardSpec
import org.joda.time.DateTime
import org.mockito.Matchers._
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JobUpdaterSpec extends StandardSpec {
  private val lockRepository = mock[LockRepository]

  private val jobStatusRepository = mock[JobStatusRepository]

  "update jobs in JobUpdater" should {
    val job = JobStatus(UUIDs.timeBased(), JobType1, UUIDs.timeBased(), JobState.Running, JobResult.Pending, DateTime.now(), None)

    "set jobStatus of dead jobs to dead" in {
      when(lockRepository.getAll()).thenReturn(Future.successful(Seq.empty))
      when(jobStatusRepository.getLatestMetadata(anyBoolean())(any())).thenReturn(Future.successful(List(job)))
      when(jobStatusRepository.updateJobState(any(), any())(any())).thenReturn(Future.successful(job))

      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)

      await(jobUpdater.updateJobs())
      eventually {
        verify(jobStatusRepository, times(1)).updateJobState(job, JobState.Dead)
      }
    }
  }
}
