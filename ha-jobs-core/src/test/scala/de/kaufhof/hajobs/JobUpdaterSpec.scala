package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.StandardSpec
import org.joda.time.DateTime
import org.mockito.Matchers._
import org.mockito.Mockito._
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JobUpdaterSpec extends StandardSpec {
  private val lockRepository = mock[LockRepository]

  private val jobStatusRepository = mock[JobStatusRepository]

  "update jobs in JobUpdater" should {
    val jobId: UUID = UUIDs.timeBased()
    val triggerId: UUID = UUIDs.timeBased()
    val job = JobStatus(jobId, JobType1, triggerId, JobState.Running, JobResult.Pending, DateTime.now(), None)
    val jobWithData = JobStatus(jobId, JobType1, triggerId, JobState.Running, JobResult.Pending, DateTime.now(), Some(Json.toJson("test")))

    "set jobStatus of dead jobs to dead" in {
      when(lockRepository.getAll()).thenReturn(Future.successful(Seq.empty))
      val successful: Future[Map[JobType, List[JobStatus]]] = Future.successful(Map(JobType1 -> List(job)))
      when(jobStatusRepository.getAllMetadata(anyBoolean())(any())).thenReturn(successful)
      when(jobStatusRepository.updateJobState(any(), any())(any())).thenReturn(Future.successful(jobWithData))
      when(jobStatusRepository.get(any(), any(), anyBoolean())(any())).thenReturn(Future.successful(Some(jobWithData)))

      val jobUpdater = new JobUpdater(lockRepository, jobStatusRepository)

      await(jobUpdater.updateJobs())
      eventually {
        verify(jobStatusRepository, times(1)).updateJobState(jobWithData, JobState.Dead)
      }
    }
  }
}
