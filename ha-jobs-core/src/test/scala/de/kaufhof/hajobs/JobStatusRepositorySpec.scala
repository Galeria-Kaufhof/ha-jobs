package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.CassandraSpec
import JobState._
import org.joda.time.DateTime
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class JobStatusRepositorySpec extends CassandraSpec {

  private lazy val repo = new JobStatusRepository(session, ttl = 1.minute, TestJobTypes)
  private val type1 = JobType1
  private val type2 = JobType2
  private val anyTriggerId = UUIDs.timeBased()

  override protected def beforeEach(): Unit = {
    await(repo.clear)
  }

  "job status repository" should {

    "get a job status by id" in {
      assume(await(repo.getLatestMetadata()) === List.empty)
      // given
      val jobStatus1: JobStatus = JobStatus(anyTriggerId, type1, UUIDs.timeBased(), Finished, JobResult.Success, DateTime.now, Some(Json.toJson("muhmuh")))
      // a job status without content should also be loaded
      val jobStatus2: JobStatus = JobStatus(anyTriggerId, type1, UUIDs.timeBased(), Finished, JobResult.Success, DateTime.now)

      // when
      await(Future.sequence(Seq(repo.save(jobStatus1), repo.save(jobStatus2))))

      // then
      eventually {
        await(repo.get(type1, jobStatus1.jobId)) should be(Some(jobStatus1))
        await(repo.get(type1, jobStatus2.jobId)) should be(Some(jobStatus2))
      }
    }

    "return empty list if nothing is saved in the repo" in {
      assume(await(repo.getLatestMetadata()) === List.empty)
      await(repo.list(type1)) should be(Nil)
    }

    "list Job status if job status is saved in the repo" in {
      assume(await(repo.getLatestMetadata()) === List.empty)
      // given
      val jobId1: UUID = UUIDs.timeBased()
      val jobId2: UUID = UUIDs.timeBased()
      val jobStatus1: JobStatus = JobStatus(anyTriggerId, type1, jobId1, Finished, JobResult.Failed, DateTime.now, Some(Json.toJson("muhmuh")))
      val jobStatus2: JobStatus = JobStatus(anyTriggerId, type1, jobId2, Finished, JobResult.Failed, DateTime.now, Some(Json.toJson("muhmuh")))

      // when
      await(Future.sequence(Seq(repo.save(jobStatus1), repo.save(jobStatus2))))

      // then
      val jobList: List[JobStatus] = await(repo.list(type1))
      assert(jobList != Nil)
      assert(jobList.size == 2)
    }

    "return all JobStates on listAll" in {
      assume(await(repo.getLatestMetadata()) === List.empty)
      val jobId1: UUID = UUIDs.timeBased()
      val jobId2: UUID = UUIDs.timeBased()
      val jobStatus1: JobStatus = JobStatus(anyTriggerId, type1, jobId1, Running, JobResult.Pending, DateTime.now, Some(Json.toJson("muhmuh")))
      val jobStatus2: JobStatus = JobStatus(anyTriggerId, type2, jobId2, Running, JobResult.Pending, DateTime.now, Some(Json.toJson("muhmuh")))

      // when
      await(Future.sequence(Seq(repo.save(jobStatus1), repo.save(jobStatus2))))

      eventually {
        val lst = await(repo.getLatestMetadata())
        lst.size should be(2)
        lst.map(_.jobType) should contain allOf(type1, type2)
      }
    }

    "return all latest JobStatus Metadata on getAllMetadata" in {
      assume(await(repo.getMetadata(limitByJobType = _ => 10)).flatMap(_._2) === List.empty)
      val jobId1: UUID = UUIDs.startOf(DateTime.now.minusMillis(5000).getMillis)
      val jobId2: UUID = UUIDs.startOf(DateTime.now.minusMillis(3000).getMillis)
      val jobId3: UUID = UUIDs.startOf(DateTime.now.minusMillis(1000).getMillis)
      val jobId4: UUID = UUIDs.startOf(DateTime.now.minusMillis(1000).getMillis)
      val jobId5: UUID = UUIDs.startOf(DateTime.now.minusMillis(0).getMillis)
      val jobId6: UUID = UUIDs.startOf(DateTime.now.minusMillis(2000).getMillis)
      val jobStatus1: JobStatus = JobStatus(anyTriggerId, type1, jobId1, Failed, JobResult.Failed, DateTime.now.minusMillis(5000), Some(Json.toJson("muhmuh1")))
      val jobStatus2: JobStatus = JobStatus(anyTriggerId, type1, jobId2, Failed, JobResult.Failed, DateTime.now.minusMillis(1000), Some(Json.toJson("muhmuh2")))
      val jobStatus3: JobStatus = JobStatus(anyTriggerId, type1, jobId3, Failed, JobResult.Failed, DateTime.now.minusMillis(3000), Some(Json.toJson("muhmuh3")))
      val jobStatus4: JobStatus = JobStatus(anyTriggerId, type2, jobId4, Failed, JobResult.Failed, DateTime.now.minusMillis(0), Some(Json.toJson("muhmuh4")))
      val jobStatus5: JobStatus = JobStatus(anyTriggerId, type2, jobId5, Failed, JobResult.Failed, DateTime.now.minusMillis(1000), Some(Json.toJson("muhmuh5")))
      val jobStatus6: JobStatus = JobStatus(anyTriggerId, type2, jobId6, Failed, JobResult.Failed, DateTime.now.minusMillis(2000), Some(Json.toJson("muhmuh6")))

      // when
      await(Future.sequence(Seq(repo.save(jobStatus1), repo.save(jobStatus2), repo.save(jobStatus3), repo.save(jobStatus4), repo.save(jobStatus5), repo.save(jobStatus6))))

      eventually {
        val map = await(repo.getMetadata(limitByJobType = _ => 2))
        map.size should be(2)
        map.flatMap(_._2).size should be(4)
        map.flatMap(_._2).map(_.jobId).toList should contain theSameElementsInOrderAs List(jobId2, jobId3, jobId4, jobId5)
      }
    }

    "update meta data and insert data on update" in {
      assume(await(repo.getLatestMetadata()) === List.empty)
      val jobId1: UUID = UUIDs.timeBased()
      val jobStatus1: JobStatus = JobStatus(anyTriggerId, type1, jobId1, Finished, JobResult.Failed, DateTime.now, Some(Json.toJson("muhmuh")))

      // when
      await(repo.save(jobStatus1))
      eventually{
        await(repo.getLatestMetadata()).find(_.jobId == jobId1).map(_.jobState).value should be (jobStatus1.jobState)
      }

      val jobStatus2 = await(repo.updateJobState(jobStatus1, Canceled))
      eventually{
        await(repo.getLatestMetadata()).find(_.jobId == jobId1).map(_.jobState).value should be (jobStatus2.jobState)
      }

      eventually {
        await(repo.getJobHistory(type1, jobId1)).map(_.jobState) should contain theSameElementsAs Seq(Canceled, Finished)
      }
    }
  }
}
