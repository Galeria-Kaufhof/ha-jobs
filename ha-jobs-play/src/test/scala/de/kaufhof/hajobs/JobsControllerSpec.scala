package de.kaufhof.hajobs

import java.util.UUID.randomUUID

import org.joda.time.DateTime
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import play.api.http.MimeTypes
import play.api.libs.json.Json
import play.api.mvc._
import play.api.test.FakeRequest
import play.api.test.Helpers._

import scala.concurrent.Future
import scala.language.postfixOps

class JobsControllerSpec extends WordSpec with BeforeAndAfterEach with Matchers with MockitoSugar {

  private val jobManager = mock[JobManager]

  private val jobType1 = JobType("testJob1", LockType("lock1"))
  private implicit val jobTypes = JobTypes(jobType1)

  override protected def beforeEach(): Unit = {
    reset(jobManager)
    when(jobManager.allJobStatus(jobType1)).thenReturn(Future.successful(List.empty))
    when(jobManager.triggerJob(jobType1)).thenReturn(Future.successful(Started(randomUUID())))
  }

  private def importStatusUrl(jobType: String, jobId: String): String = s"/$jobType/imports/$jobId"

  // We don't have working reverse routes, therefore we're unit testing the controller...
  private val controller = new JobsController(jobManager, JobTypes(jobType1), new {
    def importStatus(jobType: String, jobId: String) = Call("GET", importStatusUrl(jobType, jobId))
  })

  private def run(action: Action[AnyContent]): Future[Result] = action.apply(FakeRequest())

  "JobsController" should {
    "start a job when triggerImport is Called" in {
      val result = run(controller.importTrigger(jobType1.name))

      status(result) should be(CREATED)
      verify(jobManager, times(1)).triggerJob(jobType1)
    }

    "return correct status code and redirect if a job is running" in {
      val someId = randomUUID()
      when(jobManager.triggerJob(jobType1)).thenReturn(Future.successful(LockedStatus(Some(someId))))

      val result = run(controller.importTrigger(jobType1.name))
      status(result) should be(CONFLICT)
      header("Location", result) should be (Some(importStatusUrl(jobType1.name, someId.toString)))
    }

    "return import Location when asked without importId while running" in {
      val someId = randomUUID()
      when(jobManager.allJobStatus(jobType1)).thenReturn(Future.successful(List(JobStatus(randomUUID(),
        jobType1,
        someId,
        JobState.Running,
        JobResult.Pending,
        DateTime.now()
      ))))

      val result = run(controller.importCheck(jobType1.name))
      status(result) should be(TEMPORARY_REDIRECT)
      header("Location", result) should be(Some(importStatusUrl(jobType1.name, someId.toString)))
    }

    "return 404 if a not exisiting job is referenced in url" in {
      val result = run(controller.importCheck("notExisting"))
      status(result) should be(NOT_FOUND)
    }

    "return 404 when idle" in {
      val result = run(controller.importCheck(jobType1.name))
      status(result) should be(NOT_FOUND)
      contentAsString(result) should be(empty)
    }

    "return 200 plus details when running" in {
      val someId = randomUUID()
      when(jobManager.jobStatus(jobType1, someId)).thenReturn(Future.successful(Some(JobStatus(randomUUID(),
        jobType1,
        someId,
        JobState.Running,
        JobResult.Pending,
        DateTime.now()
      ))))

      val result = run(controller.importStatus(jobType1.name, someId.toString))
      status(result) should be(OK)
      contentType(result) should be(Some(MimeTypes.JSON))

      Json.fromJson[JobStatus](contentAsJson(result)).get.jobState should equal(JobState.Running)
    }
  }
}