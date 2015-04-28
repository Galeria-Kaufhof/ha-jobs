package de.kaufhof.hajobs

import java.util.UUID

import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Try
import scala.util.control.NonFatal

class JobsController(jobManager: JobManager,
                     jobTypes: JobTypes,
                     // We don't have a reverse router yet, this needs to be supplied by the app
                     reverseRouter: {
                       def status(jobType: String, jobId: String): Call
                     }) extends Controller {

  private val logger = Logger(getClass)

  private def statusUrl(jobType: JobType, jobId: UUID): String = {
    reverseRouter.status(jobType.name, jobId.toString).url
  }

  /**
   * Determines the latest job execution and redirects (temporarily, 307) to its status details url.
   * If no job executions are found 404 is returned.
   */
  def latest(jobTypeString: String): Action[AnyContent] = Action.async {
    jobTypes(jobTypeString).map { jobType =>
      val jobStatusFuture: Future[List[JobStatus]] = jobManager.allJobStatus(jobType)
      jobStatusFuture.map(_.headOption).map {
        case Some(j: JobStatus) =>
          TemporaryRedirect(statusUrl(jobType, j.jobId))
        case None => NotFound
      }
    }.getOrElse(Future.successful(NotFound))
  }

  /**
   * Returns the status details for the given job type and job id.
   */
  def status(jobTypeString: String, jobIdAsString: String): Action[AnyContent] = Action.async {
    Try(UUID.fromString(jobIdAsString)).map { uuid =>
      jobTypes(jobTypeString).map { jobType =>
        val jobStatusFuture: Future[Option[JobStatus]] = jobManager.jobStatus(jobType, uuid)
        jobStatusFuture.map {
          case Some(j: JobStatus) => Ok(Json.toJson(j))
          case None => NotFound
        }
      }.getOrElse(Future.successful(NotFound))
    }.getOrElse(Future.successful(NotFound))
  }

  /**
   * Returns the list of job executions for the given job type.
   */
  def list(jobTypeString: String): Action[AnyContent] = Action.async {
    jobTypes(jobTypeString).map { jobType =>
      val jobStatusFuture: Future[List[JobStatus]] = jobManager.allJobStatus(jobType)
      jobStatusFuture.map { jobs =>
        Ok(Json.obj("jobs" -> jobs, "latest" -> jobs.headOption.map(job => statusUrl(jobType, job.jobId))))
      }
    }.getOrElse(Future.successful(NotFound))
  }

  /**
   * Starts the execution of the given job type.
   */
  def run(jobTypeString: String): Action[AnyContent] = Action.async { implicit request =>
    jobTypes(jobTypeString).map { jobType =>
      jobManager.triggerJob(jobType).map {
        case Started(newJobId, None) =>
          Created(Json.obj("status" -> "OK"))
            .withHeaders(("Location", statusUrl(jobType, newJobId)))

        case Started(newJobId, Some(message)) =>
          Created(Json.obj("status" -> "OK", "message" -> message))
            .withHeaders(("Location", statusUrl(jobType, newJobId)))

        case LockedStatus(runningJobId) =>
          val conflict = Conflict(Json.obj("status" -> "KO", "message" -> s"The job's already running"))
          runningJobId.map(id =>
            conflict.withHeaders(("Location", statusUrl(jobType, id)))
          ).getOrElse(
              conflict
            )

        case Error(message) =>
          InternalServerError(Json.obj("status" -> "KO", "message" -> s"$message"))

      }.recover {
        case NonFatal(e) =>
          logger.error("Error when starting job", e)
          InternalServerError(s"Error when starting job: $e")
      }
    }.getOrElse(Future.successful(NotFound))
  }

}