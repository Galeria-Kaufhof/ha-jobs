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
                       def importStatus(jobType: String, importId: String): Call
                     }) extends Controller {

  private val logger = Logger(getClass)

  private def statusUrl(jobType: JobType, jobId: UUID): String = {
    reverseRouter.importStatus(jobType.name, jobId.toString).url
  }

  def importCheck(jobTypeString: String): Action[AnyContent] = Action.async {
    stringToJobType(jobTypeString).map { jobType =>
      val jobStatusFuture: Future[List[JobStatus]] = jobManager.allJobStatus(jobType)
      jobStatusFuture.map(_.headOption).map {
        case Some(j: JobStatus) =>
          TemporaryRedirect(statusUrl(jobType, j.jobId))
        case None => NotFound
      }
    }.getOrElse(Future.successful(NotFound))
  }

  def importStatus(jobTypeString: String, importIdAsString: String): Action[AnyContent] = Action.async {
    Try(UUID.fromString(importIdAsString)).map { uuid =>
      stringToJobType(jobTypeString).map { jobType =>
        val jobStatusFuture: Future[Option[JobStatus]] = jobManager.jobStatus(jobType, uuid)
        jobStatusFuture.map {
          case Some(j: JobStatus) => Ok(Json.toJson(j))
          case None => NotFound
        }
      }.getOrElse(Future.successful(NotFound))
    }.getOrElse(Future.successful(NotFound))
  }

  def importList(jobTypeString: String): Action[AnyContent] = Action.async {
    stringToJobType(jobTypeString).map { jobType =>
      val jobStatusFuture: Future[List[JobStatus]] = jobManager.allJobStatus(jobType)
      jobStatusFuture.map { jobs =>
        Ok(Json.obj("jobs" -> jobs, "latest" -> jobs.headOption.map(job => statusUrl(jobType, job.jobId))))
      }
    }.getOrElse(Future.successful(NotFound))
  }

  def importTrigger(jobTypeString: String): Action[AnyContent] = Action.async { implicit request =>
    stringToJobType(jobTypeString).map { jobType =>
      jobManager.triggerJob(jobType).map {
        case Started(newJobId, None) =>
          Created(Json.obj("status" -> "OK"))
            .withHeaders(("Location", statusUrl(jobType, newJobId)))

        case Started(newJobId, Some(message)) =>
          Created(Json.obj("status" -> "OK", "message" -> message))
            .withHeaders(("Location", statusUrl(jobType, newJobId)))

        case LockedStatus(runningJobId) =>
          val conflict = Conflict(Json.obj("status" -> "KO", "message" -> s"There's already an import running"))
          runningJobId.map(id =>
            conflict.withHeaders(("Location", statusUrl(jobType, id)))
          ).getOrElse(
              conflict
            )

        case Error(message) =>
          InternalServerError(Json.obj("status" -> "KO", "message" -> s"$message"))

      }.recover {
        case NonFatal(e) =>
          logger.error("error when starting importer", e)
          InternalServerError(s"error when starting importer: $e")
      }
    }.getOrElse(Future.successful(NotFound))
  }

  private def stringToJobType(str: String) = Try(Some(jobTypes(str))).getOrElse(None)
}