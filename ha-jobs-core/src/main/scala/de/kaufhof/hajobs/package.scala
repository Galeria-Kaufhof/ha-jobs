package de.kaufhof

import org.slf4j.LoggerFactory._

import scala.concurrent.{ExecutionContext, Future}

package object hajobs {

  private val logger = getLogger("de.kaufhof.hajobs")

  type Jobs = Map[JobType, Job]

  // scalastyle:off method.name We allow an upper case method name to mimic a Jobs apply method
  def Jobs(jobs: Seq[Job]): Jobs = jobs.map(s => s.jobType -> s).toMap
  // scalastyle:on

  /**
    * Tries function max n times.
    */
  final def retry[T](n: Int, description: String)(fn: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    fn.recoverWith {
      case e if n > 1 =>
        logger.warn(s"$description failed, trying again (${n - 1} attempts left)", e)
        retry(n - 1, description)(fn)
      case e: Throwable =>
        logger.warn(s"All retries for $description failed, returning error.", e)
        Future.failed[T](e)
    }
  }

}
