package de.kaufhof.hajobs

import scala.concurrent.{ExecutionContext, Future}

trait StatusWriter {

  /**
    * Saves a JobStatus (to the database or in memory for tests) and returns the saved object.
    */
  def save(jobStatus: JobStatus)(implicit ec: ExecutionContext): Future[JobStatus]

}
