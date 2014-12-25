package de.kaufhof.hajobs

import com.datastax.driver.core.Session
import scaldi.Module

class JobRepositoryModule(jobTypes: JobTypes, lockTypes: LockTypes) extends Module {

  protected def jobStatusRepo: JobStatusRepository = new JobStatusRepository(inject[Session], jobTypes = jobTypes)
  protected def lockRepo: LockRepository = new LockRepository(inject[Session], lockTypes)

  binding to jobStatusRepo
  binding to lockRepo

}