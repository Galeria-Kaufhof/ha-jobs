package de.kaufhof.hajobs

import com.datastax.driver.core.Session
import scaldi.Module

class JobRepositoryModule extends Module {

  protected def jobStatusRepo: JobStatusRepository = new JobStatusRepository(inject[Session])
  protected def lockRepo: LockRepository = new LockRepository(inject[Session])

  binding to jobStatusRepo
  binding to lockRepo

}