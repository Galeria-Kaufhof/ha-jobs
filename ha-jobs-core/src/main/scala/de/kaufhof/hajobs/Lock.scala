package de.kaufhof.hajobs

import java.util.UUID

import de.kaufhof.hajobs.LockTypes.JobSupervisorLock

/**
 * A Lock exists for jobType and jobId.
 */
case class Lock(lockType: LockType, jobId: UUID)

/**
 * A LockType describes a lock used by a JobType (e.g. JobType(stockFeed) can
 * reference a LockType(stock)).
 */
case class LockType(name: String)

case class LockTypes(all: Iterable[LockType]) {

  private val _all = all.toSeq :+ JobSupervisorLock

  /**
   * Resolves a LockType by name. Compares built in LockTypes and given LockTypes.
   */
  final def apply(name: String): Option[LockType] = _all.find(_.name == name)

}

object LockTypes {

  object JobSupervisorLock extends LockType("supervisor")

  def apply(lockTypes: LockType*): LockTypes = new LockTypes(lockTypes)

}