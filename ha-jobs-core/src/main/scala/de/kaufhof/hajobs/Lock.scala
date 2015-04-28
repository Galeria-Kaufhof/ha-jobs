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

class LockTypes(lockTypes: Seq[LockType]) {

  /**
   * Resolves a LockType by name. Compares built in LockTypes and given LockTypes.
   */
  final def apply(name: String): Option[LockType] = (lockTypes :+ JobSupervisorLock).find(_.name == name)

  def all: Seq[LockType] = lockTypes

}

object LockTypes {

  object JobSupervisorLock extends LockType("supervisor")

  def apply(lockTypes: LockType*): LockTypes = new LockTypes(lockTypes)

}