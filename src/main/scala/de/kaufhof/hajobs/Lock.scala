package de.kaufhof.hajobs

import java.util.{NoSuchElementException, UUID}

/**
 * A Lock exists for jobType and jobId.
 */
case class Lock(lockType: LockType, jobId: UUID)

/**
 * A LockType describes a lock used by a JobType (e.g. JobType(stockFeed) can
 * reference a LockType(stock)).
 */
case class LockType(name: String)

trait LockTypes {

  import LockTypes._

  /**
   * Resolves a LockType by name. Compares built in LockTypes and if none matched
   * delegates to [[byName]].
   *
   * Throws a NoSuchElementException if there's no LockType
   * with the given name. In this case exception is preferred over returning an Option to be
   * more conformant with the JobStatus enum type hierarchy.
   */
  @throws[NoSuchElementException]
  final def apply(name: String): LockType = lookup(name)

  private def lookup: PartialFunction[String, LockType] = byName orElse {
    case JobSupervisorLock.name => JobSupervisorLock
    case unknown => throw new NoSuchElementException(s"Could not find LockType with name '$unknown'.")
  }

  /**
   * Resolves a LockType by name.
   */
  protected def byName: PartialFunction[String, LockType]

}

object LockTypes {

  object JobSupervisorLock extends LockType("supervisor")

  def apply(lockTypes: LockType*): LockTypes = new LockTypes {
    private val lockTypesByName = lockTypes.map(lockType => lockType.name -> lockType).toMap
    override protected def byName: PartialFunction[String, LockType] = lockTypesByName
  }

}