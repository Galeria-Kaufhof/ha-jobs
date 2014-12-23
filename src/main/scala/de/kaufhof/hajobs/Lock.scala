package de.kaufhof.hajobs

import java.util.{NoSuchElementException, UUID}

import play.api.Logger

/**
 * A Lock exists for jobType and jobId.
 */
case class Lock(lockType: LockType, jobId: UUID)

/**
 * A LockType describes a lock used by a JobType (e.g. JobType(stockFeed) can
 * reference a LockType(stock)).
 */
sealed trait LockType {
  def name: String
}

object LockType {

  private val logger = Logger(classOf[LockType])

  abstract class NamedLockType(override val name: String) extends LockType
  case object JobSupervisor extends NamedLockType("supervisor")
  case object Product extends NamedLockType("products")
  case object Stoerer extends NamedLockType("stoerer")
  case object Kpi extends NamedLockType("kpi")
  case object Dictionary extends NamedLockType("dictionary")
  case object Stock extends NamedLockType("stock")

  /**
   * Resolves a LockType by name. Throws a NoSuchElementException if there's no LockType
   * with the given name. In this case exception is preferred over returning an Option to be
   * more conformant with the JobStatus enum type hierarchy.
   */
  @throws[NoSuchElementException]
  def withName(name: String): LockType = name match {
    case JobSupervisor.name => JobSupervisor
    case Product.name => Product
    case Stoerer.name => Stoerer
    case Kpi.name => Kpi
    case Dictionary.name => Dictionary
    case Stock.name => Stock
    case _ =>
      throw new NoSuchElementException(s"Could not find LockType with name '$name'.")
  }
}