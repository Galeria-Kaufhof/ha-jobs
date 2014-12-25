package de.kaufhof.hajobs

object LockType1 extends LockType("testLock1")
object LockType2 extends LockType("testLock2")

object TestLockTypes extends LockTypes {
  override protected def byName: PartialFunction[String, LockType] = {
    case LockType1.name => LockType1
    case LockType2.name => LockType2
  }
}

object JobType1 extends JobType("testJob1", LockType1)
object JobType2 extends JobType("testJob2", LockType2)

object TestJobTypes extends JobTypes {
  override protected def byName: PartialFunction[String, JobType] = {
    case JobType1.name => JobType1
    case JobType2.name => JobType2
  }
}