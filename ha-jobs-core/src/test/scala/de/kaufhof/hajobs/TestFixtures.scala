package de.kaufhof.hajobs

object LockType1 extends LockType("testLock1")
object LockType2 extends LockType("testLock2")

object TestLockTypes extends LockTypes(List(LockType1, LockType2))

object JobType1 extends JobType("testJob1", LockType1)
object JobType2 extends JobType("testJob2", LockType2)

object TestJobTypes extends JobTypes(List(JobType1, JobType2))
