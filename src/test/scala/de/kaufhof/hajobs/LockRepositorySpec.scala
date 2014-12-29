package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.CassandraSpec
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class LockRepositorySpec extends CassandraSpec {

  private lazy val repo = new LockRepository(session, TestLockTypes)

  override protected def beforeEach(): Unit = {
    await(repo.clear())
    eventually {
      await(repo.getAll()) should be(Seq.empty)
    }
  }

  "LockRepository" must {
    "save and delete lock entries" in {
      await(repo.save(JobType1))
      eventually{
        await(repo.getIdForType(JobType1)) should be ('defined)
      }
      await(repo.delete(JobType1))
      eventually{
        await(repo.getIdForType(JobType1)) should be (None)
      }
    }

    "return all locks" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()

      await(repo.acquireLock(JobType1, uuid))
      await(repo.acquireLock(JobType2, uuid2))

      await(repo.getAll()) should contain allOf (Lock(LockType1, uuid), Lock(LockType2, uuid2))
    }
    
    "allow lock for a single request" in {
      val uuid = UUIDs.timeBased()
      await(repo.acquireLock(JobType1, uuid)) should be (true)
    }

    "not allow lock while another lock is set" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType1, uuid)) should be (true)
      await(repo.acquireLock(JobType1, uuid2)) should be (false)
    }

    "allow another lock, after a lock was released" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType1, uuid)) should be (true)
      await(repo.releaseLock(JobType1, uuid)) should be (true)
      await(repo.acquireLock(JobType1, uuid2)) should be (true)
    }

    "allow another lock, after the specified timeout" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType1, uuid, 1 second)) should be (true)
      Thread.sleep(1000)
      eventually {
        await(repo.acquireLock(JobType1, uuid2)) should be(true)
      }
    }

    "renew a lock" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType1, uuid, 2 seconds)) should be (true)
      updateLockFor(JobType1, uuid, 3 seconds)
      await(repo.updateLock(JobType1, uuid, 2 seconds)) should be (true)
      await(repo.updateLock(JobType1, uuid2, 2 seconds)) should be (false)
    }

    "not renew a lock if it already timed out" in {
      val uuid = UUIDs.timeBased()
      await(repo.acquireLock(JobType1, uuid, 1 second)) should be(true)
      Thread.sleep(1000)
      eventually {
        await(repo.updateLock(JobType1, uuid, 1 second)) should be(false)
      }
    }
  }

  private def updateLockFor(jobType: JobType, jobId: UUID, duration: FiniteDuration) = {
    val start = DateTime.now().getMillis
    while (DateTime.now().getMillis < (start + duration.toMillis)) {
      await(repo.updateLock(jobType, jobId, duration)) should be (true)
      Thread.sleep(50)
    }
  }
}

