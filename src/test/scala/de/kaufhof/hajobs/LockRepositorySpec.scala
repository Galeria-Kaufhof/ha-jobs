package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.CassandraSpec
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.language.postfixOps

class LockRepositorySpec extends CassandraSpec {

  private lazy val repo = new LockRepository(session)

  override protected def beforeEach(): Unit = await(repo.clear())

  "LockRepository" ignore {
    "save and delete lock entries" in {
      await(repo.getAll()) should be(Seq.empty)
      await(repo.save(JobType.KpiImporter))
      eventually{
        await(repo.getIdForType(JobType.KpiImporter)) should not be (None)
      }
      await(repo.delete(JobType.KpiImporter))
      eventually{
        await(repo.getIdForType(JobType.KpiImporter)) should be (None)
      }
    }

    "return all locks" in {
      await(repo.getAll()) should be(Seq.empty)
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()

      await(repo.acquireLock(JobType.KpiImporter, uuid))
      await(repo.acquireLock(JobType.ProductImporter, uuid2))

      await(repo.getAll()) should contain allOf (Lock(LockType.Kpi, uuid), Lock(LockType.Product, uuid2))
    }
    
    "allow lock for a single request" in {
      val uuid = UUIDs.timeBased()
      await(repo.acquireLock(JobType.KpiImporter, uuid)) should be (true)
    }

    "not allow lock while another lock is set" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType.KpiImporter, uuid)) should be (true)
      await(repo.acquireLock(JobType.KpiImporter, uuid2)) should be (false)
    }

    "allow another lock, after a lock was released" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType.KpiImporter, uuid)) should be (true)
      await(repo.releaseLock(JobType.KpiImporter, uuid)) should be (true)
      await(repo.acquireLock(JobType.KpiImporter, uuid2)) should be (true)
    }

    "allow another lock, after the specified timeout" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType.KpiImporter, uuid, 1 second)) should be (true)
      Thread.sleep(1000)
      eventually {
        await(repo.acquireLock(JobType.KpiImporter, uuid2)) should be(true)
      }
    }

    "renew a lock" in {
      val uuid = UUIDs.timeBased()
      val uuid2 = UUIDs.timeBased()
      await(repo.acquireLock(JobType.KpiImporter, uuid, 2 seconds)) should be (true)
      updateLockFor(JobType.KpiImporter, uuid, 3 seconds)
      await(repo.updateLock(JobType.KpiImporter, uuid, 2 seconds)) should be (true)
      await(repo.updateLock(JobType.KpiImporter, uuid2, 2 seconds)) should be (false)
    }

    "not renew a lock if it already timed out" in {
      val uuid = UUIDs.timeBased()
      await(repo.acquireLock(JobType.StoererImporter, uuid, 1 second)) should be(true)
      Thread.sleep(1000)
      eventually {
        await(repo.updateLock(JobType.StoererImporter, uuid, 1 second)) should be(false)
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

