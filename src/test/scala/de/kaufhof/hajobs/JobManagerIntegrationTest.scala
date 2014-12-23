package de.kaufhof.hajobs

import java.util.concurrent.CountDownLatch

import de.kaufhof.hajobs.testutils.CassandraSpec
import play.api.test._

import scala.concurrent.Future
import scala.language.postfixOps

class JobManagerIntegrationTest extends CassandraSpec with DefaultAwaitTimeout with FutureAwaits {
  private lazy val jobStatusRepository = new JobStatusRepository(session)

  private lazy val lockRepository = new LockRepository(session)

  "JobManager locking" should {
    // TODO: Too flaky on deployment pipeline, whats the problem
    "should prevent StockSnapshotImport and StockFeedImport from running in parallel" ignore {
/*      val cdl = new CountDownLatch(1);
      val mockedScheduler = mock[Scheduler]

      val manager = new JobManager(Seq(new StockFeedJob(jobStatusRepository), new StockSnapshotJob(jobStatusRepository, cdl)), lockRepository, mock[ActorSystem], mockedScheduler, false)
      manager.triggerJob(JobType.StockSnapshotImporter)
      await(manager.triggerJob(JobType.StockFeedImporter)) should be (a[LockedStatus])
      cdl.countDown() */
    }
  }
}

object JobManagerIntegrationTest {
  class StockSnapshotJob(jobStatusRepository: JobStatusRepository, cdl: CountDownLatch) extends Job(JobType.StockSnapshotImporter, jobStatusRepository, 3) {
    override def run()(implicit context: JobContext): Future[JobStartStatus] = {
      cdl.await()
      Future.successful(Started(context.jobId))
    }

    override def cancel(): Unit = ???
  }

  class StockFeedJob(jobStatusRepository: JobStatusRepository) extends Job(JobType.StockFeedImporter, jobStatusRepository, 3) {
    override def run()(implicit context: JobContext): Future[JobStartStatus] = {
      Future.successful(Started(context.jobId))
    }

    override def cancel(): Unit = ???
  }
}