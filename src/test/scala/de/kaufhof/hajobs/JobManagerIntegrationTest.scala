package de.kaufhof.hajobs

import java.util.concurrent.CountDownLatch

import de.kaufhof.hajobs.testutils.CassandraSpec
import play.api.test._

import scala.concurrent.Future
import scala.language.postfixOps

class JobManagerIntegrationTest extends CassandraSpec with DefaultAwaitTimeout with FutureAwaits {
  private lazy val jobStatusRepository = new JobStatusRepository(session, jobTypes = JobManagerIntegrationTest.TestJobTypes)

  private lazy val lockRepository = new LockRepository(session, TestLockTypes)

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
  class Job1(jobStatusRepository: JobStatusRepository, cdl: CountDownLatch) extends Job(JobType1, jobStatusRepository, 3) {
    override def run()(implicit context: JobContext): Future[JobStartStatus] = {
      cdl.await()
      Future.successful(Started(context.jobId))
    }

    override def cancel(): Unit = ???
  }

  object JobType12 extends JobType("testJob12", JobType1.lockType)

  class Job2(jobStatusRepository: JobStatusRepository) extends Job(JobType12, jobStatusRepository, 3) {
    override def run()(implicit context: JobContext): Future[JobStartStatus] = {
      Future.successful(Started(context.jobId))
    }

    override def cancel(): Unit = ???
  }

  object TestJobTypes extends JobTypes {
    override protected def byName: PartialFunction[String, JobType] = {
      case JobType1.name => JobType1
      case JobType12.name => JobType12
    }
  }
}