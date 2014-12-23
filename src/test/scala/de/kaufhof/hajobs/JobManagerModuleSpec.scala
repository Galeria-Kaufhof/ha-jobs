package de.kaufhof.hajobs

import de.kaufhof.hajobs.testutils.StandardSpec
import org.scalatest.mock.MockitoSugar

class JobManagerModuleSpec extends StandardSpec with MockitoSugar {

  /*
  private object TestModule extends Module {
    val configMock = mock[Configuration]
    Mockito.when(configMock.getBoolean(anyString())).thenReturn(Some(true))
    binding to configMock
    bind [JobStatusRepository] to mock[JobStatusRepository]
    bind [LockRepository] to mock[LockRepository]
    bind [RankingImportService] to mock[RankingImportService]
    bind [SpellingDictionaryImportService] to mock[SpellingDictionaryImportService]
    bind [DumpImportService] identifiedBy 'stoerer to mock[DumpImportService]
    bind [DumpImportService] identifiedBy 'product to mock[DumpImportService]
    bind [StockFeedImportService] to mock[StockFeedImportService]
    bind [DumpImportService] identifiedBy 'stock to mock[DumpImportService]
  }

  private class TestJobManagerModule extends JobManagerModuleImpl(mock[JobRepositoryModule], mock[RankingImportModule],
    mock[SpellingDictionaryImportModule],
    mock[StoererImportModule],
    mock[ProductImportModule],
    mock[StockSnapshotImportModule],
    mock[StockFeedImportModule]) {
    override protected def actorSystem = mock[ActorSystem]

    override protected def scheduler = mock[Scheduler]
  }

  "JobManagerModule" should {
    "provide JobManager with JobSupervisor" in {
      implicit val injector = (TestModule :: new TestJobManagerModule).injector
      import scaldi.Injectable._
      val jobManager = Option(inject[JobManager])
      jobManager should be ('defined)

      jobManager.get.getJob(JobType.JobSupervisor) should not be (null)
      inject[Scheduler].shutdown(false)
    }
  }
  */
}

