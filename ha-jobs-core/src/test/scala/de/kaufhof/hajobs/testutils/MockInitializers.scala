package de.kaufhof.hajobs.testutils

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.{JobStatus, JobStatusRepository, JobType, LockRepository}
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.quartz.Scheduler
import org.scalatest.mock.MockitoSugar.mock

import scala.concurrent.Future
import scala.concurrent.duration.Duration

object MockInitializers {
  def initializeScheduler = mock[Scheduler]

  def initializeLockRepo(): LockRepository = initializeLockRepo(mock[LockRepository])

  def initializeLockRepo(lockRepo: LockRepository): LockRepository = {
    Mockito.reset(lockRepo)
    when(lockRepo.acquireLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.successful(true))
    when(lockRepo.updateLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.successful(true))
    when(lockRepo.getIdForType(any[JobType])(any())).thenReturn(Future.successful(Some(UUIDs.timeBased())))
    lockRepo
  }

  def initJobStatusRepoMock(): JobStatusRepository = initJobStatusRepoMock(mock[JobStatusRepository])

  def initJobStatusRepoMock(jobStatusRepo: JobStatusRepository): JobStatusRepository = {
    reset(jobStatusRepo)

    @volatile
    var tempJobStatus: JobStatus = null

    doAnswer(new Answer[Future[JobStatus]] {
      override def answer(invocation: InvocationOnMock): Future[JobStatus] = {
        val args: Array[AnyRef] = invocation.getArguments
        tempJobStatus = args(0).asInstanceOf[JobStatus]
        Future.successful(tempJobStatus)
      }
    }).when(jobStatusRepo).save(any())(any())

    when(jobStatusRepo.updateJobState(any(), any())(any())).thenAnswer(futureIdentityAnswer())

    when(jobStatusRepo.list(any[JobType], anyInt(), anyBoolean())(any())).thenAnswer(new Answer[Future[List[JobStatus]]] {

      override def answer(invocation: InvocationOnMock) = Future.successful(if (tempJobStatus == null) Nil else List(tempJobStatus))
    })
    when(jobStatusRepo.get(any[JobType], any(), anyBoolean())(any())).thenAnswer(new Answer[Future[Option[JobStatus]]] {
      override def answer(invocation: InvocationOnMock) = Future.successful(Option(tempJobStatus))
    })

    jobStatusRepo
  }

  def futureIdentityAnswer[T](): Answer[Future[T]] = {
    new Answer[Future[T]] {
      override def answer(invocation: InvocationOnMock) = {
        val args = invocation.getArguments
        Future.successful(args(0).asInstanceOf[T])
      }
    }
  }
}