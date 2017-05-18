package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.JobExecutorSpec.{ImmediateJob, PromiseJob}
import de.kaufhof.hajobs.testutils.ActorSpec
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito._
import org.scalatest.OptionValues._
import org.scalatest.concurrent.Eventually._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps

class JobExecutorSpec extends ActorSpec("JobExecutorSpec") {

  private val lockRepository = mock[LockRepository]
  when(lockRepository.acquireLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.successful(true))
  when(lockRepository.updateLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.successful(true))

  private val triggerId = UUIDs.timeBased()

  import JobExecutor._

  "An JobExecutor actor" must {

    "execute a job if lock was acquired" in {

      aut = system.actorOf(JobExecutor.props(lockRepository), "JobExecutor1")

      val job = ImmediateJob()
      aut ! Execute(job, triggerId)

      val started = expectMsgType[Started]

      val jobId = started.jobId

      verify(lockRepository).acquireLock(meq(job.jobType), meq(jobId), meq(job.lockTimeout))(any[ExecutionContext])

      // if lastContext is defined we know our job was executed
      job.lastContext.map(_.jobId).value shouldBe jobId

      eventually {
        verify(lockRepository).releaseLock(meq(job.jobType), meq(jobId))(any[ExecutionContext])
      }
    }

    "handle lost lock and allow to run another job afterwards" in {

      aut = system.actorOf(JobExecutor.props(lockRepository), "JobExecutor2")

      val job = PromiseJob()
      aut ! Execute(job, triggerId)

      val started = expectMsgType[Started]
      val jobId = started.jobId

      when(lockRepository.updateLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.successful(false))

      eventually {
        job.canceled shouldBe true
      }

      // the job should be executable once more
      aut ! Execute(job, triggerId)

      // When the job could be Started we're happy
      expectMsgType[Started]

    }

  }

}

object JobExecutorSpec {

  case class ImmediateJob() extends Job(JobType1, 3) {

    var lastContext = Option.empty[JobContext]

    override def run()(implicit context: JobContext): JobExecution = {
      lastContext = Some(context)
      new JobExecution() {
        override val result = Future.successful(())
        override def cancel(): Unit = ()
      }
    }
  }

  case class PromiseJob(promise: Promise[Unit] = Promise[Unit](),
                        override val lockTimeout: FiniteDuration = 100 millis) extends Job(JobType2, 3) {

    @volatile
    var canceled = false

    override def run()(implicit context: JobContext): JobExecution = new JobExecution() {

      override val result = promise.future
      override def cancel(): Unit = {
        canceled = true
        promise.failure(new Exception("canceled"))
      }

    }
  }

}