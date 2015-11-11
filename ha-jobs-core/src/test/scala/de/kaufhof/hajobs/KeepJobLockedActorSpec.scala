package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.testutils.ActorSpec
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class KeepJobLockedActorSpec extends ActorSpec("KeepJobLockedSpec") {

  "An KeepJobLocked actor" must {
    "cancel a job if it loses its lock" in {
      var cancelled = false
      val lockRepository = mock[LockRepository]
      when(lockRepository.updateLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.successful(false))
      aut = system.actorOf(KeepJobLockedActor.props(lockRepository, JobType1, UUIDs.timeBased(), 1 second, () => cancelled = true))

      eventually {
        cancelled should be (true)
      }
    }

    "cancel a job if working on lockRepo fails" in {
      var cancelled = false
      val lockRepository = mock[LockRepository]
      when(lockRepository.updateLock(any[JobType], any[UUID], any[Duration])(any())).thenThrow(new RuntimeException("Simulated Exception"))
      aut = system.actorOf(KeepJobLockedActor.props(lockRepository, JobType1, UUIDs.timeBased(), 1 second, () => cancelled = true))

      eventually {
        cancelled should be (true)
      }
    }

    "cancel a job if communication with c* fails" in {
      var cancelled = false
      val lockRepository = mock[LockRepository]
      when(lockRepository.updateLock(any[JobType], any[UUID], any[Duration])(any())).thenReturn(Future.failed(new RuntimeException("Simulated Exception")))
      aut = system.actorOf(KeepJobLockedActor.props(lockRepository, JobType1, UUIDs.timeBased(), 1 second, () => cancelled = true))

      eventually {
        cancelled should be (true)
      }
    }
 }
}