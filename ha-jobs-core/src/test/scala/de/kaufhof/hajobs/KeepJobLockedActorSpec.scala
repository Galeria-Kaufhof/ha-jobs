package de.kaufhof.hajobs

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.utils.UUIDs
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Millis, Span}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class KeepJobLockedActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
    with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEachTestData with MockitoSugar {

  def this() = this(ActorSystem("KeepJobLockedSpec"))

  implicit val patienceConfig = new PatienceConfig(scaled(Span(5000, Millis)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  var aut: ActorRef = null

  override def afterEach(testData: TestData): Unit = {
    system.stop(aut)
  }

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