package de.kaufhof.hajobs

import java.util.UUID._
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._
import akka.testkit.{TestKit, TestKitBase, TestProbe}
import de.kaufhof.hajobs.testutils.StandardSpec

class ActorJobSpec extends StandardSpec with TestKitBase {

  override implicit lazy val system = ActorSystem("ActorJobIntegrationSpec")

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "ActorJob" should {

    def test(actor: ActorRef => Actor)(check: (Job, TestProbe, AtomicBoolean) => Unit) = {
      val testProbe = new TestProbe(system)
      val jobType = new JobType("actorJob", new LockType("actorJobLock"))
      val props = Props(actor(testProbe.ref))
      val job = new ActorJob(jobType, _ => props, system, mock[JobStatusRepository])

      var finishCalled = new AtomicBoolean(false)
      implicit val context = JobContext(randomUUID(), randomUUID(), () => finishCalled.set(true))
      job.run()

      check(job, testProbe, finishCalled)
    }

    "run actor from props" in {

      class MyActor(receiver: ActorRef) extends Actor {
        receiver ! "run"
        override def receive: Receive = {
          case "ACK" => context.stop(self)
        }
      }

      test(testProbe => new MyActor(testProbe)) { (_, testProbe, finishCalled) =>
        testProbe.expectMsg("run")
        testProbe.reply("ACK")
        eventually {
          finishCalled.get() shouldBe true
        }
      }

    }

    "cancel actor on cancel()" in {

      class MyActor(receiver: ActorRef) extends Actor {
        receiver ! "run"
        override def receive: Receive = {
          case ActorJob.Cancel => context.stop(self)
        }
      }

      test(testProbe => new MyActor(testProbe)) { (job, testProbe, finishCalled) =>
        testProbe.expectMsg("run")
        job.cancel()
        eventually {
          finishCalled.get() shouldBe true
        }
      }

    }
  }
}