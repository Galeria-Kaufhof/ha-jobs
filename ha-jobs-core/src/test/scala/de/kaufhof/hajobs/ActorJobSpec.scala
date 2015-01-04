package de.kaufhof.hajobs

import java.util.UUID._

import akka.actor._
import akka.testkit.{TestKit, TestKitBase, TestProbe}
import de.kaufhof.hajobs.testutils.StandardSpec

import scala.util.Success

class ActorJobSpec extends StandardSpec with TestKitBase {

  override implicit lazy val system = ActorSystem("ActorJobIntegrationSpec")

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "ActorJob" should {

    def test(actor: ActorRef => Actor)(check: (TestProbe, JobExecution) => Unit) = {
      val testProbe = new TestProbe(system)
      val jobType = new JobType("actorJob", new LockType("actorJobLock"))
      val props = Props(actor(testProbe.ref))
      val job = new ActorJob(jobType, _ => props, system)

      implicit val context = JobContext(jobType, randomUUID(), randomUUID())
      val jobExecution = job.run()

      check(testProbe, jobExecution)
    }

    "run actor from props" in {

      class MyActor(receiver: ActorRef) extends Actor {
        receiver ! "run"
        override def receive: Receive = {
          case "ACK" => context.stop(self)
        }
      }

      test(testProbe => new MyActor(testProbe)) { (testProbe, jobExecution) =>
        testProbe.expectMsg("run")
        testProbe.reply("ACK")
        eventually {
          jobExecution.result.value shouldBe Some(Success(()))
        }
      }

    }

    "cancel actor on cancel() and complete the result" in {

      class MyActor(receiver: ActorRef) extends Actor {
        receiver ! "run"
        override def receive: Receive = {
          case ActorJob.Cancel => context.stop(self)
        }
      }

      test(testProbe => new MyActor(testProbe)) { (testProbe, jobExecution) =>
        testProbe.expectMsg("run")
        jobExecution.cancel()
        eventually {
          jobExecution.result.value shouldBe Some(Success(()))
        }
      }

    }
  }
}