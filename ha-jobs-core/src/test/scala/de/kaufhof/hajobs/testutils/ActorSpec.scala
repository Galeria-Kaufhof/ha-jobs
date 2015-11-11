package de.kaufhof.hajobs.testutils

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Millis, Span}

import scala.language.postfixOps

abstract class ActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
    with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEachTestData with MockitoSugar {

  def this(actorSystemName: String) = this(ActorSystem(actorSystemName))

  protected implicit val patienceConfig = new PatienceConfig(scaled(Span(5000, Millis)))

  protected var aut: ActorRef = null

  override def afterEach(testData: TestData): Unit = {
    system.stop(aut)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

}