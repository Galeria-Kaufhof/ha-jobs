package de.kaufhof.hajobs.testutils

import org.scalatest._
import org.scalatest.concurrent.{IntegrationPatience, Eventually}
import org.scalatest.mockito.MockitoSugar
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

abstract class StandardSpec extends WordSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers
  with DefaultAwaitTimeout with FutureAwaits with OptionValues with EitherValues with MockitoSugar
  with Eventually with IntegrationPatience {

}