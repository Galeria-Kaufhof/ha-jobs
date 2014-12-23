package de.kaufhof.hajobs.testutils

import org.scalatest._
import play.api.test.{DefaultAwaitTimeout, FutureAwaits}

abstract class StandardSpec extends WordSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers
  with DefaultAwaitTimeout with FutureAwaits with OptionValues with EitherValues {

}