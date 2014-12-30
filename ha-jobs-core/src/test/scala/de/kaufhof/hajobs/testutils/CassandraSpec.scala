package de.kaufhof.hajobs.testutils

import org.scalatest.concurrent.{Eventually, IntegrationPatience}

abstract class CassandraSpec extends StandardSpec with TestCassandraConnection with Eventually with IntegrationPatience {

}