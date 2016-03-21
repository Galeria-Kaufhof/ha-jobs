package de.kaufhof.hajobs.testutils

import java.io.File
import java.util.Date

import de.kaufhof.pillar._
import com.datastax.driver.core.Session

object Pillar {

  private val registry = Registry.fromDirectory(new File(getClass.getClassLoader.getResource("migrations").toURI))
  private val migrator = Migrator(registry, new LoggerReporter)

  def initialize(session: Session, keyspace: String, replicationFactor: Int): Unit = {
    migrator.initialize(session, keyspace, new ReplicationOptions(Map("class" -> "SimpleStrategy", "replication_factor" -> replicationFactor)))
  }

  def migrate(session: Session): Unit = {
    migrator.migrate(session)
  }

}

import play.api.Logger

class LoggerReporter extends Reporter {
  def initializing(session: Session, keyspace: String, replicationOptions: ReplicationOptions) {
    Logger.info(s"Initializing keyspace '${keyspace}'")
  }

  def migrating(session: Session, dateRestriction: Option[Date]) {
    Logger.info(s"Migrating keyspace '${session.getLoggedKeyspace}'")
  }

  def applying(migration: Migration) {
    Logger.info(s"Applying ${migration.authoredAt.getTime}: ${migration.description}")
  }

  def reversing(migration: Migration) {
    Logger.info(s"Reversing ${migration.authoredAt.getTime}: ${migration.description}")
  }

  def destroying(session: Session, keyspace: String) {
    Logger.info(s"Destroying keyspace '${keyspace}'")
  }
}