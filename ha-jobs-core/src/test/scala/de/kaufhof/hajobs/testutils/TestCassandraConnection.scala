package de.kaufhof.hajobs.testutils

import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory
import play.api.Logger

import scala.util.control.NonFatal

object TestCassandraConnection {

  private lazy val config = ConfigFactory.load()

  private var sessionCache: Option[Session] = None

  private def createSessionAndInitKeyspace(uri: CassandraConnectionUri, isDev: Boolean,
                                           defaultConsistencyLevel: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM):
  Session = sessionCache.synchronized {
    sessionCache.getOrElse {
      Logger.info(s"Connecting cassandra on ${uri.hosts.mkString("(", ",", ")")}:$uri.port (keyspace ${uri.keyspace})")
      val cluster = new Cluster.Builder().
        addContactPoints(uri.hosts.toArray: _*).
        withPort(uri.port).
        withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
      val session = cluster.connect

      // This is just here until we remove it finally when the SBT pillar plugin really works, i.e. with RUN hooks
      if (isDev) {
        if (Option(cluster.getMetadata.getKeyspace(uri.keyspace)).isEmpty) {
          Logger.debug("keyspace does not exist yet. creating it and running migrations.")
          Pillar.initialize(session, uri.keyspace, 1)
        }
        try {
          session.execute(s"USE ${uri.keyspace}")
          Pillar.migrate(session)
        } catch {
          case NonFatal(e) => Logger.error("exception running migrations", e); throw e
        }
        // END ...... sbt pillar hack
      } else {
        session.execute(s"USE ${uri.keyspace}")
      }

      sessionCache = Some(session)

      sessionCache.get
    }
  }

  private val cassandraUrl = CassandraConnectionUri(config.getString("cassandra.url"))
  lazy val Session = createSessionAndInitKeyspace(cassandraUrl, true, ConsistencyLevel.LOCAL_QUORUM)

}

trait TestCassandraConnection {

   protected lazy val session = TestCassandraConnection.Session

 }
