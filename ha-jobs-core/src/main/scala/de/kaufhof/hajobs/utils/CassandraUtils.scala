package de.kaufhof.hajobs.utils

import com.datastax.driver.core.{ResultSet, ResultSetFuture}
import com.google.common.util.concurrent.{FutureCallback, Futures}

import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

object CassandraUtils {

  implicit def toScalaFuture(resultSet: ResultSetFuture): Future[ResultSet] = {
    val p = Promise[ResultSet]()
    Futures.addCallback[ResultSet](resultSet, new FutureCallback[ResultSet] {
      override def onSuccess(result: ResultSet): Unit = p success result

      override def onFailure(t: Throwable): Unit = p failure t
    })
    p.future
  }

}
