package de.kaufhof.hajobs

import java.util.UUID

object JobContextTestCompanion {
  // defining some callback method so it can be used in createContext (see below)
  def someCallback: Unit = Unit

  // need to store that method to reuse the same function again, if it's not the same function
  // checks for equality would fail in tests
  val dummyCallback = someCallback _


  def createContext(jobId: UUID, triggerId: UUID): JobContext = {
    JobContext(jobId, triggerId, dummyCallback)
  }
}