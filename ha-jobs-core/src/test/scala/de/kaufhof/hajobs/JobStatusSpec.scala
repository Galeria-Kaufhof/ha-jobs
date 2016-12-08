package de.kaufhof.hajobs

import java.util.UUID
import java.util.UUID._

import com.datastax.driver.core.utils.UUIDs
import de.kaufhof.hajobs.JobResult._
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs.testutils.StandardSpec
import org.joda.time.format.{ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTime, Period}
import play.api.libs.json._

class JobStatusSpec extends StandardSpec {

  private implicit val jobTypes = TestJobTypes

  "JobStatus JsonFormat" should {

    val now = DateTime.now
    val startTime: Long = now.minusMinutes(10).getMillis
    val startTimeUUID: UUID = UUIDs.startOf(startTime)
    val jobStatus: JobStatus = JobStatus(randomUUID(), JobType1, startTimeUUID, Finished, Success, now, Some(Json.toJson("muhmuh")))
    val json = JobStatus.jobStatusWrites.writes(jobStatus)

    "render the timestamp in a readable form" in {
      (json \ "jobStatusTs").as[String] should be(now.toString(ISODateTimeFormat.dateTime()))

      // verify that everything else is fine
      JobStatus.jobStatusReads.reads(json).get should be(jobStatus)
    }

    "render the duration in a readable form" in {
      (json \ "duration").as[String] should be(new Period(0, 10, 0, 0).toString(PeriodFormat.wordBased()))

      // verify that everything else is fine
      JobStatus.jobStatusReads.reads(json).get should be(jobStatus)
    }

    "render the startTime in a readable form" in {
      val readableStartTime: String = new DateTime(startTime).toString(ISODateTimeFormat.dateTime())
      (json \ "startTime").as[String] should be(readableStartTime)

      // verify that everything else is fine
      JobStatus.jobStatusReads.reads(json).get should be(jobStatus)
    }

    "read the timestamp in legacy form (with just the unix timestamp)" in {
      // Create a legacy json representation with jobStatusTs as millis
      val jsonTransformer = (__ \ 'jobStatusTs).json.update(
        __.read[JsString].map { jobStatusTsString =>
          val jobStatusTs = ISODateTimeFormat.dateTime().parseDateTime(jobStatusTsString.value)
          JsNumber(jobStatusTs.getMillis)
        }
      )
      val legacyJson = json.transform(jsonTransformer).get
      (legacyJson \ "jobStatusTs").as[Long] should be(now.getMillis)

      // verify that the legacy json can be read
      JobStatus.jobStatusReads.reads(legacyJson).get should be(jobStatus)
    }
  }

}
