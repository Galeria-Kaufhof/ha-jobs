package de.kaufhof.hajobs

import java.util.UUID._

import de.kaufhof.hajobs.JobResult._
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs.testutils.StandardSpec
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json._

class JobStatusSpec extends StandardSpec {

  private implicit val jobTypes = TestJobTypes

  "JobStatus JsonFormat" should {

    val now = DateTime.now
    val jobStatus: JobStatus = JobStatus(randomUUID(), JobType1, randomUUID(), Finished, Success, now, Some(Json.toJson("muhmuh")))
    val json = JobStatus.jobStatusWrites.writes(jobStatus)

    "render the timestamp in a readable form" in {
      (json \ "jobStatusTs").as[String] should be (now.toString(ISODateTimeFormat.dateTime()))

      // verify that everything else is fine
      JobStatus.jobStatusReads.reads(json).get should be (jobStatus)
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
      (legacyJson \ "jobStatusTs").as[Long] should be (now.getMillis)

      // verify that the legacy json can be read
      JobStatus.jobStatusReads.reads(legacyJson).get should be (jobStatus)
    }
  }

}
