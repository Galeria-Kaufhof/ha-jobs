package de.kaufhof

package object hajobs {

  type Jobs = Map[JobType, Job]

  // scalastyle:off method.name We allow an upper case method name to mimic a Jobs apply method
  def Jobs(jobs: Seq[Job]): Jobs = jobs.map(s => s.jobType -> s).toMap
  // scalastyle:on


}
