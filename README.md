# HA-Jobs

[![Build Status](https://travis-ci.org/Galeria-Kaufhof/ha-jobs.png?branch=play-2.5)](https://travis-ci.org/Galeria-Kaufhof/ha-jobs)

Support for distributed, highly available (batch) singleton jobs, with job scheduling, locking, supervision and job status persistence.
Implemented with Scala, Akka and Cassandra.

## New in 1.7.5

- added user interface for monitoring and triggering jobs

## New in 1.7.4

- Stability improvements of unit tests. Previous version should be avoided.

## New in 1.7.3

- A log message with level error is now logged when a job result
  is a failed future. Previously, there was no special handling
  for failed futures, they were handled like successful futures.
- Jobs that were scheduled during a downtime of the server
  are now automatically triggered after a restart. This is
  only done if the next regular run would be more than
  30 minutes in the future (configurable).

## New in 1.7.2

- added a controller Action to cancel jobs via REST API 

## New in 1.7.1

- added a trait StatusWriter to JobStatusRepository for easier mocking in tests. 

## New in 1.7.0

- add new job status 
- add waiting time to dead job detection 
- add start time and duration to job status json output

## New in 1.6.0

- update play from 2.4 to 2.5.9
- update akka to 2.4.12

## New in 1.5.3

- Improved documentation of JobManager.allJobsScheduled 
- Added hook in JobManager for checking preconditions before jobs are started 
- Added more information in log message for dead job detection 
- Increase version of cassandra driver from 3.0.0 to 3.0.2 
- Removed SBT plugin dependency-graph 

## New in 1.5.2

- #25 job supervisor should retry failed jobs by given retrigger count

## New in 1.5.1

- critical bugfix in JobSupervisor

```
GET    /jobs/:jobType    @de.kaufhof.hajobs.JobsController.list(jobType, limit: Int ?= 20)
```

## Contents

- [Overview](#overview)
 - [Features](#features)
 - [Constraints](#constraints)
 - [Implementation/Solution Details](#implementationsolution-details)
 - [Alternative Solutions](#alternative-solutions)
- [Installation / Setup](#installation--setup)
- [Usage](#usage)
 - [Example 1: A Job with Cron Schedule, Persistence and Supervision](#example-1-a-job-with-cron-schedule-persistence-and-supervision)
 - [Example 2: An Actor Job with Cron Schedule etc.](#example-2-an-actor-job-with-cron-schedule-etc)
 - [Example 3: An Actor Job running continuously](#example-3-an-actor-job-running-continuously)
 - [Play! REST API](#play-rest-api)
- [License](#license)

## Overview

### Features:

* Job Locking
  * Ensures, that a job is run by at most one instance
  * Locks are automatically released when an instance crashed
  * When a lock is lost / timed out (e.g. because the jvm is only/mainly doing GC), the job will be canceled so that
    another instance can step in
* Job Supervision
  * When a crashed job is detected, the job will be resumed/retried (maybe by another instance) a configured number of times
* Job Scheduling
  * Jobs can be scheduled, the [Quartz](http://quartz-scheduler.org/) scheduler is used for this.
* Job Status / Reporting
  * For each job the status and result is stored, jobs can also provide details to be stored
* Job Management
  * There's a [Play! Framework](https://www.playframework.com/) controller that provides a REST api for jobs, which
    allows to start a job, get a list of job executions, and details for a specific job execution.

### Constraints:

* There's no need for work distribution / if a single instance can execute a job.

### Implementation/Solution Details:

Currently for Job Locking Cassandra's lightweight transactions are used, locks are stored with a certain TTL (e.g. 30 seconds)
in a dedicated table.
As long as a job is running, an (Akka) actor keeps the lock for the job active.
When the job completes, the actor is stopped and the lock is released/deleted.
When the instance running the job crashes, the lock will be deleted due to the used TTL.

Job Supervision is done by a Job, that detects dead jobs and restarts them the configured
number of times.

There's the `JobManager`, that allows to start or restart a job, and schedules jobs based on their scheduling pattern.

A job must extend `Job`, which has the following interface:

```scala
abstract class Job(val jobType: JobType,
                   val retriggerCount: Int,
                   val cronExpression: Option[String] = None,
                   val lockTimeout: FiniteDuration = 60 seconds) {

  /**
   * Starts a new job and returns a [[de.kaufhof.hajobs.JobExecution JobExecution]].
   * The [[de.kaufhof.hajobs.JobExecution#result JobExecution.result]] future must be
   * completed when the job execution is finished.
   *
   * This method (`run`) should not "block", all the work must be performed
   * by the returned `JobExecution`.
   */
  def run()(implicit context: JobContext): JobExecution

}
```

The `JobType` identifies the job, it is defined with a name and a `LockType`.
There's a distinction between `JobType` and `LockType` so that jobs with the same `LockType` cannot
run simultaneously. A `LockType` is just defined by name.

If your job is implemented as an actor, you can just use the `ActorJob`, as shown by examples below.

### Alternative Solutions

* For job locking [Zookeeper could be used](http://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_Locks).
* For job locking and job supervision [Akka's Cluster Singleton](http://doc.akka.io/docs/akka/snapshot/contrib/cluster-singleton.html) could be used.
  There's also a nice [Activator template for distributed workers with Akka](http://typesafe.com/activator/template/akka-distributed-workers) that provides
  an example for the cluster singleton.
* [Chronos](https://github.com/airbnb/chronos) Fault tolerant job scheduler for Mesos which handles dependencies and ISO8601 based schedules. _Batteries included_

## Installation / Setup

You must add the ha-jobs to the dependencies of the build file, e.g. add to `build.sbt`:

    libraryDependencies += "de.kaufhof" %% "ha-jobs" % "1.7.5"

It is published to maven central for both scala 2.10 and 2.11.

You must create the tables `lock`, `job_status_data` and `job_status_meta` in the used Cassandra keyspace, according
to the ([Pillar](https://github.com/comeara/pillar)) migration scripts in
[ha-jobs-core/src/test/resources/migrations](https://github.com/Galeria-Kaufhof/ha-jobs/tree/master/ha-jobs-core/src/test/resources/migrations).

## Usage

For a single Job you have to

* Define the `LockType` (with name)
* Define the `JobType` (with name and `LockType`)
* Implement your concrete job, which must extend `Job`.
* If you're using an Akka Actor for your Job, you can just use the `ActorJob` as `Job` implementation and configure it
  with the `Props` creating your Actor (see also the example below).

To complete the setup you also need

* the `JobSupervisor` job, which detects dead jobs and retriggers them on an alive instance
* the `JobManager`, which schedules Jobs according to their cron patterns

You also need to setup/configure 2 or 3 things more, which should be self-explanatory.

### Example 1: A Job with Cron Schedule, Persistence and Supervision

Here's a fully working example of a job (might e.g. import products) that is started every 10 minutes.
The job stores its status when starting/finished, it might do so as well during the import so that its progress
could be tracked.
In this example the JobSupervisor is also configured, so that failed/dead jobs would be detected.

```scala
import akka.actor.ActorSystem
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs._
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success, Try}

// == Product Import Job
val ProductImportLockType = LockType("ProductImportLock")
val ProductImportJobType = JobType("ProductImport", ProductImportLockType)

/**
 * A job that normally would import products, but now only prints "importing" some times.
 */
class ProductImport(override val jobStatusRepository: JobStatusRepository,
                    cronExpression: Option[String]) extends Job(
  ProductImportJobType, retriggerCount = 3, cronExpression = cronExpression) with WriteStatus {

  override def run()(implicit context: JobContext): JobExecution = new JobExecution() {

    private val promise = Promise[Unit]()
    override val result = promise.future

    override def cancel(): Unit = {
      // We might update some flag that could be checked by `importProducts()`
    }

    writeStatus(Running)

    // onComplete: after updating our status we must complete the result. This will
    // release the lock and stop the lock keeper actor.
    importProducts().onComplete(updateStatus.andThen(_ => promise.success(())))

    // A not so long running operation, but still producing some side effect
    private def importProducts(): Future[Int] = {
      Future.successful {
        println("Importing products ... done.")
        42 // products imported
      }
    }

    private def updateStatus(implicit context: JobContext): Try[Int] => Future[JobStatus] = {
      case Success(count) =>
        writeStatus(Finished, Some(Json.obj("count" -> count)))
      case Failure(e) =>
        writeStatus(Failed, Some(Json.obj("error" -> e.getMessage)))
    }

  }

}

// Setup repos needed for jobs + job manager
// session: the Cassandra Session (com.datastax.driver.core.Session)
val statusRepo = new JobStatusRepository(session, jobTypes = JobTypes(ProductImportJobType))
val lockRepo = new LockRepository(session, LockTypes(ProductImportLockType))

// Setup jobs
val productImporter = new ProductImport(statusRepo, Some("0 0 * * * ?"))
val jobSupervisor = new JobSupervisor(manager, lockRepo, statusRepo, Some("0 * * * * ?"))

val system = ActorSystem("example1")

// Setup the JobManager
val manager: JobManager = new JobManager(Seq(productImporter, jobSupervisor), lockRepo, statusRepo, system)

// You should `shutdown()` the manager when the application is stopped.
```

In the given example, once the `JobManager` is created, it will schedule the `productImporter` according to its `cronExpression`.

The `JobTypes` and `LockTypes` must provide all your custom `JobType`s and `LockType`s, they're needed by
the repositories when loading db records, which is done when a job status is reported (e.g. via the Play! REST API).

### Example 2: An Actor Job with Cron Schedule etc.

If the previous Job would be implemented via an Actor this would be used together with the `ActorJob`.
So the `ProductImport` class and its instance would be replaced by this

```scala
class ProductImportActor(override val jobStatusRepository: JobStatusRepository)
                        (implicit jobContext: JobContext) extends Actor with WriteStatus {

  writeStatus(Running)

  self ! "go"

  override def receive: Receive = {
    case "go" =>
      println("Importing products ... done.")
      writeStatus(Finished, Some(Json.obj("count" -> 42)))
      // no need to tell the context that we're finished, this will be done by ActorJob when we're stopped.
      context.stop(self)
    case ActorJob.Cancel =>
      // We should support ActorJob.Cancel and stop() processing.
      writeStatus(Canceled)
      context.stop(self)
  }

}

object ProductImportActor {
  def props(statusRepo: JobStatusRepository)(jobContext: JobContext) =
    Props(new ProductImportActor(statusRepo)(jobContext))
}

// the `ActorJob` creates a new actor from the given Props on each schedule
val productImporter = new ActorJob(ProductImportJobType, ProductImportActor.props(statusRepo),
  system, cronExpression = Some("0 0 * * * ?"))
```

### Example 3: An Actor Job running continuously

A use case for this might be an actor that regularly consumes a queue, with a high frequency.
So the actor job should be started on system start, grab the lock, and run infinitely.

The relevant parts from the example above would be modified like this:

```scala
class QueueConsumerActor(interval: FiniteDuration,
                         override val jobStatusRepository: JobStatusRepository)
                        (implicit jobContext: JobContext) extends Actor with WriteStatus {

  writeStatus(Running)

  self ! "consume"

  override def receive: Receive = consuming(0)

  private def consuming(consumed: Int): Receive = {
    case "consume" =>
      println(s"Consuming, until now consumed $consumed items...")
      writeStatus(Running, Some(Json.obj("consumed" -> consumed)))
      context.system.scheduler.scheduleOnce(interval, self, "consume")
      context.become(consuming(consumed + 42))
    case ActorJob.Cancel =>
      // We should support ActorJob.Cancel and stop() processing.
      writeStatus(Canceled)
      context.stop(self)
  }

}

object QueueConsumerActor {
  def props(interval: FiniteDuration, statusRepo: JobStatusRepository)(jobContext: JobContext) =
    Props(new QueueConsumerActor(interval, statusRepo)(jobContext))
}

// the ActorJob does not define a `cronExpression`
val queueConsumer = new ActorJob(ConsumerJobType, QueueConsumerActor.props(2 seconds, statusRepo),
  system, cronExpression = None)
val manager: JobManager = new JobManager(Seq(queueConsumer, jobSupervisor), lockRepo, statusRepo, system)

// manually trigger the job
manager.triggerJob(ConsumerJobType) onComplete {
  case Success(Started(jobId, _)) => println(s"Started queue consumer job $jobId")
  // The Success case can also carry LockedStatus or Error
  case Success(els) => println(s"Could not start queue consumer: $els")
  case Failure(e) => println(s"An exception occurred when trying to start queue consumer: $e")
}
```

### Play! REST API

The module `ha-jobs-play` provides a Play! controller that allows to start jobs and retrieve the job status via HTTP.

To use this you must add the following to the build file:

    libraryDependencies += "de.kaufhof" %% "ha-jobs-play" % "1.7.5"

In your routes file you have to add these routes (of course you may choose different urls):


    GET    /jobs                    @de.kaufhof.hajobs.JobsController.types
    POST   /jobs/:jobType           @de.kaufhof.hajobs.JobsController.run(jobType)
    DELETE /jobs/:jobType           @de.kaufhof.hajobs.JobsController.cancel(jobType)
    GET    /jobs/:jobType           @de.kaufhof.hajobs.JobsController.list(jobType, limit: Int ?= 20)
    GET    /jobs/:jobType/latest    @de.kaufhof.hajobs.JobsController.latest(jobType)
    GET    /jobs/:jobType/:jobId    @de.kaufhof.hajobs.JobsController.status(jobType, jobId)
    
    # The entry point to the gui
    GET    /jobsOverview            @de.kaufhof.hajobs.OverviewController.index()
    
    # Map static resources from the /public folder to the /assets URL path (used by frontend)
    GET    /assets/*file            controllers.Assets.at(path = "/public", file)
    

Use your preferred dependency injection mechanism to provide the managed `JobsController` to your application. Either by
adding a new module to your application.conf or to your `ApplicationLoader`s load function.

```scala
val jobManager = ... // the JobManager
val jobTypes = ... // e.g. JobTypes(ProductImportJobType) in the 1st example
new JobsController(jobManager, jobTypes, de.kaufhof.hajobs.routes.JobsController)
new OverviewController //supplies the single page application gui
```

The `de.kaufhof.hajobs.routes.JobsController` is the reverse router (`ReverseJobsController`) created by Play!
on compilation.

Then you can manage your jobs via http, e.g. using the following for a job of `JobType("productimport")`:

```bash
# get a list of all job types
curl http://localhost:9000/jobs
# get a list of all job executions
curl http://localhost:9000/jobs/productimport
# get redirected to the status of the latest job execution
curl -L http://localhost:9000/jobs/productimport/latest
# get job execution status/details
curl http://localhost:9000/jobs/productimport/a13037f0-9076-11e4-a8d6-4ff0e8bdfb24
# execute the job
curl -X POST http://localhost:9000/jobs/productimport
# cancel the job execution
curl -X DELETE http://localhost:9000/jobs/productimport
```

## License

The license is Apache 2.0, see LICENSE.
