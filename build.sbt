import net.virtualvoid.sbt.graph.Plugin.graphSettings

val projectVersion = "1.0.1"

val projectSettings = Seq(
  description := "Run distributed, highly available (batch) jobs, with job locking and supervision.",
  organization := "de.kaufhof",
  version := projectVersion,
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("https://github.com/Galeria-Kaufhof/ha-jobs"))
)

val buildSettings = Seq(
  scalaVersion := "2.11.6",
  crossScalaVersions := Seq("2.10.5", "2.11.6"),
  scalacOptions ++= Seq("-language:reflectiveCalls", "-feature", "-deprecation"),
  // fork (tests) to free resources. otherwise c* sessions are collected and will OOME at some point
  fork := true
)

val publishSettings = Seq(
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra := (
    <scm>
      <url>git@github.com:Galeria-Kaufhof/ha-jobs.git</url>
      <connection>scm:git:git@github.com:Galeria-Kaufhof/ha-jobs.git</connection>
    </scm>
    <developers>
      <developer>
        <id>martin.grotzke</id>
        <name>Martin Grotzke</name>
        <url>https://github.com/magro</url>
      </developer>
    </developers>
  )
)

val playVersion = "2.3.7"
val akkaVersion = "2.3.7"
val scalatest = "org.scalatest" %% "scalatest" % "2.2.0" % "test"
val mockito = "org.mockito" % "mockito-core" % "1.9.5" % "test"
val playTest = "com.typesafe.play" %% "play-test" % "2.3.0" % "test"

lazy val core = project.in(file("ha-jobs-core"))
  .settings(name := "ha-jobs")
  .settings(projectSettings: _*)
  .settings(buildSettings: _*)
  .settings(publishSettings: _*)
  .settings(graphSettings: _*)
  .settings(
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4",
      "com.typesafe.play" %% "play-json" % playVersion exclude("com.typesafe.play", "play_" + scalaVersion.value.substring(0, 4)),
      "joda-time" % "joda-time" % "2.6",
      "org.slf4j" % "slf4j-api" % "1.7.9",
      "org.quartz-scheduler" % "quartz" % "2.2.1",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      playTest,
      scalatest,
      mockito,
      "com.chrisomeara" %% "pillar" % "2.0.1" % "test"
    )
  )

lazy val play = project.in(file("ha-jobs-play"))
  .dependsOn(core)
  .settings(
    name := "ha-jobs-play",
    description := "Adds a Play controller that allows to manage jobs."
  )
  .settings(projectSettings: _*)
  .settings(buildSettings: _*)
  .settings(publishSettings: _*)
  .settings(graphSettings: _*)
  .settings(
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play" % playVersion,
      "com.typesafe.play" %% "play-json" % playVersion,
      playTest,
      scalatest,
      mockito
    )
  )

lazy val main = project.in(file(".")).aggregate(core, play)
