val projectVersion = "1.8.0"
val projectScalaVersion = "2.12.2"

scalaVersion := projectScalaVersion
crossScalaVersions := Seq(projectScalaVersion, "2.11.11")

val projectSettings = Seq(
  description := "Run distributed, highly available (batch) jobs, with job locking and supervision.",
  organization := "de.kaufhof",
  version := projectVersion,
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("https://github.com/Galeria-Kaufhof/ha-jobs"))
)

val buildSettings = Seq(
  scalaVersion := projectScalaVersion,
  crossScalaVersions := Seq(projectScalaVersion, "2.11.11"),
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
  pomExtra := <scm>
    <url>git@github.com:Galeria-Kaufhof/ha-jobs.git</url>
    <connection>scm:git:git@github.com:Galeria-Kaufhof/ha-jobs.git</connection>
  </scm>
    <developers>
      <developer>
        <id>martin.grotzke</id>
        <name>Martin Grotzke</name>
        <url>https://github.com/magro</url>
      </developer>
      <developer>
        <id>lichtsprung</id>
        <name>Robert Giacinto</name>
        <url>https://github.com/lichtsprung</url>
      </developer>
      <developer>
        <id>adelafogoros</id>
        <name>Adela Fogoros</name>
        <url>https://github.com/adelafogoros</url>
      </developer>
      <developer>
        <id>muellenborn</id>
        <name>Markus Müllenborn</name>
        <url>https://github.com/muellenborn</url>
      </developer>
      <developer>
        <id>MarcoPriebe</id>
        <name>Marco Priebe</name>
        <url>https://github.com/MarcoPriebe</url>
      </developer>
    </developers>
)

val playVersion = "2.6.0"
val akkaVersion = "2.5.3"
val scalatest = "org.scalatest" %% "scalatest" % "3.0.3" % "test"
val mockito = "org.mockito" % "mockito-core" % "2.8.47" % "test"
val playTest = "com.typesafe.play" %% "play-test" % playVersion % "test"

lazy val core = project.in(file("ha-jobs-core"))
  .settings(name := "ha-jobs")
  .settings(projectSettings: _*)
  .settings(buildSettings: _*)
  .settings(publishSettings: _*)
  .settings(
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
      "com.typesafe.play" %% "play-json" % playVersion exclude("com.typesafe.play", "play_" + scalaVersion.value.substring(0, 4)),
      "joda-time" % "joda-time" % "2.9.9",
      "org.slf4j" % "slf4j-api" % "1.7.25",
      "org.quartz-scheduler" % "quartz" % "2.3.0",
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      playTest,
      scalatest,
      mockito,
      "de.kaufhof" %% "pillar" % "4.1.0" % "test"
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
