
name := "ha-jobs"

description := "Run distributed, highly available (batch) jobs, with job locking and supervision."

organization := "de.kaufhof"

version := "1.0.0-RC1"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/Galeria-Kaufhof/cassandra-jobs"))

scalaVersion := "2.11.4"

crossScalaVersions := Seq("2.10.4", "2.11.4")

scalacOptions ++= Seq("-language:reflectiveCalls", "-feature", "-deprecation")

val playVersion = "2.3.7"

val akkaVersion = "2.3.7"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4",
  // FIXME: the dependency on play should be removed
  "com.typesafe.play" %% "play" % playVersion,
  "com.typesafe.play" %% "play-json" % playVersion /*exclude("com.typesafe.play", "play_2.11")*/,
  "joda-time" % "joda-time" % "2.6",
  "org.slf4j" % "slf4j-api" % "1.7.9",
  "org.quartz-scheduler" % "quartz" % "2.2.1",
  "org.scaldi" %% "scaldi" % "0.4" % "optional",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "org.mockito" % "mockito-core" % "1.9.5" % "test",
  "com.typesafe.play" %% "play-test" % "2.3.0" % "test",
  "com.chrisomeara" %% "pillar" % "2.0.1" % "test"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

net.virtualvoid.sbt.graph.Plugin.graphSettings

// Publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:Galeria-Kaufhof/cassandra-jobs.git</url>
    <connection>scm:git:git@github.com:Galeria-Kaufhof/cassandra-jobs.git</connection>
  </scm>
    <developers>
      <developer>
        <id>martin.grotzke</id>
        <name>Martin Grotzke</name>
        <url>https://github.com/magro</url>
      </developer>
    </developers>)
