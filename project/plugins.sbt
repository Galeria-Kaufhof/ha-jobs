addSbtPlugin("com.typesafe.sbt" % "sbt-web" % "1.1.0")

// aggregate tasks across subprojects and their crossScalaVersions
// use `very publishSigned` to cross publish subprojects.
addSbtPlugin("com.eed3si9n" % "sbt-doge" % "0.1.1")

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.5.9")