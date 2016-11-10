addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

// aggregate tasks across subprojects and their crossScalaVersions
// use `very publishSigned` to cross publish subprojects.
addSbtPlugin("com.eed3si9n" % "sbt-doge" % "0.1.1")