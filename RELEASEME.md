# How to Release the HA-Jobs project

Set of instructions to become able to release the ha-jobs project to maven central and/or as snapshot to sonatype

## Contents

- [Requirements](#requirements)
 - [GPG Key](#gpgkey)
 - [Sonatype Account](#sonatype-account)
 - [SBT-PGP Plugin](#sbt-pgp-plugin)
 - [Sonatype Credentials File](#sonatype-credentials-file)
 - [GPG SBT-File](#gpg-sbt-file)
- [Preperations](#preparations)
- [Snapshot Release](#snapshot-release)
- [Final Release](#final-release)
- [Sonatype](#sonatype)
 

## Requirements

* To get your project hosted on Sonatype (and Maven Central) here is a good [summary](http://www.scala-sbt.org/0.13/docs/Using-Sonatype.html#Fourth+-+Adding+credentials)

### GPG Key

 * First of all, if you did not for other projects, you have to generate a [GPG Key](https://www.gnupg.org/gph/en/manual/c14.html).
 * to check which keys you own private keys for: gpg --list-secret-keys
 * generate the key: gpg --gen-key
:wq * send the public key to a keyserver: gpg --send-keys ID4711AA (replace ID4711AA with your own key id), if the default 
   server does not work try to specify a server yourself, e.g.: gpg --keyserver hkp://pool.sks-keyservers.net --send-keys ID4711AA (replace ID4711AA with your own key id), if the default
 * check if your key has been uploaded/synchronized: gpg --recv-keys ID4711AA (replace ID4711AA with your own key id)
  
### Sonatype Account

 * If you did not own a sonatype account so far, you have to sign up at [Sonatype](http://central.sonatype.org/pages/ossrh-guide.html)
 * First create a jira account
 * Request for publishing permissions in the group "de.kaufhof" via jira ticket
 * to learn how to use sonatype with your credentials, check [this](http://www.scala-sbt.org/0.13/docs/Using-Sonatype.html#Fourth+-+Adding+credentials)

### Sonatype Credentials File

 * if not existing, create ~/.sbt/0.13/sonatype.sbt
 * edit above file and add
  ```credentials += Credentials("Sonatype Nexus Repository Manager",
                                "oss.sonatype.org",
                                "<your username>",
                                "<your password>")
  ```

### GPG SBT-File

 * if not existing, create ~/.sbt/0.13/plugins/gpg.sbt
 * edit above file and add sbt-pgp plugin with `addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")` (for sbt version 0.13.5 and above)
   or with `addSbtPlugin("com.typesafe.sbt" % "sbt-pgp" % "0.8.3")` (for older sbt versions)

## Preperations

 * Check [GitHub](https://github.com/Galeria-Kaufhof/ha-jobs) for eventually open pull requests which should be mentioned in the release
 * If there are open pull requests, check out and test them first, when they are OK you may merge them into the master
 * Checkout/Clone the data status you want to release to your local repository
 * if you wanna change some code before the release, do so and use `git push` to add them to github before you start with an release
 * btw. builds from this project are made with travis, you can see an overview [here](https://travis-ci.org/Galeria-Kaufhof/ha-jobs) 
 

## Final release

 * to build a regular release just increment the version number in your project build.sbt AND the README.md without adding
   -SNAPSHOT at the end. If you released a SNAPSHOT with this code stand before do not incremnt the verison number, just remove the
   -SNAPSHOT ending
 * start sbt on your project root
 * publish the changes with `very publishSigned`
 * confirm the publish with the your gpg password when sbt asks you
 * for the final release you have to sign in to [Sonatype](https://oss.sonatype.org) with your credentials
 * go to `Staging Repositories` on the left side
 * search for the kaufhof repo, click on it
 * close the repo (the button is at the menu bar above the repo table), `remember!` that closing the repo means no further 
   release of this version is possible!
 * after the repo has been closed, press the release button
 * after 10-30 minutes your release should be distributed
