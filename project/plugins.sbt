// You may use this file to add plugin dependencies for sbt.

resolvers += "Spark Packages repo" at "https://repos.spark-packages.org/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

//addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.1.1")
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

//addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

//addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.2.3")

//addSbtPlugin("com.databricks" %% "sbt-databricks" % "0.1.3")

//addSbtPlugin("me.lessis" % "bintray-sbt" % "0.6.1")
//addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

//addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0") // Use the latest version of sbt-assembly