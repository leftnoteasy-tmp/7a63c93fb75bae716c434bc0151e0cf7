import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

    val appName         = "appengine-proxy"
    val appVersion      = "1.0"

    val appDependencies = Seq(
      "org.apache.hadoop" % "hadoop-yarn-client" % "2.0.5-alpha",
      "org.apache.hadoop" % "hadoop-yarn-api" % "2.0.5-alpha",
      "org.apache.hadoop" % "hadoop-yarn-common" % "2.0.5-alpha",
      "org.apache.hadoop" % "hadoop-yarn-server-common" % "2.0.5-alpha",
      "org.apache.hadoop" % "hadoop-common" % "2.0.5-alpha",
      "org.apache.hadoop" % "hadoop-hdfs" % "2.0.5-alpha",
      javaCore,
      javaJdbc,
      javaEbean
    )

    val main = play.Project(appName, appVersion, appDependencies).settings(
      // Add your own project settings here      
    )

}
            
