name := "Spark_Data_Lake"

version := "0.1"

scalaVersion := "2.11.12"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

resolvers ++= Seq(
  "apache-snapshots" at "https://repo1.maven.org/maven2/"
)

resolvers ++= Seq(
  "apache-snapshots" at "http://central.maven.org/maven2/"
)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"

libraryDependencies += "io.delta" %% "delta-core" % "0.1.0"