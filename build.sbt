name := "CassandraServer"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.1",
  "com.datastax.cassandra" % "cassandra-driver-mapping" % "2.1.1")     

play.Project.playJavaSettings
