name := "ExamplesAvro"

version := "1.0"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Cloudera CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq (
  "com.twitter" %% "bijection-avro" % "0.9.2",
  "org.apache.avro" % "avro" % "1.8.1",
  "com.gensler" % "scalavro_2.10" % "0.6.2"
)
    