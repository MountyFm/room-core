name := "room-core"

version := "0.1"

scalaVersion := "2.12.12"
credentials += Credentials("Artifactory Realm", "mounty.jfrog.io", "sansyzbayevdaniyar3@gmail.com", "AKCp8k8iXkJUazq2J2CAa5uT4XvrDwf9Y9uzWsLuGcoq5C1pYix9DaP2CGsAUjgvH4mReFuoJ")


scalacOptions ++= Seq(
  "-feature", // Emit warning and location for usages of features that should be imported explicitly
  "-unchecked", // Enable additional warnings where generated code depends on assumptions
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-explaintypes", // Explain type errors in more detail
  "-opt:l:inline", // Enable cross-method optimizations: l:method,inline
  "-opt-inline-from:**", // Optimisations
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id
  "-Ywarn-dead-code", // Warn when dead code is identified
  "-Ywarn-unused:imports,patvars,privates,locals,explicits,implicits" // Warn unused
)


resolvers +=
  "Artifactory" at "https://mounty.jfrog.io/artifactory/mounty-domain-sbt-release-local"

resolvers += Resolver.bintrayRepo("akka", "snapshots")

libraryDependencies ++= Seq(
  "joda-time"  % "joda-time"  % "2.10.13",
  "kz.mounty" %% "mounty-domain" % "0.1.1-SNAPSHOT",
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.4.0",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-simple" % "1.7.32",
  "org.json4s" %% "json4s-jackson" % "4.0.3",
  "com.typesafe.akka" %% "akka-stream" % "2.6.17",
  "org.json4s" %% "json4s-native" % "4.0.3",
  "com.rabbitmq" % "amqp-client" % "5.14.0",
  "com.typesafe.akka" %% "akka-slf4j" % "2.6.17",
  "com.typesafe.akka" %% "akka-actor" % "2.6.17",
  "com.github.scredis" %% "scredis" % "2.4.3"
)
