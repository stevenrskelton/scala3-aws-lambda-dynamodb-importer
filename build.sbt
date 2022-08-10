lazy val root = project
  .in(file("."))
  .settings(
    name := "aws-lambda-dynamo-import",
    organization := "ca.stevenskelton.awslambda.dynamoimport",
    description := "Lambda function that inserts new items into a DynamoDB table",
    version := "0.1.0",
    //scalaVersion := "2.13.8",
    scalaVersion := "3.1.3",
    scalacOptions += s"-target:jvm-11",
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    libraryDependencies ++= Seq(
        "com.amazonaws" % "aws-lambda-java-core" % "1.2.1",
        "com.amazonaws" % "aws-lambda-java-serialization" % "1.0.0",
        "software.amazon.awssdk" % "dynamodb" % "2.17.248",
	      "org.scalameta" %% "munit" % "0.7.29" % Test
    )
  )

assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
}