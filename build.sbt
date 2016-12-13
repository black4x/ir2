name := "prj2"

version := "1.0"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "lib"

javaOptions in run ++= Seq("-Xms10G", "-Xmx14G")

libraryDependencies += "org.iq80.leveldb" % "leveldb" % "0.1"
