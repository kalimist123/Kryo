name := "Kryo"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"
val procVer = "7.5.2"
libraryDependencies ++= Seq(
  "org.clulab" %% "processors-main" % procVer,
  "org.clulab" %% "processors-corenlp" % procVer,
  "org.clulab" %% "processors-odin" % procVer,
  "org.clulab" %% "processors-modelsmain" % procVer,
  "org.clulab" %% "processors-modelscorenlp" % procVer,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion


)