package com.diaceutics.kryo

import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor, Sentence}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import java.io.File
object ExtractEntitiesFromCsv {

  val proc: Processor = new FastNLPProcessor()
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("NLP Example")
      .config("spark.master", "local")
      .getOrCreate()


    import spark.implicits._
    val path ="Kryo\\src\\resources\\"
    val files =getListOfFiles(path)
    files foreach (file=>{ (file.getPath)

    val empDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file.getPath)
        val newDf = empDF.withColumn("annotated", lineAgg($"textstrings"))
        newDf.show(1000, false)
    })
  }


  def lineAgg = udf((line: String) => {

    val aggEnt = new NamedEntityAggregator();
    val sentenceChunks = aggEnt.extractEntities(proc, line)
    val sb = new StringBuilder
    sentenceChunks foreach { sentence =>
      sentence.map(s => sb.append(s.toString()))
     }
    sb.toString()
  })

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}