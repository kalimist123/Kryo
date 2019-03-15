package com.diaceutics.kryo

import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor, Sentence}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf

object ExtractEntitiesFromCsv {

  val proc: Processor = new FastNLPProcessor()
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("NLP Example")
      .config("spark.master", "local")
      .getOrCreate()

    val proc: Processor = new FastNLPProcessor()
    val aggEnt = new NamedEntityAggregator();

    val empDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("Kryo\\src\\resources\\1col.csv")

    import spark.implicits._
    val newDf = empDF.withColumn("annotated", lineAgg($"textstrings"))
    newDf.show(1000, false)

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


}