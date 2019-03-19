package com.diaceutics.kryo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.clulab.processors.Processor
import org.clulab.processors.fastnlp.FastNLPProcessor

object ExtractEntitiesFromCsvWithMapPartition {

  val proc: Processor = new FastNLPProcessor()
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("NLP Example")
      .config("spark.master", "local")
      .getOrCreate()

    val proc: Processor = new FastNLPProcessor()
    val aggEnt = new NamedEntityAggregator();

    val empDf = spark.read.textFile("Kryo\\src\\resources\\1col.csv")


    val ents  = aggEnt.extract(empDf.rdd)
    ents.foreach{sentenceChunk=>sentenceChunk.sentences.foreach(println)}

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