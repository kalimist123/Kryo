package com.diaceutics.kryo

import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor, Sentence}
import org.apache.spark.sql.{SQLContext, SparkSession}


object ExtractEntitiesFromCsv {


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
      .load("Kryo\\src\\resources\\5c8ac617a7d1780004000081.csv")

    empDF.collect().foreach { x => {
      println(x.getString(1))
      val sentenceChunks = aggEnt.extractEntities(proc, x.getString(1))
      sentenceChunks foreach { sentence =>
        sentence foreach println
      }

    }
    }

  }

}
