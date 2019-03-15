package com.diaceutics.kryo
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor, Sentence}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ExtractEntitiesFromCsv {


  def main(args:Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Scala NLP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val proc:Processor = new FastNLPProcessor()
    val aggEnt = new NamedEntityAggregator();
    val empDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("Kryo\\src\\resources\\5c8ac617a7d1780004000081.csv")

    empDF.collect().foreach{x => {
                           println(x.getString(1))
      val sentenceChunks = aggEnt.extractEntities(proc, x.getString(1))
      sentenceChunks foreach { sentence =>
        sentence foreach println
      }

       }}

    }

}
