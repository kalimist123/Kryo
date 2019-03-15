package com.diaceutics.kryo
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor, Sentence}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object AggregateEntitiesWithRDD {



  def main(args:Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Scala NLP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val proc:Processor = new FastNLPProcessor()

    val empDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("Kryo\\src\\resources\\5c8ac617a7d1780004000081.csv")

    empDF.collect().foreach{x => {
                           println(x.getString(1))
      val sentenceChunks = extractEntities(proc, x.getString(1))
      sentenceChunks foreach { sentence =>
        sentence foreach println
      }

       }}

    }


  private def extractEntities(processor: Processor, corpus: String) = {
    val doc = processor.annotate(corpus)
    doc.sentences map processSentence
  }

  private def aggregate(entities: Array[Entity]) = {
    aggregateEntities(entities.head, entities.tail)
      .filter(_.eType != "O")
  }

  private def aggregateEntities(current: Entity, entities: Array[Entity], processed : List[Entity] = List[Entity]()): List[Entity] = {
    if(entities.isEmpty) {
      current :: processed
    } else {
      val entity = entities.head
      if(entity.eType == current.eType) {
        val agg = Entity(current.eType, current.eVal + " " + entity.eVal)
        aggregateEntities(agg, entities.tail, processed)
      } else {
        aggregateEntities(entity, entities.tail, current :: processed)
      }
    }
  }

  private def processSentence(sentence: Sentence): List[Entity] = {
    val entities = sentence.lemmas.get
      .zip(sentence.entities.get)
      .map { case (eVal, eType) =>
        Entity(eType, eVal)
      }

    aggregate(entities)
  }

}
