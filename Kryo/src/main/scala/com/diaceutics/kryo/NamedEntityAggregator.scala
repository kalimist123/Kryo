package com.diaceutics.kryo

import org.apache.spark.rdd.RDD
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Processor, Sentence}

class NamedEntityAggregator extends Serializable {

  def extractEntities(processor: Processor, corpus: String) = {
    val doc = processor.annotate(corpus)
    doc.sentences map processSentence
  }

  private def aggregate(entities: Array[Entity]) = {
    aggregateEntities(entities.head, entities.tail)
      .filter(_.eType != "O")
  }

  private def aggregateEntities(current: Entity, entities: Array[Entity], processed: List[Entity] = List[Entity]()): List[Entity] = {
    if (entities.isEmpty) {
      current :: processed
    } else {
      val entity = entities.head
      if (entity.eType == current.eType) {
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

  def extract(corpusRdd: RDD[String]):
  RDD[Entities] = {
    corpusRdd mapPartitions {
      case it=>
        val processor = new
            FastNLPProcessor()
        it map {
          corpus =>
            val entities =
              extractEntities(processor, corpus)
            new Entities(entities)
        }
    }
  }

}
