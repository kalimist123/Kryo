package com.diaceutics.kryo

case class Entity(eType: String, eVal: String)

case class Entities(sentences: Array[List[Entity]]) {

  def getSentences = sentences

  def getEntities(eType: String) = {
    sentences flatMap { sentence =>
      sentence
    } filter { entity =>
      eType == entity.eType
    } map { entity =>
      entity.eVal
    }
  }

}
