


package com.diaceutics.kryo
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.serialization.DocumentSerializer
import org.clulab.processors.{Document, Processor}

object ProcessorExample {
  def main(args:Array[String]) {
    // create the processor
    val proc:Processor = new FastNLPProcessor()

    val doc = proc.annotate("Michael Munro went to China and New York. He visited Beijing to see Tony Santiago, on January 10th, 2013.")
    printDoc(doc)
    // serialize the doc using our custom serializer
    val ser = new DocumentSerializer
    val out = ser.save(doc)
    println("SERIALIZED DOC:\n" + out)
  }

  def printDoc(doc:Document) {
    // let's print the sentence-level annotations
    var sentenceCount = 0
    for (sentence <- doc.sentences) {
      println("Sentence #" + sentenceCount + ":")
      println("Tokens: " + sentence.words.mkString(" "))
      sentence.entities.foreach(entities => println(s"Named entities: ${entities.mkString(" ")}"))
      sentenceCount += 1
      println("\n")
    }

  }
}
