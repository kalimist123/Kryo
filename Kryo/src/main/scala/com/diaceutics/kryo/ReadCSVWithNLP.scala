package com.diaceutics.kryo
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor}
import scala.io.Source

/*
we needed to change the version of clulab
and override the jackson dependencies
 */

object ReadCSVWithNLP {


  def main(args:Array[String]): Unit = {
    val proc:Processor = new FastNLPProcessor()
    val bufferedSource = Source.fromFile("Kryo\\src\\resources\\5c8ac617a7d1780004000081.csv")
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)

      val doc = proc.annotate(cols(1))
      printDoc(doc)
    }
    bufferedSource.close

  }

  def printDoc(doc:Document) {
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
