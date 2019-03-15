package com.diaceutics.kryo
import org.clulab.processors.fastnlp.FastNLPProcessor
import org.clulab.processors.{Document, Processor, Sentence}
import scala.io.Source
object ExtractEntitiesWithoutRdd {


  def main(args:Array[String]): Unit = {
    val proc:Processor = new FastNLPProcessor()
    val aggEnt = new NamedEntityAggregator();
    val bufferedSource = Source.fromFile("Kryo\\src\\resources\\5c8ac617a7d1780004000081.csv")
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)

        val sentenceChunks = aggEnt.extractEntities(proc,
          cols(1))
        sentenceChunks foreach { sentence =>
          sentence foreach println
      }
    }
    bufferedSource.close
  }


}
