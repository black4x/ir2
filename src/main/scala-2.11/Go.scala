import java.io.{FileInputStream, InputStream}

import ch.ethz.dal.tinyir.alerts.ScoredResult
import ch.ethz.dal.tinyir.io.ZipDirStream
import ch.ethz.dal.tinyir.processing.TipsterParse
import utils.TopResults

object Go extends App {

  val tipsterStream = new ZipDirStream("data").stream
  //val relevanceStream = new FileInputStream("data/relevance-judgements.csv")

  println("Number of files in zips = " + tipsterStream.length)

 // println(getCodeValueMap("51", relevanceStream).size)

  val query = "Airbus Subsidies".toLowerCase().split(" ")
  val top1000 = new TopResults(1000)

  tipsterStream.foreach(is => {
    val tipsterDocument = new TipsterParse(is)

    val tokens = tipsterDocument.tokens
    val termFrequencyMap = getTermFrequencyMap(query, tokens)
    if (termFrequencyMap.values.sum > 0) {
      val queryProbability = frequencyProductNoSmoothing(tokens.size, termFrequencyMap)
      top1000.add(ScoredResult(tipsterDocument.name, queryProbability))
      //println(queryProbability)
    }
  })

  println(top1000.results)

  def getTermFrequencyMap(query: Array[String], tokens: List[String]): Map[String, Int] = {
    query.map(t => (t, tokens.count(word => word.toLowerCase() == t))).toMap
  }

  def frequencyProductNoSmoothing(size: Int, termFrequencyMap: Map[String, Int]): Double = {
    termFrequencyMap.values.filter(tf => tf != 0).map(tf => tf.toDouble / size.toDouble).product
  }

  def getCodeValueMap(forNumber: String, inputStream: InputStream): Map[String, (Int, Int)] =
    scala.io.Source.fromInputStream(inputStream).getLines()
      .filterNot(_ startsWith forNumber)
      .map(_ split " ")
      // 51 0 AP880311-0301 1
      .collect { case Array(number, ignore, name, relevance) => (name, (number.toInt, relevance.toInt)) }
      .toMap


}
