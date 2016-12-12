package search

import ch.ethz.dal.tinyir.alerts.ScoredResult
import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.util.StopWatch
import utils.TopResults

import scala.collection.mutable

// 2 runs through entire collection!
object NoIndex extends App {

  var stream = new TipsterStream("data").stream.take(10000)
  println("Number of files in zips = " + stream.length)

  val query = Utils.filter("Airbus Subsidies")

  var totalNumber = 0
  val tokenFrequencyMapInCollection = mutable.Map[String, Int]()

  val myStopWatch = new StopWatch()
  myStopWatch.start

  val numberOfAllTokens = stream.foreach(stream => {
    val tokens = Utils.filter(stream.content)
    totalNumber += tokens.size
    query.foreach(q => {
      val n = tokenFrequencyMapInCollection.getOrElse(q, 0)
      tokenFrequencyMapInCollection(q) = n + tokens.count(t => t == q)
    })
  })

  println("total number of tokens = " + totalNumber)
  println("query token's frequency in entire collection = " + tokenFrequencyMapInCollection)


  val top100 = new TopResults(100)

  stream.foreach(doc => {
    val tokens = Utils.filter(doc.content)
    val termFrequencyMap = getTermFrequencyMap(query, tokens)
    if (termFrequencyMap.values.sum > 0) {
      // check if there is at least one word from query in doc's content
      val queryProbability = languageModelScore(query, tokens, termFrequencyMap)
      top100.add(ScoredResult(doc.name, queryProbability))
    }
  })

  var queryID = 51
  var result_formatted = Map[(Int, Int), String]()
  var rank = 0
  top100.results.foreach(result => {
    rank += 1
    result_formatted += ((queryID, rank) -> result.title)
  })

  println(result_formatted)

  myStopWatch.stop
  Utils.showResults(result_formatted)
  println("done " + myStopWatch.stopped)

  def getTermFrequencyMap(query: Seq[String], tokens: Seq[String]): Map[String, Int] = {
    query.map(t => (t, tokens.count(word => word == t))).toMap
  }

  def languageModelScore(query: Seq[String], tokens: Seq[String], termFrequencyMap: Map[String, Int]): Double = {
    val lambda = 0.6f
    query.map(qtoken => {
      val tokenFrequencyInDoc = termFrequencyMap.getOrElse(qtoken, 0).toDouble
      val numberOfTokensInDoc = tokens.length.toDouble
      lambda * (tokenFrequencyInDoc / numberOfTokensInDoc) + (1 - lambda) * tokenFrequencyMapInCollection(qtoken).toDouble / totalNumber
    }).product
  }

}
