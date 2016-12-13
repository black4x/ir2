package search

import ch.ethz.dal.tinyir.alerts.ScoredResult
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream}
import ch.ethz.dal.tinyir.util.StopWatch
import utils.{InOutUtils, TopResults}

import scala.collection.mutable

// per query 2 runs through entire collection!
object NoIndex extends App {

  var stream = new TipsterStream("data").stream.take(50000)
  println("Number of files in zips = " + stream.length)

  var allQueries: List[(Int, String)] = InOutUtils.getValidationQueries(DocStream.getStream("data/questions-descriptions.txt")).take(1)

  println("Number of queries = " + allQueries.size)

  allQueries.foreach(q => run(q))

  // ----------------------------- END of Execution ------------------------

  def run(queryToRun: (Int, String)): Unit = {
    val query = InOutUtils.filter(queryToRun._2)

    var totalNumber = 0
    val tokenFrequencyMapInCollection = mutable.Map[String, Int]()

    val myStopWatch = new StopWatch()
    myStopWatch.start

    stream.foreach(stream => {
      val tokens = InOutUtils.filter(stream.content)
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
      val tokens = InOutUtils.filter(doc.content)
      val termFrequencyMap = getTermFrequencyMap(query, tokens)
      if (termFrequencyMap.values.sum > 0) {
        // check if there is at least one word from query in doc's content
        val queryProbability = languageModelScore(query, tokens, termFrequencyMap, tokenFrequencyMapInCollection, totalNumber)
        top100.add(ScoredResult(doc.name, queryProbability))
      }
    })

    val queryID = queryToRun._1
    var result_formatted = Map[(Int, Int), String]()
    var rank = 0
    top100.results.foreach(result => {
      rank += 1
      result_formatted += ((queryID, rank) -> result.title)
    })

    myStopWatch.stop
    println("done " + myStopWatch.stopped)
    InOutUtils.evalResuts(result_formatted)
    println(" ---------------------------------------------------------------------------------- ")

  }

  def getTermFrequencyMap(query: Seq[String], tokens: Seq[String]): Map[String, Int] = {
    query.map(t => (t, tokens.count(word => word == t))).toMap
  }

  def languageModelScore(query: Seq[String], tokens: Seq[String],
                         termFrequencyMap: Map[String, Int],
                         tokenFrequencyMapInCollection: mutable.Map[String, Int],
                         totalNumber: Int): Double = {
    val lambda = 0.6f
    query.map(qtoken => {
      val tokenFrequencyInDoc = termFrequencyMap.getOrElse(qtoken, 0).toDouble
      val numberOfTokensInDoc = tokens.length.toDouble
      lambda * (tokenFrequencyInDoc / numberOfTokensInDoc) + (1 - lambda) * tokenFrequencyMapInCollection(qtoken).toDouble / totalNumber
    }).product
  }

}
