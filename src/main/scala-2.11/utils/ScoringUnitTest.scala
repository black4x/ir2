package utils

import scala.collection.Map

/**
  * Created by Ralph on 22/11/16.
  */
object ScoringUnitTest extends App {

  var relevance_judgement = Map[(Int, Int, String), Int]()
  var query_results = Map[(Int, Int), String]()

  // add some dummy values
  relevance_judgement += ((1, 0, "DOC-A") -> 1)
  relevance_judgement += ((1, 0, "DOC-B") -> 1)
  relevance_judgement += ((1, 0, "DOC-C") -> 0)
  relevance_judgement += ((1, 0, "DOC-D") -> 0)

  relevance_judgement += ((2, 0, "DOC-A") -> 1)
  relevance_judgement += ((2, 0, "DOC-E") -> 0)

  // 1 correct out of 3
  query_results += ((1,1) -> "DOC-A")
  query_results += ((1,2) -> "DOC-C")
  query_results += ((1,3) -> "DOC-D")

  query_results += ((2,1) -> "DOC-A")
  query_results += ((2,2) -> "DOC-E")

  //println(relevance_judgement)
  //println(query_results)

  val myScoring = new Scoring(relevance_judgement, query_results)

  val metrics = myScoring.calculateMetrics()

  /*
  metrics.foreach(metrics_per_query => {
    println(metrics_per_query._1 + " " + metrics_per_query._2.mkString)
  })
  */

  metrics.foreach(metrics_per_query => {
    print(metrics_per_query._1 + " ")
    metrics_per_query._2.foreach(metric => { print(metric.toString + " ")})
    println(" ")
  })


}
