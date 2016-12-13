package Evaluation

import scala.collection.Map

/**
  * Created by Ralph on 22/11/16.
  */
object QueryEvaluationUnitTest /*extends App */{

  var relevance_judgement = Map[(Int, Int, String), Int]()
  var query_results = Map[(Int, Int), String]()


// To test AP as in the lecture slides
  relevance_judgement += ((1, 0, "DOC-A") -> 1)
  relevance_judgement += ((1, 0, "DOC-C") -> 1)
  relevance_judgement += ((1, 0, "DOC-F") -> 1)
  relevance_judgement += ((1, 0, "DOC-I") -> 1)
  relevance_judgement += ((1, 0, "DOC-J") -> 1)

  // 10 results but only 5 are correct
  query_results += ((1,1) -> "DOC-A")
  query_results += ((1,2) -> "DOC-B")
  query_results += ((1,3) -> "DOC-C")
  query_results += ((1,4) -> "DOC-D")
  query_results += ((1,5) -> "DOC-E")
  query_results += ((1,6) -> "DOC-F")
  query_results += ((1,7) -> "DOC-G")
  query_results += ((1,8) -> "DOC-H")
  query_results += ((1,9) -> "DOC-I")
  query_results += ((1,10)-> "DOC-J")

  relevance_judgement += ((2, 0, "DOC-B") -> 1)
  relevance_judgement += ((2, 0, "DOC-E") -> 1)
  relevance_judgement += ((2, 0, "DOC-G") -> 1)

  // 10 results but only 3 are correct
  query_results += ((2,1) -> "DOC-A")
  query_results += ((2,2) -> "DOC-B")
  query_results += ((2,3) -> "DOC-C")
  query_results += ((2,4) -> "DOC-D")
  query_results += ((2,5) -> "DOC-E")
  query_results += ((2,6) -> "DOC-F")
  query_results += ((2,7) -> "DOC-G")
  query_results += ((2,8) -> "DOC-H")
  query_results += ((2,9) -> "DOC-I")
  query_results += ((2,10)-> "DOC-J")



  // Projection to project away the dummy number 0
  val relevance_judgement_clean: Map[(Int, String), Int] = relevance_judgement.map(
    rj_tuple => ((rj_tuple._1._1, rj_tuple._1._3), rj_tuple._2))

  val myQE = new QueryEvaluation(relevance_judgement_clean, query_results)
  myQE.calculateMetrics()

  val metrics = myQE.getQueryMetrics()
  val meanAvgPrecision = myQE.getMAP()



  metrics.foreach(metrics_per_query => {
    print(metrics_per_query._1 + " ")
    metrics_per_query._2.foreach(metric => { print(metric.toString + " ")})
    println(" ")
  })

  println("MAP is: " + meanAvgPrecision)


}
