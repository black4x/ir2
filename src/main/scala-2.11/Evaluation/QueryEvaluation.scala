package Evaluation

import scala.collection.Map
import scala.collection.immutable.ListMap

/**
  * Created by Ralph on 20/11/16.
  */


// query_results: [(Query ID, Rank), Doc ID]
class QueryEvaluation(val relevance_judgement: Map[(Int, String), Int], val query_results: Map[(Int, Int), String]) {

  val total_relevant_judged_docs: Map[(Int, String), Int] = relevance_judgement.filter((x) => x._2 == 1)
  val queryIDs: List[Int] = query_results.map(result => result._1).map(key => key._1).toList.distinct
  var metrics_all_queries = Map[Int, Array[Double]]()
  var sum_ap_of_query: Double = 0.0

  // Returns the metrics per query. The List contains the metrics for each query
  def calculateMetrics() {

    queryIDs.foreach{ queryID =>

      val results_of_single_query = query_results.filter((key) => key._1._1 == queryID)

      // Get all relevant docs for current query from the relevance judgement
      val all_relevant_docs_for_query: List[String] = total_relevant_judged_docs.filter((x) => x._1._1 == queryID).map(x => x._1._2).toList

      // Count how many documents returned by the query are correct based on the relevance judgement
      var tp = 0.0
      results_of_single_query.foreach(result_tuple => {
        if (all_relevant_docs_for_query.contains(result_tuple._2)){
         //println("matching doc: " + result_tuple._2)
          tp += 1
        }
      })

      // Get TP + FP = all results of query
      val tp_fp = results_of_single_query.size

      // Get TP + FN = all relevant docs
      val tp_fn = all_relevant_docs_for_query.length


      // Calculate precision
      val precision_of_query = calculatePrecisionPerQuery(tp, tp_fp)


      // Calculate recall
      val recall_of_query = calculateRecallPerQuery(tp, tp_fn)

      // Calculate F1
      val f1_of_query = calculateF1PerQuery(precision_of_query, recall_of_query)

      // Calculate AP
      val ap_of_query = calculateAvgPrecisionPerQuery(results_of_single_query, all_relevant_docs_for_query, tp_fn)
      sum_ap_of_query += ap_of_query

      // Add results
      metrics_all_queries += (queryID -> Array(precision_of_query, recall_of_query, f1_of_query, ap_of_query))

    }

  }

  def getQueryMetrics(): Map[Int, Array[Double]] = { return metrics_all_queries }

  def getMAP(): Double = { return sum_ap_of_query / queryIDs.length }


  private def calculatePrecisionPerQuery(tp: Double, tp_fp: Double): Double = {
    if (tp_fp == 0){ return 0 }
    return tp / tp_fp
  }

  private def calculateRecallPerQuery(tp: Double, tp_fn: Double): Double = {
    if (tp_fn == 0){ return 0 }
    return tp / tp_fn
  }

  private def calculateF1PerQuery(precision: Double, recall: Double): Double = {
    if (precision + recall == 0) {return 0}
    return 2 * ((precision * recall) / (precision + recall))
  }

  // The AP is calculated "raw" and using strategy 1 (whole corpus)
  private def calculateAvgPrecisionPerQuery(query_result: Map[(Int, Int), String], rel_docs: List[String], tp_fn: Double): Double = {

    var precision_at_rank_k: Double = 0
    var total_precision: Double = 0
    var correct_docs_at_rank_k: Double = 0 // number of correct docs returned by query at rank k
    var returned_docs_at_rank_k: Double = 0 // total number docs returned by query at rank k

    if (tp_fn == 0) { return 0.0 }

    val query_result_sorted_by_rank = ListMap(query_result.toSeq.sortWith(_._1._2 < _._1._2):_*)

    query_result_sorted_by_rank.foreach(result_tuple => {
      returned_docs_at_rank_k += 1
      if (rel_docs.contains(result_tuple._2)){
        correct_docs_at_rank_k += 1
        precision_at_rank_k = correct_docs_at_rank_k / returned_docs_at_rank_k
        total_precision = total_precision + precision_at_rank_k
      }
    })

    return total_precision / tp_fn

  }


}
