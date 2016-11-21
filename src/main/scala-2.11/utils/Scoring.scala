package utils

import ch.ethz.dal.tinyir.processing.XMLDocument

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by Ralph on 20/11/16.
  */

//todo, work in progress

// query_results: [(Query ID, Rank), Doc ID]
// Calculate metrics per Query. Except for MAP (which divides the APs of each query by the number of queries)
class Scoring(val relevance_judgement: Map[(Int, Int, String), Int], val query_results: Map[(Int, Int), String]) {

  // Returns the metrics per query. The List contains the metrics for each query
  def calculateMetrics(): Map[Int, Array[Double]] = {


    var metrics_all_queries = Map[Int, Array[Double]]()


    val queryIDs: List[Int] = query_results.map(result => result._1).map(key => key._1).toList.distinct

    queryIDs.foreach{ queryID =>

      // Calculate precision
      val results_of_single_query = query_results.filter((key) => key._1 == queryID)
      val precision_of_query = calculatePrecisionPerQuery(queryID, results_of_single_query)

      // Calculate recall

      val recall_of_query = 1.0

      // Calculate F1

      val f1_of_query = 1.0

      // Calculate AP


      val ap_of_query = 1.0

      // Add results
      metrics_all_queries += (queryID -> (precision_of_query, recall_of_query, f1_of_query, ap_of_query))

    }

    return metrics_all_queries
    
  }

  private def calculatePrecisionPerQuery(query: Int, result: Map[(Int, Int), String]): Double = {

    // First, get all correct documents for the current query from the relevance judgement
    // Filter on relevant doc (=1) and then filter for the current Query and then get only the Doc IDs (can be optimized :-) )
    val total_relevant_docs: List[String] = relevance_judgement.filter((x) => x._2 == 1).filter((x) => x._1._1 == query).map(x => x._1._3).toList

    // Count how many documents returned by the query are correct based on the relevance judgement
    var tp = 0.0
    result.foreach(result_tuple => {
      if (total_relevant_docs.contains(result_tuple._2)){
        tp += 1
      }
    })

    // Get TP + FP = all results of query
    val tp_fp = result.size

    // Now calculate it
    return tp / tp_fp

  }


  private def calculateRecallPerQuery(): Double = {

    1.0
  }



  private def calculateF1PerQuery(): Double = {

    1.0
  }

  def calculateAvgPrecisionPerQuery(): Double = {
    1.0

  }

  def calculateMAP(): Double = {
    1.0

  }



}
