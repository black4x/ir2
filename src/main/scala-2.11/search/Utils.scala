package search

import Evaluation.QueryEvaluation
import ch.ethz.dal.tinyir.io.DocStream
import ch.ethz.dal.tinyir.processing.{StopWords, Tokenizer}
import com.github.aztek.porterstemmer.PorterStemmer
import utils.InOutUtils

import scala.collection.Map

object Utils {

  def tokenize (text: String) : List[String] =text.toLowerCase.split("[- .,;:?!*&$-+\"\'\t\n\r\f `]+").filter(w => w.length >= 3).toList

  def filter(content: String): Seq[String] = StopWords.filterOutSW(tokenize(content))
    .map(v => PorterStemmer.stem(v))

  def showResults(query_results_top_100: Map[(Int, Int), String]): Unit = {
    val relevance_judgement_stream = DocStream.getStream("data/relevance-judgements.csv")
    val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)
    val myQE = new QueryEvaluation(relevance_judgement, query_results_top_100)
    myQE.calculateMetrics()

    val metrics = myQE.getQueryMetrics()
    val meanAvgPrecision = myQE.getMAP()

    metrics.foreach(metrics_per_query => {
      print("Query: " + metrics_per_query._1 + " -> ")
      print("Precision: " + metrics_per_query._2(0))
      print(", Recall: " + metrics_per_query._2(1))
      print(", F1: " + metrics_per_query._2(2))
      print(", Avg Precision: " + metrics_per_query._2(3))
      println(" ")
    })

    println("MAP is: " + meanAvgPrecision)
  }

}
