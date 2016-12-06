import java.io.FileInputStream

import Evaluation.QueryEvaluation
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream, ZipDirStream}
import ch.ethz.dal.tinyir.processing
import ch.ethz.dal.tinyir.processing.{TipsterParse, XMLDocument}
import ch.ethz.dal.tinyir.util.StopWatch
import main.{QuerySystem, QuerySystemWithSharding}
import utils.InOutUtils

import scala.collection.immutable.ListMap
import scala.collection.Map

/**
  * Created by Ralph on 30/11/16.
  */
object GoQuery extends App {

  // Todo: Read parameters from console

  val index_mode = "normal" // sharding"

  val myStopWatch = new StopWatch()
  myStopWatch.start

  val path : String = "data"
  var collection_tipster_stream = new TipsterStream(path).stream
  collection_tipster_stream = collection_tipster_stream.take(200)

  val relevance_judgement_stream = DocStream.getStream("data/relevance-judgements.csv")     //new FileInputStream("data/relevance-judgements.csv")
  val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)


  // Get list of query IDs and their titles (query ID needed for submission format!)
  val query_stream = DocStream.getStream("data/questions-descriptions.txt")
  val test_queries = InOutUtils.getTestQueries(query_stream)


  // Create the Inverted Index for the document collection
  var q_sys: QuerySystem = null
  var q_sys_sharding: QuerySystemWithSharding = null
  if (index_mode == "normal") {
    q_sys = new QuerySystem(collection_tipster_stream)
  } else{
    q_sys_sharding = new QuerySystemWithSharding(collection_tipster_stream,30000)
  }

  // TODO: submit parameter that tells which query model to use: language or term based

  println("start of query")
  var query_results_top_100 = Map[(Int, Int), String]()
  test_queries.foreach( query => {

    // Combine the results of each query into one Map
    query_results_top_100 = query_results_top_100 ++ q_sys.query(query._1, query._2)

  })

  // Sort by Query ID
  val query_results_top_100_sorted = ListMap(query_results_top_100.toSeq.sortBy(key => (key._1._1, key._1._2)):_*)

  query_results_top_100_sorted.foreach(result => {

    println(result)

  })

  // Evaluate results (calculate metrics)
  val myQE = new QueryEvaluation(relevance_judgement, query_results_top_100)
  myQE.calculateMetrics()

  val metrics = myQE.getQueryMetrics()
  val meanAvgPrecision = myQE.getMAP()

  metrics.foreach(metrics_per_query => {
    print(metrics_per_query._1 + " ")
    metrics_per_query._2.foreach(metric => { print(metric.toString + " ")})
    println(" ")
  })

  println("MAP is: " + meanAvgPrecision)

  myStopWatch.stop
  println("total time: " + myStopWatch.stopped)

}
