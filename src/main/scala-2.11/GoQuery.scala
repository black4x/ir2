import java.io.FileInputStream

import Evaluation.QueryEvaluation
import Indexing.{QSysDocMap, QSysDocMapAndDocSharding, QSysDocMapAndDocShardingVBE}
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream, ZipDirStream}
import ch.ethz.dal.tinyir.processing
import ch.ethz.dal.tinyir.processing.{TipsterParse, XMLDocument}
import ch.ethz.dal.tinyir.util.StopWatch
import utils.InOutUtils

import scala.collection.immutable.ListMap
import scala.collection.Map

/**
  * Created by Ralph on 30/11/16.
  */
object GoQuery extends App {

  // Todo: Read parameters from console: sharding or not, compression or not, run with test queries, run with real queries
  val index_mode = "sharding"//"sharding" // normal"

  val myStopWatch = new StopWatch()
  myStopWatch.start

  val path : String = "data"
  var collection_tipster_stream = new TipsterStream(path).stream.take(50000)

  val relevance_judgement_stream = DocStream.getStream("data/relevance-judgements.csv")     //new FileInputStream("data/relevance-judgements.csv")
  val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)
  //relevance_judgement.foreach(rj => println(rj))

  // Get list of query IDs and their titles (query ID needed for submission format!)
  val query_stream = DocStream.getStream("data/questions-descriptions.txt")
  val test_queries = InOutUtils.getTestQueries(query_stream)


  // Create the Inverted Index for the document collection (either normal or with document sharding)
  var q_sys: QSysDocMap = null
  var q_sys_sharding: QSysDocMapAndDocShardingVBE = null
  if (index_mode == "normal") {
    q_sys = new QSysDocMap(collection_tipster_stream)
  }
  else {
    q_sys_sharding = new QSysDocMapAndDocShardingVBE(collection_tipster_stream,collection_tipster_stream.length/100)
  }


  println("Start of queries")
  val myStopWatch2 = new StopWatch()
  myStopWatch2.start
  var query_results_top_100 = Map[(Int, Int), String]()
  test_queries.foreach( query => {
    // TODO: submit parameter that tells which query model to use: language or term based
    // Combine the results of each query into one Map
    if (index_mode == "normal"){
      query_results_top_100 = query_results_top_100 ++ q_sys.query(query._1, query._2)
    }
    else {
      query_results_top_100 = query_results_top_100 ++ q_sys_sharding.query(query._1, query._2)
    }
  })
  myStopWatch2.stop
  println("Queries executed " + myStopWatch2.stopped)

  // Sort by Query ID
  val query_results_top_100_sorted = ListMap(query_results_top_100.toSeq.sortBy(key => (key._1._1, key._1._2)):_*)

  // todo: remove and instead write to file using util.InOutUtils for that
  println("results by query: ")
  query_results_top_100_sorted.foreach(result => {println(result)})

  // Evaluate results (calculate metrics)
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

  myStopWatch.stop
  println("Indexing and query processing done " + myStopWatch.stopped)

}

