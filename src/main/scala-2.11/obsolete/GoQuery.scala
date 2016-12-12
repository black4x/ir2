package obsolete

import Evaluation.QueryEvaluation
import Indexing.{QSysDocMap, QSysDocMapAndDocSharding, QSysNoIndex}
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream}
import ch.ethz.dal.tinyir.util.StopWatch
import utils.InOutUtils

import scala.collection.Map
import scala.collection.immutable.ListMap

/**
  * Created by Ralph on 30/11/16.
  */
object GoQuery extends App {

  // Todo: Read parameters from console: sharding or not, compression or not, run with test queries, run with real queries
  val DOC_SHARDING = "shard"
  val INDEX_NORMAL = "normal"
  val NO_INDEX = "noindex"
  val VALIDATION_MODE = "vali"
  val TEST_MODE = "test"
  val TERM_BASED = "t" // t = term based, l = language model
  val LANGUAGE = "l"

  // Set default parameters
  var runMode = TEST_MODE//VALIDATION_MODE
  var model = TERM_BASED
  var indexMode = NO_INDEX//DOC_SHARDING //INDEX_NORMAL, NO_INDEX

  val myStopWatch = new StopWatch()
  myStopWatch.start

  val path : String = "data"
  var collection_tipster_stream = new TipsterStream(path).stream.take(1000)

  val relevance_judgement_stream = DocStream.getStream("data/relevance-judgements.csv")     //new FileInputStream("data/relevance-judgements.csv")
  val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)
  //relevance_judgement.foreach(rj => println(rj))

  // Get list of query IDs and their titles (query ID needed for submission format!)
  val query_stream_validation = DocStream.getStream("data/questions-descriptions.txt")
  val query_stream_test = DocStream.getStream("data/test-questions.txt")
  var queries = List[(Int, String)]()

  if (runMode == VALIDATION_MODE) {
    queries = InOutUtils.getValidationQueries(query_stream_validation)
  }
  else {
    queries = InOutUtils.getTestQueries(query_stream_test)
  }


  // Create the Inverted Index for the document collection (either normal or with document sharding)
  var q_sys: QSysDocMap = null
  var q_sys_sharding: QSysDocMapAndDocSharding = null
  var q_sys_noindex: QSysNoIndex = null
  if (indexMode == INDEX_NORMAL) {
    q_sys = new QSysDocMap(collection_tipster_stream)
  }
  else if (indexMode == DOC_SHARDING){
    q_sys_sharding = new QSysDocMapAndDocSharding(collection_tipster_stream,collection_tipster_stream.length/5)
  }
  else if (indexMode == NO_INDEX){
    q_sys_noindex = new QSysNoIndex(collection_tipster_stream)
  }


  println("Start of queries")
  val myStopWatch2 = new StopWatch()
  myStopWatch2.start
  var query_results_top_100 = Map[(Int, Int), String]()
  queries.foreach( query => {
    // TODO: submit parameter that tells which query model to use: language or term based
    // Combine the results of each query into one Map
    if (indexMode == INDEX_NORMAL){
      query_results_top_100 = query_results_top_100 ++ q_sys.query(query._1, query._2)
    }
    else if (indexMode == DOC_SHARDING) {
      query_results_top_100 = query_results_top_100 ++ q_sys_sharding.query(query._1, query._2)
    }
    else if (indexMode == NO_INDEX) {
      query_results_top_100 = query_results_top_100 ++ q_sys_noindex.query(query._1, query._2)
    }
  })
  myStopWatch2.stop
  println("Queries executed " + myStopWatch2.stopped)

  // Sort by Query ID
  val query_results_top_100_sorted = ListMap(query_results_top_100.toSeq.sortBy(key => (key._1._1, key._1._2)):_*)


  //todo: remove
  println("results by query: ")
  query_results_top_100_sorted.foreach(result => {println(result)})


  if (runMode == VALIDATION_MODE) {
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
  }
  // Write results of the 10 queries to file if run mode is TEST
  else{
    val filename = "ranking-" + model + "-28.run"
    InOutUtils.saveResults(query_results_top_100_sorted, filename)
  }


  myStopWatch.stop
  println("Indexing and query processing done " + myStopWatch.stopped)

}

