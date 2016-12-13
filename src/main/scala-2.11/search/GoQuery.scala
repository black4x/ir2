package search

import Evaluation.QueryEvaluation
import Indexing.{QSysDocMapAndDocSharding, QSysNoIndex}
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream}
import ch.ethz.dal.tinyir.util.StopWatch
import utils.InOutUtils

import scala.collection.Map
import scala.collection.immutable.ListMap

/**
  * Created by Ralph on 30/11/16.
  */
object GoQuery extends App {

  val VALIDATION_MODE = "vali"
  val TEST_MODE = "test"
  val INDEX = "index"
  val NO_INDEX = "noindex"

  val TM = "t"
  val LM = "l"

  // Set default parameters
  var runMode = VALIDATION_MODE
  var indexMode = INDEX

  val myStopWatch = new StopWatch()
  myStopWatch.start

  val path : String = "data"
  var collection_tipster_stream = new TipsterStream(path).stream.take(30000)

  val relevance_judgement_stream = DocStream.getStream("data/relevance-judgements.csv")     //new FileInputStream("data/relevance-judgements.csv")
  val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)

  // Get list of query IDs and their titles (query ID needed for submission format!)
  val query_stream_validation = DocStream.getStream("data/questions-descriptions.txt")
  val query_stream_test = DocStream.getStream("data/test-questions.txt")
  var queries = List[(Int, String)]()

  // Get validation or test queries depending on runMode
  if (runMode == VALIDATION_MODE) {
    queries = InOutUtils.getValidationQueries(query_stream_validation)
  }
  else {
    queries = InOutUtils.getTestQueries(query_stream_test)
  }

  // Create the Inverted Index for the document collection or use no index
  var q_sys: QSysDocMapAndDocSharding  = null
  var q_sys_noindex: QSysNoIndex = null
  if (indexMode == INDEX){
    q_sys = new QSysDocMapAndDocSharding(collection_tipster_stream,collection_tipster_stream.length/10)
    executeQueries(TM) // run queries using term based model and save/validate results
    executeQueries(LM) // run queries using language model and save/validate results
  }
  else if (indexMode == NO_INDEX){
    q_sys_noindex = new QSysNoIndex(collection_tipster_stream)
    executeQueriesNoIndex(LM) //only Language Model without Index
  }

  myStopWatch.stop
  println("Indexing and query processing done " + myStopWatch.stopped)


//------ end of execution --------//

  def executeQueries(model: String): Unit = {
    if (model == TM) println(" ------------------ Term-based Model")
    else println(" ------------------ Language Model")

    var queryResults = Map[(Int, Int), String]()

    queries.foreach(q => {
      myStopWatch.start
      if (model == TM) queryResults = queryResults ++ q_sys.query(q._1, q._2, TM)
      else queryResults = queryResults ++ q_sys.query(q._1, q._2, LM)
      myStopWatch.stop
      println("Query [" + q._1 + "] executed: " + myStopWatch.stopped)
    })

    // Sort by Query ID
    val results_sorted = ListMap(queryResults.toSeq.sortBy(key => (key._1._1, key._1._2)): _*)

    if (runMode == VALIDATION_MODE) {
      //println("results by query: ")
      //results_sorted.foreach(result => {println(result)})
      InOutUtils.evalResuts(results_sorted)
    }
    else {
      val filename = "ranking-" + model + "-28.run"
      InOutUtils.saveResults(results_sorted, filename)
    }
  }

  def executeQueriesNoIndex(model: String): Unit = {
    if (model == TM) println(" ------------------ Term-based Model")
    else println(" ------------------ Language Model")

    var queryResults = Map[(Int, Int), String]()

    queries.foreach(q => {
      myStopWatch.start
      queryResults = queryResults ++ q_sys_noindex.query(q._1,q._2)
      myStopWatch.stop
      println("Query [" + q._1 + "] executed: " + myStopWatch.stopped)
    })

    // Sort by Query ID
    val results_sorted = ListMap(queryResults.toSeq.sortBy(key => (key._1._1, key._1._2)): _*)

    //only valution run for no_index
    InOutUtils.evalResuts(results_sorted)

  }

}

