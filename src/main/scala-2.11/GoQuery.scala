import java.io.FileInputStream

import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream, ZipDirStream}
import ch.ethz.dal.tinyir.processing
import ch.ethz.dal.tinyir.processing.{TipsterParse, XMLDocument}
import main.QuerySystem
import utils.InOutUtils

import scala.collection.Map

/**
  * Created by Ralph on 30/11/16.
  */
object GoQuery extends App {

  val path : String = "data"
  var collection_tipster_stream = new TipsterStream(path).stream
  collection_tipster_stream = collection_tipster_stream.take(100)

  val relevance_judgement_stream = DocStream.getStream("data/relevance-judgements.csv")     //new FileInputStream("data/relevance-judgements.csv")
  val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)

  // Get the queries = title from questions-descriptions.txt (remove "Topic:")
  // this crashes as there are invalid xml tags!!
  //val query_tipster_parse = new TipsterParse(DocStream.getStream("data/questions-descriptions.txt"))

  // Get list of query IDs and their titles (query ID needed for submission format!)
  val query_stream = DocStream.getStream("data/questions-descriptions.txt")
  val test_queries = InOutUtils.getTestQueries(query_stream)



  // todo: then call the query and specify which model to use. The constructor of QuerySystem create the inverted index
  // todo: At the end submit result and relevance judgement to the evaluation class

  // Create the Inverted Index for the document collection
  val q_sys = new QuerySystem(collection_tipster_stream)

  // Start of query
  println("start of query")
  var query_results_top_100 = Map[(Int, Int), String]()
  test_queries.foreach( query => {

    query_results_top_100 = q_sys.query(query._1, query._2)

  })


  query_results_top_100.foreach(result => {

    println(result)

  })



  //println(qs.query("A court ruled Friday"))

}
