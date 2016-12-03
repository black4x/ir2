import java.io.FileInputStream

import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream, ZipDirStream}
import ch.ethz.dal.tinyir.processing
import ch.ethz.dal.tinyir.processing.{TipsterParse, XMLDocument}
import main.QuerySystem
import utils.InOutUtils

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

  val query_stream = DocStream.getStream("data/questions-descriptions.txt")
  //scala.io.Source.fromInputStream(query_stream).getLines()




  // todo: then call the query and specify which model to use. The constructor of QuerySystem create the inverted index
  // todo: At the end submit result and relevance judgement to the evaluation class

  val qs=new QuerySystem(collection_tipster_stream)
  println(qs.query("A court ruled Friday"))

}
