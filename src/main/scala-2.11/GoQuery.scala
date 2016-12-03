import java.io.FileInputStream

import ch.ethz.dal.tinyir.io.{TipsterStream, ZipDirStream}
import ch.ethz.dal.tinyir.io.ZipDirStream
import main.QuerySystem
import utils.InOutUtils

/**
  * Created by Ralph on 30/11/16.
  */
object GoQuery extends App {


  val path : String = "data"
  var parsedstream = new TipsterStream(path).stream
  parsedstream=parsedstream.take(100)

  val relevance_judgement_stream = new FileInputStream("data/relevance-judgements.csv")
  val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)


  // todo: get the title from relevance judgement as query
  // todo: then call the query and specify which model to use. The constructor of QuerySystem create the inverted index
  // todo: At the end submit result and relevance judgement to the evaluation class

  val qs=new QuerySystem(parsedstream)
  println(qs.query("A court ruled Friday"))

}
