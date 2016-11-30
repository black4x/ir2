package main

import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer

object HelloWorld {
  def main(args: Array[String]) {     
    
    val path : String = "C:\\Users\\Thomas\\Desktop\\IRProject2\\documents"
    var parsedstream = new TipsterStream(path).stream 
    parsedstream=parsedstream.take(3)


    val qs=new QuerySystem(parsedstream)
    println(qs.query("A court ruled Friday"))
    
  }
}


/*for (doc <- parsedstream) {
  println("%s,%s".format(doc.name,doc.content))

}
*/