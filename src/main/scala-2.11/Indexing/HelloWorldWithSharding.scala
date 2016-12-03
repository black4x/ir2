package main

import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer

object HelloWorld {
  def main(args: Array[String]) {     

    val path : String = "C:\\Users\\Thomas\\Desktop\\IRProject2\\documents"
    var parsedstream = new TipsterStream(path).stream 
    parsedstream=parsedstream.take(50000)
 
   
//    for (doc <- parsedstream) {
//      println("%s,%s".format(doc.name,doc.content))
//     
//    }
    
//With 30000 Docs
//Time elapsed to create invertedTFIndex:in 94.44938024 sec.
//Time elapsed to create vocabulary:in 6.15566E-4 sec.
//Time elapsed to create documentFrequency:in 4.0114E-4 sec.
//Time elapsed to create documentLength:in 121.94053439 sec.
    
//With 3000 Docs
//Time elapsed to create invertedTFIndex:in 9.167619676 sec.
 //Time elapsed to create vocabulary:in 6.53857E-4 sec.
//Time elapsed to create documentFrequency:in 4.02963E-4 sec.
//Time elapsed to create documentLength:in 14.519832688 sec.

    val qs=new QuerySystemWithSharding(parsedstream,30000)
    println(qs.query("Strategists for Jack Kemp's presidentialcampaign say George Bush"))
    
  }
}
  