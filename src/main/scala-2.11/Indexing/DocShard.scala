package Indexing

import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import main.MyTokenizer

import scala.collection.Map

class DocShard(var partstream:Stream[Document], startindex:Int) {

    val myStopWatch = new StopWatch()
    myStopWatch.start

    var docMap=Map[String, Int]()
    var documentMapWithTokenCount=Map[Int, (Int, Int)]()
    var counter=startindex

    var numberOfAllTokens = 0;

    partstream.foreach{ doc =>
        docMap +=((doc.name,counter))
        counter=counter+1
        val tokensInDoc = MyTokenizer.tokenListFiltered(doc.content)
        val tokensNumber = tokensInDoc.length
        documentMapWithTokenCount+=(getDocID(doc.name)->(tokensNumber,tokensInDoc.distinct.length))
        numberOfAllTokens+=tokensNumber
    }
    myStopWatch.stop
    //println("Time elapsed to create docMap and documentLength:%s documentLength.size:%d".format(myStopWatch.stopped,documentMapWithTokenCount.size))
    val nDocs=documentMapWithTokenCount.size


    /*creates the inverted index*/
    val invertedTFIndex:Map[String,List[(Int,Int)]] = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
    myStopWatch.stop
    //println("Time elapsed to create invertedTFIndex:%s size:".format(myStopWatch.stopped))

    myStopWatch.start
    val vocabulary=invertedTFIndex.keys
    myStopWatch.stop
    //    println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))

    myStopWatch.start
    val documentFrequency=invertedTFIndex.mapValues(list=>list.length) //Map from word (===token) to its document frequency
    myStopWatch.stop
    //    println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
    //    println(documentFrequency.toList.sortBy(-_._2))



    def tfTuples =partstream.flatMap(d =>MyTokenizer.tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTupleDocID(tk,getDocID(d.name), lst.length)})

    def getDocID(docName: String): Int = {
        // must always find an entry
        docMap.getOrElse(docName,0)
    }

}