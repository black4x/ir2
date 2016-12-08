package Indexing

import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import main.{MyTokenizer, TfTuple}

import scala.collection.Map

class DocShard2(var partstream:Stream[Document],startindex:Int) {

    val myStopWatch = new StopWatch()
    myStopWatch.start

    //    val docMap: Map[String, Int] = partstream.map(d => d.name).zip(Stream from 1).map(doc_tuple =>  doc_tuple._1 -> doc_tuple._2).toMap
    //    val documentLength = partstream.map(d =>(getDocID(d.name),(MyTokenizer.tokenListFiltered(d.content).length,MyTokenizer.tokenListFiltered(d.content).distinct.length))).toMap //Map from doc to its length

    var docMap=Map[String, Int]()
    var documentLength=Map[Int, (Int, Int)]()
    var counter=startindex

    partstream.foreach{ doc =>
        docMap +=((doc.name,counter))
        counter=counter+1
        documentLength+=(getDocID(doc.name)->(MyTokenizer.tokenListFiltered(doc.content).length,MyTokenizer.tokenListFiltered(doc.content).distinct.length))
    }
    myStopWatch.stop
    println("Time elapsed to create docMap and documentLength:%s documentLength.size:%d".format(myStopWatch.stopped,documentLength.size))
    val nDocs=documentLength.size


    /*creates the inverted index*/
    val invertedTFIndex:Map[String,List[(Int,Int)]] = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
    myStopWatch.stop
    println("Time elapsed to create invertedTFIndex:%s size:".format(myStopWatch.stopped))

    myStopWatch.start
    val vocabulary=invertedTFIndex.keys
    myStopWatch.stop
    //    println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))

    myStopWatch.start
    val documentFrequency=invertedTFIndex.mapValues(list=>list.length) //Map from word (===token) to its document frequency
    myStopWatch.stop
    //    println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
    //    println(documentFrequency.toList.sortBy(-_._2))



    def tfTuples =partstream.flatMap(d =>MyTokenizer.tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTuple2(tk,getDocID(d.name), lst.length)})

    def getDocID(docName: String): Int = {
        // must always find an entry
        return docMap.getOrElse(docName,0)
    }

}