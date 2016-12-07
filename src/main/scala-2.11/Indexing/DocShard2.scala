package Indexing

import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import main.TfTuple

import scala.collection.Map

class DocShard2(var partstream:Stream[Document]) {

    val myStopWatch = new StopWatch()

    /*Map Doc Names to Integer Numbers*/
    myStopWatch.start
    val docMap: Map[String, Int] = partstream.map(d => d.name).zip(Stream from 1).map(doc_tuple =>  doc_tuple._1 -> doc_tuple._2).toMap
    myStopWatch.stop
    println("docMap created " + myStopWatch.stopped)
    //docMap foreach (doc => println(doc))

    myStopWatch.start
    /*creates the inverted index*/
    val invertedTFIndex: Map[String, List[(Int, Int)]] = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
    myStopWatch.stop
    println("Time elapsed to create invertedTFIndex:%s".format(myStopWatch.stopped))
    
    myStopWatch.start
    val vocabulary=invertedTFIndex.keys
    myStopWatch.stop
    println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))
    
    myStopWatch.start
    val documentFrequency=invertedTFIndex.mapValues(list=>list.length) //Map from word (===token) to its document frequency
    myStopWatch.stop
    println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
   //    println(documentFrequency.toList.sortBy(-_._2))
    
    myStopWatch.start
    //stores doc0->(doclength,#distinct words)
    val documentLength: Map[Int, (Int, Int)] = partstream.map(d =>(getDocID(d.name),(tokenListFiltered(d.content).length,tokenListFiltered(d.content).distinct.length))).toMap //Map from doc to its length
    myStopWatch.stop
    println("Time elapsed to create documentLength:%s".format(myStopWatch.stopped)) 
    val nDocs=documentLength.size
        
    def tfTuples ={partstream.flatMap(d =>tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTuple2(tk,getDocID(d.name), lst.length)})}
      
    def tokenListFiltered(doccontent: String) = StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))

    /* Get the interger number previously assigned to each Doc Name*/
    def getDocID(docName: String): Int = {
        // must always find an entry
        return docMap.getOrElse(docName,0)
    }

}