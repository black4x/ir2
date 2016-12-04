package main

import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer

import scala.collection.Map



class QuerySystem(parsedstream:Stream[Document]) {

  val myStopWatch = new StopWatch()

  /*creates the inverted index*/
  myStopWatch.start
  // tfTuples is a function
  val invertedTFIndex = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
  myStopWatch.stop
  println("Time elapsed to create invertedTFIndex:%s".format(myStopWatch.stopped))

  myStopWatch.start
  val vocabulary=invertedTFIndex.keys
  myStopWatch.stop
  println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))


  myStopWatch.start
  val documentFrequency: Map[String, Double] = invertedTFIndex.mapValues(list=>list.length) //Map from word (===token) to its document frequency
  myStopWatch.stop
  println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
  //    println(documentFrequency.toList.sortBy(-_._2))

  myStopWatch.start
  //stores doc0->(doclength,#distinct words) Doc ID -> Doc Length, #distinct words
  var documentLength = parsedstream.map(d =>(d.name,(tokenListFiltered(d.content).length,tokenListFiltered(d.content).distinct.length))).toMap //Map from doc to its length
  myStopWatch.stop
  println("Time elapsed to create documentLength:%s".format(myStopWatch.stopped))
  val nDocs=documentLength.size

  def query(queryID: Int, querystring: String): Map[(Int, Int), String]= {
    val tokenList= tokenListFiltered(querystring)
    val candidateDocs=tokenList.flatMap(token=>invertedTFIndex.getOrElse(token,List())).map(pair=>pair._1).distinct
    candidateDocs.map(candidateDoc=>(candidateDoc,scoring(tokenList,candidateDoc))).sortBy(-_._2).zip(Stream from 1).take(100)
      .map(result_tuple => ((queryID, result_tuple._2), result_tuple._1._1)).toMap
  }

//obsolete
  /*
  def query2(queryID: Int, querystring: String) {
    val tokenList= tokenListFiltered(querystring)
    val candidateDocs=tokenList.flatMap(token=>invertedTFIndex.getOrElse(token,List())).map(pair=>pair._1).distinct
    candidateDocs.map(candidateDoc=>(candidateDoc,scoring(tokenList,candidateDoc))).sortBy(-_._2).zip(Stream from 1).take(100)
  }
*/
  /*Input a eg content of doc, output a list of tokens without stopwords and stemmed*/
  def tokenListFiltered(doccontent: String) = StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))

  /*tfTuples returns for an input stream of documents the list of postings including term frequencies*/
  def tfTuples ={
    parsedstream.flatMap(d =>tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTuple(tk,d.name, lst.length)})
  }

  def log2(x:Double):Double= math.log(x)/math.log(2)

  def scoring(queryTokenList:Seq[String],doc:String) = {
    //val scorings=queryTokenList.map(token=>invertedTFIndexReturnTF(token,doc))
    //val scorings=queryTokenList.map(token=>math.log(invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))

    //the following function normalized the tf with the document length. Hence longer docs are not favored.
    val scorings=queryTokenList.map(token=>log2((invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))*
      (log2(nDocs)-log2(documentFrequency.getOrElse(token, 1.0))))

    val result = scorings.sum
    result
  }

  /*
  val scorings=queryTokenList.map(token=>log2((invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))*
    (log2(nDocs)-log2(documentFrequency(token))))
*/


  /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
  def invertedTFIndexReturnTF(token:String,doc:String)={
    val res=invertedTFIndex.getOrElse(token,List()).filter(_._1==doc)
    res.map(pair=>pair._2).sum
  }

}