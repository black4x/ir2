package Indexing

import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import main.{MyTokenizer, TfTupleDocName}

import scala.collection.Map


class QSysDocMap(parsedstream:Stream[Document]) {

  val myStopWatch = new StopWatch()

  /*Map Doc Names to Integer Numbers*/
  myStopWatch.start
  val docMap: Map[String, Int] = parsedstream.map(d => d.name).zip(Stream from 1).map(doc_tuple =>  doc_tuple._1 -> doc_tuple._2).toMap
  myStopWatch.stop
  println("docMap created " + myStopWatch.stopped)
  //docMap foreach (doc => println(doc))

  /*creates the inverted index*/
  myStopWatch.start
  // tfTuples is a function
  val invertedTFIndex = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
  println("Inverted TFI " + invertedTFIndex.size)
  //println(invertedTFIndex)
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
  //   println(documentFrequency.toList.sortBy(-_._2))

  myStopWatch.start
  //stores doc0->(doclength,#distinct words) Doc ID -> Doc Length, #distinct words
  var documentLength = parsedstream.map(d =>(getDocID(d.name),(tokenListFiltered(d.content).length,tokenListFiltered(d.content).distinct.length))).toMap //Map from doc to its length
  myStopWatch.stop
  println("Time elapsed to create documentLength:%s".format(myStopWatch.stopped))
  val nDocs=documentLength.size


  def query(queryID: Int, querystring: String): Map[(Int, Int), String]= {
    val tokenList= tokenListFiltered(querystring)
    val candidateDocs=tokenList.flatMap(token=>invertedTFIndex.getOrElse(token,List())).map(pair=>pair._1).distinct

    candidateDocs.map(candidateDoc=>(candidateDoc,scoring(tokenList,candidateDoc))).sortBy(-_._2).zip(Stream from 1).take(100)
      .map(result_tuple => ((queryID, result_tuple._2), getDocName(result_tuple._1._1))).toMap
  }


  /*Input is the content of doc, output a list of tokens without stopwords and stemmed*/
  def tokenListFiltered(doccontent: String) = {
    //StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))

    // Calling the custom Tokenizer instead
    MyTokenizer.tokenListFiltered(doccontent)
  }

  /*tfTuples returns for an input stream of documents the list of postings including term frequencies*/
  def tfTuples ={
    parsedstream.flatMap(d => tokenListFiltered(d.content).groupBy(identity)
      .map{ case (tk,lst) => TfTupleDocID(tk, getDocID(d.name), lst.length)})//.filter(tuple => (tuple.count > 1))
  }

  def log2(x:Double):Double= math.log(x)/math.log(2)

  def scoring(queryTokenList:Seq[String],doc:Int) = {

    //the following function normalized the tf with the document length. Hence longer docs are not favored.
    val scorings=queryTokenList.map(token=>log2((invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))*
      (log2(nDocs)-log2(documentFrequency.getOrElse(token, 1.0))))

    val result = scorings.sum
    result
  }


  /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
  def invertedTFIndexReturnTF(token:String,doc:Int)={
    val res=invertedTFIndex.getOrElse(token,List()).filter(_._1==doc)
    res.map(pair=>pair._2).sum
  }

  /* Get the interger number previously assigned to each Doc Name*/
  def getDocID(docName: String): Int = {
    // must always find an entry
    return docMap.getOrElse(docName,0)
  }

  def getDocName(docID: Int): String ={
    // can only be one match for submitted Doc ID
    val filteredDocs = docMap.filter(doc_tuple => (doc_tuple._2 == docID)).take(1).map(relevant_doc => relevant_doc._1)
    return filteredDocs.head
  }

}