package main

import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer



class QuerySystem(parsedstream:Stream[Document]) {
    
    
    /*creates the inverted index*/
    val invertedTFIndex  =tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).sorted)
    println(invertedTFIndex)
    val vocabulary=invertedTFIndex.keys
    println(vocabulary)
    val documentFrequency=invertedTFIndex.mapValues(list=>list.length) //Map from word (===token) to its document frequency
    println(documentFrequency)
    var documentLength = parsedstream.map(d =>(d.name,tokenListFiltered(d.content).length)).toMap //Map from doc to its length
    println(documentLength)

    def query(querystring:String)={
      val tokenList= tokenListFiltered(querystring)
      val candidateDocs=tokenList.flatMap(token=>invertedTFIndex.getOrElse(token,List())).map(pair=>pair._1).distinct
      candidateDocs.map(candidateDoc=>(candidateDoc,scoring(querystring,candidateDoc))).sortBy(-_._2)
    }
    
    
    
    /*Input a eg content of doc, output a list of tokens without stopwords and stemmed*/
    def tokenListFiltered(doccontent: String) = StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))
    
    /*tfTuples returns for an input stream of documents the list of postings including termfrequencies*/
    def tfTuples ={
      parsedstream.flatMap(d =>tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTuple(tk,d.name, lst.length)}).toList
    }
    
    def scoring(querystring:String,doc:String)={
      val queryTokenList=tokenListFiltered(querystring)
      //val scorings=queryTokenList.map(token=>invertedTFIndexReturnTF(token,doc))
      val scorings=queryTokenList.map(token=>math.log(invertedTFIndexReturnTF(token,doc)+1)/documentLength(doc).toDouble)
      
      val result=scorings.sum
      result
    }
    
    /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
    def invertedTFIndexReturnTF(token:String,doc:String)={
      val res=invertedTFIndex.getOrElse(token,List()).filter(_._1==doc)
      res.map(pair=>pair._2).sum
    }
    
  
}