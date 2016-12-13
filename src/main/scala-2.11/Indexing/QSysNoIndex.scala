package Indexing

import java.io.InputStream

import ch.ethz.dal.tinyir.alerts.ScoredResult
import ch.ethz.dal.tinyir.processing.{Document, TipsterParse}
import ch.ethz.dal.tinyir.util.StopWatch
import main.MyTokenizer
import utils.TopResults

import scala.collection.Map

/**
  * Created by Ralph on 11/12/16.
  */
class QSysNoIndex (parsedstream:Stream[Document]) {

  val myStopWatch = new StopWatch()
  myStopWatch.start

  val top100 = new TopResults(100)

  def query(queryID: Int, querystring: String): Map[(Int, Int), String]= {

    val tokenList = tokenListFiltered(querystring)

    parsedstream.foreach(is => {

      val tokens = is.tokens
      val termFrequencyMap = getTermFrequencyMap(tokenList.toArray, tokens)
      if (termFrequencyMap.values.sum > 0) {
        val queryProbability = frequencyProductNoSmoothing(tokens.size, termFrequencyMap)
        top100.add(ScoredResult(is.name, queryProbability))
        //println(queryProbability)
      }
    })
    var result_formatted = Map[(Int, Int), String]()
    var rank = 0
    top100.results.foreach(result => {
      rank += 1
      result_formatted += ((queryID, rank) -> result.title)
    })

    println(top100.results)
    return result_formatted
  }

  def getTermFrequencyMap(query: Array[String], tokens: List[String]): Map[String, Int] = {
    query.map(t => (t, tokens.count(word => word.toLowerCase() == t))).toMap
  }

  def frequencyProductNoSmoothing(size: Int, termFrequencyMap: Map[String, Int]): Double = {
    termFrequencyMap.values.filter(tf => tf != 0).map(tf => tf.toDouble / size.toDouble).product
  }

  /*Input is the content of doc, output a list of tokens without stopwords and stemmed*/
  def tokenListFiltered(doccontent: String) = {
    //StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))

    // Calling the custom Tokenizer instead
    MyTokenizer.tokenListFiltered(doccontent)
  }


}


/*
val myStopWatch = new StopWatch()
myStopWatch.start

val top100 = new TopResults(100)

def query(queryID: Int, querystring: String): Map[(Int, Int), String]= {

  val tokenList = tokenListFiltered(querystring)

  parsedstream.foreach(is => {

  val tokens = is.tokens
  val termFrequencyMap = getTermFrequencyMap(tokenList.toArray, tokens)
  if (termFrequencyMap.values.sum > 0) {
  val queryProbability = frequencyProductNoSmoothing(tokens.size, termFrequencyMap)
  top100.add(ScoredResult(is.name, queryProbability))
  //println(queryProbability)
}
})
  var result_formatted = Map[(Int, Int), String]()
  var rank = 0
  top100.results.foreach(result => {
  rank += 1
  result_formatted += ((queryID, rank) -> result.title)
})

  println(top100.results)
  return result_formatted
}

  def getTermFrequencyMap(query: Array[String], tokens: List[String]): Map[String, Int] = {
  query.map(t => (t, tokens.count(word => word.toLowerCase() == t))).toMap
}

  def frequencyProductNoSmoothing(size: Int, termFrequencyMap: Map[String, Int]): Double = {
  termFrequencyMap.values.filter(tf => tf != 0).map(tf => tf.toDouble / size.toDouble).product
}

  /*Input is the content of doc, output a list of tokens without stopwords and stemmed*/
  def tokenListFiltered(doccontent: String) = {
  //StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))

  // Calling the custom Tokenizer instead
  MyTokenizer.tokenListFiltered(doccontent)
}
*/
