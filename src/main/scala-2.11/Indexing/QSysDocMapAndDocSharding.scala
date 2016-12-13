package Indexing

import ch.ethz.dal.tinyir.processing._
import main.MyTokenizer

import scala.collection.mutable.ListBuffer


class QSysDocMapAndDocSharding(var wholestream: Stream[Document], chuncksize: Int = 30000) {

  // Buffers for scoring models (metrics that are the same for each candidate doc)
  var termFrequencyCollectionMap = Map[String, Int]()
  var docFrequencyTokenMap = Map[String, Int]()

  private val runtime = Runtime.getRuntime

  print("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / 1000000)
  print("** Free Memory:  " + runtime.freeMemory / 1000000)
  print("** Total Memory: " + runtime.totalMemory / 1000000)
  println("** Max Memory:   " + runtime.maxMemory / 1000000)

  var docShards = new ListBuffer[DocShard]()
  var counter = 1
  while (wholestream.nonEmpty) {

    var partstream = wholestream.take(chuncksize)
    wholestream = wholestream.drop(chuncksize)
    val docShard = new DocShard(partstream, counter)
    docShards += docShard
    counter = counter + chuncksize

    print("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / 1000000)
    print("** Free Memory:  " + runtime.freeMemory / 1000000)
    print("** Total Memory: " + runtime.totalMemory / 1000000)
    println("** Max Memory:   " + runtime.maxMemory / 1000000)
  }

  def getDocName(docID: Int): String = {
    // can only be one match for submitted Doc ID

    var loop = 0
    var filteredDoc = List[String]()
    while (loop < docShards.length) {
      val filteredDoc = docShards(loop).docMap.filter(doc_tuple => doc_tuple._2 == docID).keys.toList
      if (filteredDoc.nonEmpty) {
        return filteredDoc.head
      }
      loop = loop + 1
    }
    return filteredDoc.head
  }


  def documentFrequency(token: String): Int = {

    var docFrequencyToken = docFrequencyTokenMap.getOrElse(token, 0)
    if (docFrequencyToken == 0) {
      docFrequencyToken = docShards.toList.map(x => x.documentFrequency.getOrElse(token, 0)).sum
      docFrequencyTokenMap += (token -> docFrequencyToken)
    }

    return docFrequencyToken

    //docShards.toList.map(x => x.documentFrequency.getOrElse(token, 0)).sum
  }

  def documentLength(doc: Int) = {
    var loop = 0
    var notFound = true
    var res = (0, 0)
    while (res == (0, 0) && loop < docShards.length) {
      res = docShards(loop).documentMapWithTokenCount.getOrElse(doc, (0, 0))
      loop = loop + 1
    }
    res
  }

  val nDocs = docShards.toList.map(x => x.documentMapWithTokenCount.size).sum
  val numberOfTokensInCollection = docShards.toList.map(x => x.numberOfAllTokens).sum

  def query(queryID: Int, querystring: String, model: String): Map[(Int, Int), String] = {
    val tokenList = MyTokenizer.tokenListFiltered(querystring)
    var candidateDocs = Seq[Int]()
    var candidateDocsShardReduced = Seq[(String, Int)]()

    if (tokenList.size > 1) {
      for (loop <- docShards.indices) {
        // reduce candidate list by only picking the ones that have at least 2 query terms
        candidateDocsShardReduced = tokenList.flatMap(token => docShards(loop).invertedTFIndex.getOrElse(token, List()).map(pair => (token -> pair._1)))
        val candidateDocsReduced = candidateDocsShardReduced.groupBy(_._2).map { case (doc, lst) => (doc, lst.length) }.filter(_._2 >= 2).map(_._1)
        candidateDocs = candidateDocs.union(candidateDocsReduced.toSeq)
      }
    }
    else {
      for (loop <- docShards.indices) {
        val candidateDocsShard = tokenList.flatMap(token => docShards(loop).invertedTFIndex.getOrElse(token, List())).map(pair => pair._1).distinct
        candidateDocs = candidateDocs.union(candidateDocsShard)
      }
    }


    val res1 =
      if (model == "l") {
        candidateDocs.map(candidateDoc => (candidateDoc, languageModelScoring(candidateDoc, 0.5f, tokenList, candidateDocs))).sortBy(-_._2).zip(Stream from 1).take(100)
      } else {
        candidateDocs.map(candidateDoc => (candidateDoc, termModelScoring(tokenList, candidateDoc))).sortBy(-_._2).zip(Stream from 1).take(100)
      }
    res1.map(result_tuple => ((queryID, result_tuple._2), getDocName(result_tuple._1._1))).toMap
  }


  def log2(x: Double): Double = math.log(x) / math.log(2)

  def termModelScoring(queryTokenList: Seq[String], doc: Int) = {

    //the following function normalized the tf with the document length. Hence longer docs are not favourte. Used
    val scorings = queryTokenList.map(token => log2((invertedTFIndexReturnTF(token, doc) + 1.0) / log2(documentLength(doc)._1 + documentLength(doc)._2)) *
      (log2(nDocs) - log2(documentFrequency(token))))

    val result = scorings.sum
    result
  }

  def languageModelScoring(docId: Int, lambda: Float, queryTokenList: Seq[String], candidateDocs: Seq[Int]) =
    queryTokenList.map(token => {
      val tokenFrequencyInDoc = invertedTFIndexReturnTF(token, docId).toDouble
      val numberOfTokensInDoc = documentLength(docId)._1.toDouble
      lambda * (tokenFrequencyInDoc / numberOfTokensInDoc) + (1 - lambda) * lmSmoothingNumber(token)
    }).product


  def lmSmoothingNumber(token: String): Double =
    termFrequencyInCollection(token).toDouble / numberOfTokensInCollection.toDouble

  def termFrequencyInCollection(token: String): Int = {

    var termFrequencyCollection = termFrequencyCollectionMap.getOrElse(token,0)
    if (termFrequencyCollection == 0) {
      // Sum up term frequencies in inverted index (note: there can be serveral shards with the same token)
      termFrequencyCollection = docShards.toList.map(x => x.invertedTFIndex.getOrElse(token, List()).map(item => item._2).sum).sum
      // buffer collection frequency for token
      termFrequencyCollectionMap += (token -> termFrequencyCollection)
    }
    return termFrequencyCollection

    //candidateDocs.map(docId => invertedTFIndexReturnTF(token, docId)).sum

  }

  /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
  def invertedTFIndexReturnTF(token: String, doc: Int) = {
    var loop = 0
    var res = List[(Int, Int)]()

    while (res.isEmpty && loop < docShards.length) {
      // res=docShards(loop).invertedTFIndex.getOrElse(token,List())
      res = docShards(loop).invertedTFIndex.getOrElse(token, List()).filter(_._1 == doc)
      loop = loop + 1

    }
    res.map(pair => pair._2).sum
  }
}