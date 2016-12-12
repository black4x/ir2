package search

import Evaluation.QueryEvaluation
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream}
import ch.ethz.dal.tinyir.processing.{StopWords, Tokenizer, XMLDocument}
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import utils.InOutUtils

import scala.collection.Map
import scala.collection.immutable.ListMap

case class TermDocItem(termHash: Int, docInt: Int, tf: Int)

case class DocItem(docInt: Int, tf: Int)

// 1 run through entire collection!
object LazyIndex extends App {

  val TOTAL_NUMBER = 10000
  // global number of docs to take into consideration MAX = 100000

  // getNumberOfShards(total number, shard size in %)
  val shardsNumber = getNumberOfShards(TOTAL_NUMBER, 10)
  val shardSize = TOTAL_NUMBER / shardsNumber

  println(shardsNumber + " shards")

  var stream = new TipsterStream("data").stream.take(TOTAL_NUMBER)

  // token_id -> (token, raw tokens count in document, distinct tokens in doc)
  var docInfoMap = Map[Int, (String, Int, Int)]()

  // token_id -> Stream of DocItem(docInt: Int, tf: Int)
  var invIndexMap = Map[Int, Stream[DocItem]]()

  val myStopWatch = new StopWatch()
  myStopWatch.start

  // ONLY ONE time runs through entire collection
  var chunkLengthTotal = 0
  for (i <- 0 until shardsNumber) {
    val streamChunk = stream.slice(i * shardSize, i * shardSize + shardSize)
    invIndexMap = merge(invIndexMap, createInvertedIndex(streamChunk))
    printStat(i)
  }

  myStopWatch.stop
  println("index " + myStopWatch.stopped + " tokens = " + invIndexMap.size)
  println("start queries")

  myStopWatch.start

  // TODO: For now take only first query for test (it reads all query but we do take(1) later
  //val oneQuery = InOutUtils.getValidationQueries(DocStream.getStream(path + "/questions-descriptions.txt")).head
  var allQueries: List[(Int, String)] = InOutUtils.getValidationQueries(DocStream.getStream("data/questions-descriptions.txt"))
  var queryResults = Map[(Int, Int), String]()
  //var queryResults = query(oneQuery, termModelScoring)

  // Todo: remove later. If only one query shall be executed for testing do this:
  allQueries = allQueries.take(1)

  allQueries.foreach(q => {
    queryResults = queryResults ++ query(q, languageModelScoring)
  })

  myStopWatch.stop
  println("Queries executed: " + myStopWatch.stopped)

  // Sort by Query ID
  val results_sorted = ListMap(queryResults.toSeq.sortBy(key => (key._1._1, key._1._2)): _*)

  // TODO: only in VALIDATION mode (for 40 queries)
  Utils.showResults(results_sorted)


  //TODO, print results for 10 queries to file. Currently deactivated
  // If run mode is "TEST" (proessing the 10 queries) save results to file
  val model = "t" // or l for language
  if (1 == 2) {
    val filename = "ranking-" + model + "-28.run"
    InOutUtils.saveResults(results_sorted, filename)
  }

  println("results by query: ")
  results_sorted.foreach(result => {
    println(result)
  })


  // ----------------------- END OF EXECUTION !!!! ----------------------------------------

  def languageModelScoring(docId: Int, queryTokenList: Seq[Int]): Double = {
    val lambda = 0.6f
    queryTokenList.map(tokenId => {
      val tokenFrequencyInDoc = getTermFrequencyFromInvIndex(tokenId, docId).toDouble
      val numberOfTokensInDoc = getDocLength(docId).toDouble
      lambda * (tokenFrequencyInDoc / numberOfTokensInDoc) + (1 - lambda) * lmSmoothingNumber(tokenId, sumAllTokens())
    }).product
  }

  def termModelScoring(docId: Int, queryTokenList: Seq[Int]): Double = {
    //println("scoring doc: " + docId)
    queryTokenList.map(token =>
      log2((getTermFrequencyFromInvIndex(token, docId) + 1.0) / (getDocLength(docId).toDouble + getDocDistinctTkn(docId) /*getDistinctTokensNumberForDoc(docId)*/)) *
        (log2(TOTAL_NUMBER) - log2(invIndexMap.getOrElse(token, Stream.Empty).length))).sum

  }

  def getDistinctTokensNumberForDoc(docId: Int): Int =
    invIndexMap.count(item => item._2.exists(x => x.docInt == docId))

  def lmSmoothingNumber(token: Int, totalCount: Double): Double =
    termFrequencyInCollection(token).toDouble / totalCount

  def termFrequencyInCollection(tokenId: Int): Int =
    invIndexMap.getOrElse(tokenId, Stream.Empty).map(item => item.tf).sum

  def getTermFrequencyFromInvIndex(tokenId: Int, docId: Int): Int =
    invIndexMap.getOrElse(tokenId, Stream.Empty)
      .find(item => item.docInt == docId) match {
      case Some(d) => d.tf
      case None => 0
    }

  def getCandidateDocumentsIds(queryTokensIds: Seq[Int]): Set[Int] = {
    queryTokensIds.map(tokenId => {
      invIndexMap.getOrElse(tokenId, Stream.Empty).toSet
    }).reduce(_ ++ _).map(x => x.docInt)
  }

  def createInvertedIndex(stream: Stream[XMLDocument]): Map[Int, Stream[DocItem]] =
    stream.flatMap(doc => {
      val filteredContent = Utils.filter(doc.content)
      val distinctTokensInDoc: Int = filteredContent.distinct.length
      docInfoMap += (doc.name.hashCode -> (doc.name, filteredContent.length, distinctTokensInDoc))
      filteredContent.groupBy(identity)
        //.filter(x => x._2.length > tokenFrequencyMin)
        .map {
        case (tk, lst) => TermDocItem(tk.hashCode, doc.name.hashCode, lst.length)
      }
    })
      .groupBy(_.termHash)
      // .filter(q => q._2.size < docFrequencyMax)
      .mapValues(_.map(tokenDocItem => DocItem(tokenDocItem.docInt, tokenDocItem.tf)).sortBy(x => x.docInt))

  def merge(i1: Map[Int, Stream[DocItem]], i2: Map[Int, Stream[DocItem]]): Map[Int, Stream[DocItem]] =
    i1 ++ i2.map { case (token, stream) => token -> (stream ++ i1.getOrElse(token, Stream.Empty)).distinct }

  def printUsedMem(): Unit = {
    val runtime = Runtime.getRuntime

    val usedMem = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024)
    val maxMem = runtime.maxMemory / (1024 * 1024)
    val p = usedMem.toDouble / maxMem.toDouble * 100
    val warning = if (p > 90) {
      " !!!!!!!! > 90% "
    } else {
      ""
    }
    println(" memory used:  " + f"$p%2.0f" + "% " + warning)
  }

  def printStat(currentShard: Int): Unit = {
    val p = (currentShard / shardsNumber.toDouble) * 100
    print(f"$p%2.0f" + "% ")
    printUsedMem()
  }

  def getDocNameByHashCode(hashCode: Int): String =
    docInfoMap.getOrElse(hashCode, ("", 0, 0))._1

  def sumAllTokens(): Int =
    docInfoMap.map(x => x._2._2).sum

  def getDocLength(docId: Int): Int =
    docInfoMap.getOrElse(docId, ("", 0, 0))._2

  def getDocDistinctTkn(docId: Int): Int =
    docInfoMap.getOrElse(docId, ("", 0, 0))._3

  def query(query: (Int, String), scoringFunction: (Int, Seq[Int]) => Double): Map[(Int, Int), String] = {
    val content = query._2
    val queryID = query._1
    val queryTokensIds = Utils.filter(content).map(token => token.hashCode)

    val candidatesIDs = getCandidateDocumentsIds(queryTokensIds)

    // Scoring (sort results descending and return only top 100 results)
    val results = candidatesIDs.map(docId => docId -> scoringFunction(docId, queryTokensIds))
      .toSeq.sortBy(x => -x._2).take(100).zip(Stream from 1)

    results.map(x => ((queryID, x._2), getDocNameByHashCode(x._1._1))).toMap
  }

  def log2(x: Double): Double = math.log(x) / math.log(2)

  def getNumberOfShards(totalNumber: Int, percent: Short): Int = {
    val n = (totalNumber * (percent.toFloat / 100)).toInt
    if (n == 0) 1 else n
  }

}


