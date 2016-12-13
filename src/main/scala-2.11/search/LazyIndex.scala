package search

import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream}
import ch.ethz.dal.tinyir.processing.XMLDocument
import ch.ethz.dal.tinyir.util.StopWatch
import utils.InOutUtils

import scala.collection.Map
import scala.collection.immutable.ListMap

case class TermDocItem(termHash: Int, docInt: Int, tf: Int)

case class DocItem(docInt: Int, tf: Int)

// 1 run through entire collection!
object LazyIndex extends App {
  val TOTAL_NUMBER = 100000

  val VALIDATION_MODE = "vali"
  val TEST_MODE = "test"

  val runMode = VALIDATION_MODE

  val TM = "t"
  val LM = "l"

  var termFrequencyCollectionMap = Map[Int, Int]()

  // global number of docs to take into consideration MAX = 100000

  // getNumberOfShards(total number, shard size in %)
  val shardsNumber = 100
  //getNumberOfShards(TOTAL_NUMBER, 50)
  val shardSize = TOTAL_NUMBER / shardsNumber

  println(shardsNumber + " shards")

  var stream = new TipsterStream("data").stream.take(TOTAL_NUMBER)

  // token_id -> (token, raw tokens count in document, distinct tokens in doc)
  var docInfoMap = Map[Int, (String, Int, Int)]()

  // token_id -> Stream of DocItem(docInt: Int, tf: Int)
  //var invIndexMap = Map[Int, List[DocItem]]()
  var invIndexMap = Map[Int, List[DocItem]]()

  val myStopWatch = new StopWatch()
  myStopWatch.start

  // ONLY ONE time runs through entire collection
  var chunkLengthTotal = 0
  for (i <- 0 until shardsNumber) {
    invIndexMap = merge(invIndexMap, createInvertedIndex(stream.slice(i * shardSize, i * shardSize + shardSize)))
    printStat(i)
  }

  println("merging... ")

  myStopWatch.stop
  println("index " + myStopWatch.stopped + " tokens = " + invIndexMap.size)
  printUsedMem()

  println("start queries")


  val query_stream_validation = DocStream.getStream("data/questions-descriptions.txt")
  val query_stream_test = DocStream.getStream("data/test-questions.txt")
  var queries = List[(Int, String)]()

  if (runMode == VALIDATION_MODE) {
    queries = InOutUtils.getValidationQueries(query_stream_validation)
  }
  else {
    queries = InOutUtils.getTestQueries(query_stream_test)
  }
  printUsedMem()
  executeQueries(TM)
  printUsedMem()
  executeQueries(LM)
  printUsedMem()


  // ----------------------- END OF EXECUTION !!!! ----------------------------------------

  def executeQueries(model: String): Unit = {
    if (model == TM) println(" ------------------ Term-based Model")
    else println(" ------------------ Language Model")

    var queryResults = Map[(Int, Int), String]()

    queries.foreach(q => {
      myStopWatch.start
      if (model == TM) queryResults = queryResults ++ query(q, termModelScoring)
      else queryResults = queryResults ++ query(q, languageModelScoring)
      myStopWatch.stop
      println("Query [" + q._1 + "] executed: " + myStopWatch.stopped)
    })

    // Sort by Query ID
    val results_sorted = ListMap(queryResults.toSeq.sortBy(key => (key._1._1, key._1._2)): _*)

    if (runMode == VALIDATION_MODE) {
      //println("results by query: ")
      //results_sorted.foreach(result => {println(result)})

      InOutUtils.evalResuts(results_sorted)

    } else {
      val filename = "ranking-" + model + "-28.run"
      InOutUtils.saveResults(results_sorted, filename)
    }
  }

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
      log2((getTermFrequencyFromInvIndex(token, docId) + 1.0) / log2(getDocLength(docId).toDouble + getDocDistinctTkn(docId))) *
        (log2(TOTAL_NUMBER) - log2(invIndexMap.getOrElse(token, List()).length))).sum

  }

  def getDistinctTokensNumberForDoc(docId: Int): Int =
    invIndexMap.count(item => item._2.exists(x => x.docInt == docId))

  def lmSmoothingNumber(token: Int, totalCount: Double): Double = {
    termFrequencyInCollection(token).toDouble / totalCount
  }

  def termFrequencyInCollection(tokenId: Int): Int = {

    var termFrequencyCollection = termFrequencyCollectionMap.getOrElse(tokenId,0)
    if (termFrequencyCollection == 0) {
      termFrequencyCollection = invIndexMap.getOrElse(tokenId, List()).map(item => item.tf).sum
      termFrequencyCollectionMap += (tokenId -> termFrequencyCollection)
    }
    return termFrequencyCollection
      //invIndexMap.getOrElse(tokenId, List()).map(item => item.tf).sum
  }

  def getTermFrequencyFromInvIndex(tokenId: Int, docId: Int): Int =
    invIndexMap.getOrElse(tokenId, List())
      .find(item => item.docInt == docId) match {
      case Some(d) => d.tf
      case None => 0
    }

  def getCandidateDocumentsIds(queryTokensIds: Seq[Int]): Set[Int] = {
    queryTokensIds.map(tokenId => {
      invIndexMap.getOrElse(tokenId, List()).toSet
    }).reduce(_ ++ _).map(x => x.docInt)
  }

  def createInvertedIndex(stream: Stream[XMLDocument]): Map[Int, List[DocItem]] =
    stream.flatMap(doc => {
      val filteredContent = InOutUtils.filter(doc.content)
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
      .mapValues(_.map(tokenDocItem => DocItem(tokenDocItem.docInt, tokenDocItem.tf)).sortBy(x => x.docInt).toList)

  def merge(i1: Map[Int, List[DocItem]], i2: Map[Int, List[DocItem]]): Map[Int, List[DocItem]] =
    i1 ++ i2.map { case (token, stream) => token -> sumDocItemList(stream, i1.getOrElse(token, List())) }

  def sumDocItemList(l1: List[DocItem], l2: List[DocItem]): List[DocItem] =
    (l1 ++ l2).groupBy(_.docInt).mapValues(_.map(_.tf).sum).toList map Function.tupled((id, tf) => DocItem(id, tf))

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
    println(" memory used:  (" + f"$usedMem%4d" + " from" + f"$maxMem%6d" +"): " + f"$p%2.0f" +"%"+ warning)
  }

  def printStat(currentShard: Int): Unit = {
    val p = (currentShard / shardsNumber.toDouble) * 100
    print("done: " + f"$p%2.0f" + "%  --- ")
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
    val queryTokensIds = InOutUtils.filter(content).map(token => token.hashCode)

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


