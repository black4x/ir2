package Indexing

import Evaluation.QueryEvaluation
import ch.ethz.dal.tinyir.io.{DocStream, TipsterStream}
import ch.ethz.dal.tinyir.processing.{StopWords, Tokenizer, XMLDocument}
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import utils.InOutUtils

import scala.collection.Map
import scala.collection.immutable.HashMap

object LazyIndex extends App {

  //  if (!args.isEmpty) {
  //    baseDir = args(0)
  //    classifierType = args(1)
  //    runMode = args(2)
  //  }

  type InvIndex = Map[Int, Stream[LazyDocItem]]

  val N = 10000
  val path = "data"

  // filtering magic numbers
  //val docFrequencyMax = N / 10
  // if doc is very frequent it does not go into consideration
  //val tokenLengthMin = 2
  // minimal length of token size, otherwise token is ignored
  //val tokenFrequencyMin = 0 // minimal token frequency in doc, if less, doc with that TF skipped

  //sharding magic numbers

  // Shard Size in % of total docs number
  val shardSizeInProcents = 10
  val shardsNumber = (N * (shardSizeInProcents.toDouble / 100)).toInt
  val shardSize = N / shardsNumber

  println(shardsNumber + " shards")

  var stream = new TipsterStream(path).stream.take(N)


  // Get list of query IDs and their titles (query ID needed for submission format!)
  val query_stream_validation = DocStream.getStream(path + "/questions-descriptions.txt")
  val query_stream_test = DocStream.getStream(path + "/test-questions.txt")

  // **** language model scoring vals:

  // token -> raw tokens count in document
  var docInfoMap = Map[Int, (String, Int)]()

  var invIndexMap: InvIndex = new HashMap[Int, Stream[LazyDocItem]]

  val myStopWatch = new StopWatch()
  myStopWatch.start

  for (i <- 0 to shardsNumber) {
    val streamChunk = stream.slice(i * shardSize, i * shardSize + shardSize)
    invIndexMap = merge(invIndexMap, createInvertedIndex(streamChunk))
    printStat(i)
  }

  myStopWatch.stop
  println("index " + myStopWatch.stopped + " tokens = " + invIndexMap.size)
  println("start queries")

  val query = InOutUtils.getValidationQueries(query_stream_validation).head

  // TODO all queries !
  val r = query(query._1, query._2)
  showResults(r)

  println("results by query: ")
  r.foreach(result => {
    println(result)
  })


  def languageModelScoring(docId: Int, queryTokenList: Seq[Int]): Double = {
    val lambda = 0.6f
    queryTokenList.map(tokenId => {
      val tokenFrequencyInDoc = getTermFrequencyFromInvIndex(tokenId, docId).toDouble
      val numberOfTokensInDoc = getDocLength(docId).toDouble
      lambda * (tokenFrequencyInDoc / numberOfTokensInDoc) + (1 - lambda) * lmSmoothingNumber(tokenId, sumAllTokens())
    }).product
  }

  def termModelScoring(docId: Int, queryTokenList: Seq[Int]): Double =
    queryTokenList.map(token =>
      log2((getTermFrequencyFromInvIndex(token, docId) + 1.0) / (getDocLength(docId).toDouble + getDistinctTokensNumberForDoc(docId))) *
        (log2(N) - log2(invIndexMap.getOrElse(token, Stream.Empty).length))).sum


  def getDistinctTokensNumberForDoc(docId: Int): Int =
    invIndexMap.count(item => item._2.exists(x => x.docInt == docId))

  def lmSmoothingNumber(token: Int, totalCount: Double): Double =
    termFrequencyInCollection(token).toDouble / totalCount

  def termFrequencyInCollection(tokenId: Int): Int =
    invIndexMap.getOrElse(tokenId, Stream.Empty).map(item => item.tf).sum

  // ------------------------ def: -------------------------------------

  def getTermFrequencyFromInvIndex(tokenId: Int, docId: Int): Int =
    invIndexMap.getOrElse(tokenId, Stream.Empty)
      .find(item => item.docInt == docId) match {
      case Some(d) => d.tf
      case None => 0
    }

  def getCandidateDocumensIds(queryTokensIds: Seq[Int]): Set[Int] =
    queryTokensIds.map(tokenId => {
      invIndexMap.getOrElse(tokenId, Stream.Empty).toSet
    }).reduce(_ ++ _).map(x => x.docInt)

  def createInvertedIndex(stream: Stream[XMLDocument]): InvIndex =
    stream.flatMap(doc => {
      val filteredContent = filter(doc.content)
      docInfoMap += (doc.name.hashCode -> (doc.name, filteredContent.length))
      filteredContent.groupBy(identity)
        //.filter(x => x._2.length > tokenFrequencyMin)
        .map {
        case (tk, lst) => LazyTokenDocItem(tk.hashCode, doc.name.hashCode, lst.length)
      }
    })
      .groupBy(_.termHash)
      // .filter(q => q._2.size < docFrequencyMax)
      .mapValues(_.map(tokenDocItem => LazyDocItem(tokenDocItem.docInt, tokenDocItem.tf)).sortBy(x => x.docInt))

  def filter(content: String): Seq[String] = StopWords.filterOutSW(Tokenizer.tokenize(content))
    //.filter(t => t.length > tokenLengthMin)
    .map(v => PorterStemmer.stem(v))

  def merge(i1: InvIndex, i2: InvIndex): InvIndex =
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
    docInfoMap.getOrElse(hashCode, ("", 0))._1

  def sumAllTokens(): Int =
    docInfoMap.map(x => x._2._2).sum

  def getDocLength(docId: Int): Int =
    docInfoMap.getOrElse(docId, 0 -> 0)._2

  def query(queryID: Int, content: String): Map[(Int, Int), String] = {
    val queryTokensIds = filter(content).map(token => token.hashCode)
    val candidatesIds = getCandidateDocumensIds(queryTokensIds)

    // ------ Language Model Scoring
    val results = candidatesIds.map(docId => docId -> termModelScoring(docId, queryTokensIds))
      .toSeq.sortBy(x => x._2).take(100).zip(Stream from 1)

    results.map(x => ((queryID, x._2), getDocNameByHashCode(x._1._1))).toMap
  }

  def showResults(query_results_top_100: Map[(Int, Int), String]): Unit = {
    //    if (runMode == VALIDATION_MODE) {
    // Evaluate results (calculate metrics)
    val relevance_judgement_stream = DocStream.getStream(path + "/relevance-judgements.csv")
    val relevance_judgement = InOutUtils.getCodeValueMapAll(relevance_judgement_stream)
    val myQE = new QueryEvaluation(relevance_judgement, query_results_top_100)
    myQE.calculateMetrics()

    val metrics = myQE.getQueryMetrics()
    val meanAvgPrecision = myQE.getMAP()

    metrics.foreach(metrics_per_query => {
      print("Query: " + metrics_per_query._1 + " -> ")
      print("Precision: " + metrics_per_query._2(0))
      print(", Recall: " + metrics_per_query._2(1))
      print(", F1: " + metrics_per_query._2(2))
      print(", Avg Precision: " + metrics_per_query._2(3))
      println(" ")
    })

    println("MAP is: " + meanAvgPrecision)
  }

  def log2(x: Double): Double = math.log(x) / math.log(2)

  // Write results of the 10 queries to file if run mode is TEST

}
