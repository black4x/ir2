package Indexing

import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.processing.{StopWords, Tokenizer, XMLDocument}
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer

import scala.collection.immutable.HashMap

object LazyIndex extends App {

  type InvIndex = Map[String, Stream[LazyDocItem]]

  val N = 10000

  // filtering magic numbers
  val docFrequencyMax = N / 10
  // if doc is very frequent it does not go into consideration
  val tokenLengthMin = 4
  // minimal length of token size, otherwise token is ignored
  val tokenFrequencyMin = 1 // minimal token frequency in doc, if less, doc with that TF skipped

  //sharding magic numbers

  // Shard Size in % of total docs number
  val shardSizeInProcent = 10
  val shardsNumber = (N * (shardSizeInProcent.toDouble / 100)).toInt
  val shardSize = N / shardsNumber

  println(shardsNumber + " shards")

  var stream = new TipsterStream("data").stream.take(N)

  var invIndexMap: InvIndex = new HashMap[String, Stream[LazyDocItem]]

  val myStopWatch = new StopWatch()
  myStopWatch.start

  for (i <- 0 to shardsNumber) {
    val streamChunk = stream.slice(i * shardSize, i * shardSize + shardSize)
    invIndexMap = merge(invIndexMap, invertedIndex(streamChunk))
    printStat(i)
  }

  myStopWatch.stop
  println("index " + myStopWatch.stopped + " tokens = " + invIndexMap.size)

  // TODO insted of tokens -> Int ?
  def invertedIndex(stream: Stream[XMLDocument]): InvIndex =
    stream.flatMap(doc => {
      filter(doc.content)
        .groupBy(identity)
        .filter(x => x._2.length > tokenFrequencyMin)
        .map {
          case (tk, lst) => LazyTokenDocItem(tk, doc.name.hashCode, lst.length)
        }
    })
      .groupBy(_.term)
      .filter(q => q._2.size < docFrequencyMax)
      .mapValues(_.map(tokenDocItem => LazyDocItem(tokenDocItem.docInt, tokenDocItem.tf)).sortBy(x => x.docInt))

  def printUsedMem(): Unit = {
    val runtime = Runtime.getRuntime

    val usedMem = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024)
    val maxMem = runtime.maxMemory / (1024 * 1024)
    val p = usedMem.toDouble / maxMem.toDouble * 100

    println(" memory used:  " + f"$p%2.0f" + " % ")
  }

  def printStat(currentShard: Int): Unit = {
    val p = (currentShard / shardsNumber.toDouble) * 100
    print(f"$p%2.0f" + " % ")
    printUsedMem()
  }

  def filter(content: String): Seq[String] = StopWords.filterOutSW(Tokenizer.tokenize(content))
    .filter(t => t.length > tokenLengthMin)
    .map(v => PorterStemmer.stem(v))

  def merge(i1: InvIndex, i2: InvIndex): InvIndex =
    i1 ++ i2.map { case (token, stream) => token -> (stream ++ i1.getOrElse(token, Stream.Empty)).distinct }

}
