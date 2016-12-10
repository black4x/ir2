package Indexing

import ch.ethz.dal.tinyir.io.TipsterStream
import ch.ethz.dal.tinyir.processing.{StopWords, Tokenizer}
import com.github.aztek.porterstemmer.PorterStemmer


object LazyIndex extends App {

  val N = 100000
  val docFrequencyMax = N / 10
  val tokenLengthMin = 4
  val tokenFrequencyMin = 1

  var stream = new TipsterStream("data").stream.take(N)

  printUsedMem()

  var invertedIndex = stream.zipWithIndex.flatMap(doc => {
    if (doc._2 % (N/10) == 0) {
      println((doc._2/N.toDouble) * 100)
      printUsedMem()
    }
    filter(doc._1.content)
      .groupBy(identity)
      .filter(x => x._2.length > tokenFrequencyMin)
      .map {
        case (tk, lst) => LazyTokenDocItem(tk, doc._2, lst.length)
      }
  })
    .groupBy(_.term)
    .filter(q => q._2.size < docFrequencyMax)
    .mapValues(_.map(tokenDocItem => LazyDocItem(tokenDocItem.docInt, tokenDocItem.tf)).sortBy(x => x.docInt))

  println(invertedIndex.size)
  println(invertedIndex.map(x => x._2.size).sum)

  printUsedMem()

  def printUsedMem() = {
    val runtime = Runtime.getRuntime
    print("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024))
    println(" from " + runtime.maxMemory / (1024 * 1024))
  }

  def filter(content: String): Seq[String] = StopWords.filterOutSW(Tokenizer.tokenize(content))
    .filter(t => t.length > tokenLengthMin)
    .map(v => PorterStemmer.stem(v))

}
