package main


import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import com.github.aztek.porterstemmer.PorterStemmer
import scala.collection.mutable.ListBuffer

object MyTokenizer {
 // val checker = new NorvigSpellChecker(io.Source.fromFile("C:\\Users\\Thomas\\Desktop\\IRProject2\\big.txt").mkString)  // curl http://norvig.com/big.txt > big.txt
//  Seq("speling", "korrecter", "corrected", "xyz") foreach {w => println(s"$w: ${checker(w)}")}
//  println(checker("correctedth"))
//  println(checker.apply("politicalright"))
    
  private def tokenize (text: String) : List[String] =text.toLowerCase.split("[- .,;:?!*&$-+\"\'\t\n\r\f `]+").filter(w => w.length >= 3 && w.length < 14).toList
    /*Input a eg content of doc, output a list of tokens without stopwords and stemmed*/
//  def tokenListFiltered(doccontent: String) = {
//    val res1=StopWords.filterOutSW(Tokenizer.tokenize(doccontent))
//    val res2=res1.map(v=>checker.apply(PorterStemmer.stem(v)).getOrElse(""))
//    res2
//  }
  def tokenListFiltered(doccontent: String) = StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))
}