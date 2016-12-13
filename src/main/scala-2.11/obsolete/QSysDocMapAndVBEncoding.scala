package obsolete

import Indexing.TfTupleDocID
import ch.ethz.dal.tinyir.compression.IntegerCompression
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import main.MyTokenizer

import scala.collection.Map
import scala.collection.mutable.{BitSet, ListBuffer}



class QSysDocMapAndVBEncoding(parsedstream:Stream[XMLDocument]) {

  val myStopWatch = new StopWatch()

  myStopWatch.start
  var docMap = Map[String, Int]()
  var documentLength = Map[Int, (Int, Int)]()
  var counter = 1

  parsedstream.foreach{ doc =>
    docMap += ((doc.name,counter))
    counter = counter + 1
    documentLength += (getDocID(doc.name)->(MyTokenizer.tokenListFiltered(doc.content).length,MyTokenizer.tokenListFiltered(doc.content).distinct.length))
  }
  myStopWatch.stop
  println("Time elapsed to create docMap and documentLength:%s documentLength.size:%d".format(myStopWatch.stopped,documentLength.size))
  val nDocs = documentLength.size


  /*creates the inverted index*/
  myStopWatch.start
  var invertedTFIndex = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
  println("Inverted TFI " + invertedTFIndex.size)
  myStopWatch.stop
  println("Time elapsed to create invertedTFIndex:%s".format(myStopWatch.stopped))

  // Activate this to see an Encoding Example for posting list of token "state"
  /*println("uncopressed posting list for token state (freq. not shown): ")
  invertedTFIndex.getOrElse("state", null).foreach(x =>(print("Doc ID: " + x._1 + " ")))
  println(" ")
  */

  /*Variable Byte Encoding of InvertedTFIndex */
  val invertedTFIndexCmp = invertedTFIndex.mapValues(uncompressed => compress(uncompressed.iterator))
  /*println("inverted Index with gaps")
  println(invertedTFIndexCmp)
  print("PL for token state  compressed (freq. not shown): ")
  invertedTFIndexCmp.getOrElse("state", null).foreach(x => (print(IntegerCompression.bytesToString(x._1) + " ")))
  println(" ")
*/

  // delete old inverted Index
  invertedTFIndex = null

  myStopWatch.start
  val vocabulary=invertedTFIndexCmp.keys //compressed version
  myStopWatch.stop
  println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))


  myStopWatch.start
  // compressed version
  val documentFrequency: Map[String, Double] = invertedTFIndexCmp.mapValues(list=>list.length) //Map from word (===token) to its document frequency
  myStopWatch.stop
  println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
  //   println(documentFrequency.toList.sortBy(-_._2))



  def query(queryID: Int, querystring: String): Map[(Int, Int), String]= {

    val tokenList = tokenListFiltered(querystring)

    // For each token get all docs with their term frequencies and uncompress the Gaps back to DocIDs
    var candidatePostLists = Map[String, List[(Int, Int)]]()
    tokenList.foreach(token => {
      candidatePostLists += (token -> uncompress(invertedTFIndexCmp.getOrElse(token, new ListBuffer[(Array[BitSet], Int)])))
    })

    //candidatePostLists.foreach(x => println(x))

    // Get distinct DocIDs for all candidate posting lists
    var candidateDocIDs = ListBuffer[Int]()
    candidatePostLists.map(cPl => (cPl._2.foreach(pair => {
      candidateDocIDs += pair._1
    })))
    val candidateDocIDsDistinct: List[Int] = candidateDocIDs.toList.distinct

    candidateDocIDsDistinct.map(docID => (docID, scoringTfIdf(tokenList, docID, candidatePostLists))).sortBy(-_._2).zip(Stream from 1).take(100)
      .map(result_tuple => ((queryID, result_tuple._2), getDocName(result_tuple._1._1))).toMap
  }


  // tf-idf scoring
  def scoringTfIdf(queryTokenList:Seq[String],doc:Int, candidatePostingLists:Map[String, List[(Int, Int)]]): Double = {

    var termFrequencyOfDoc: Double = 0.0
    var docLength: Double = 0.0
    var docDistinctTkn: Double = 0.0
    var result: Double = 0.0

    // For submitted candidate doc, calculate tf-idf by summing up the tf-idf value per query token
    queryTokenList.foreach(token => {
      // Get ingredients for calculating tf-idf
      val candidatePLofDoc = candidatePostingLists(token).filter(pl => (pl._1 == doc))
      if (candidatePLofDoc isEmpty){
        termFrequencyOfDoc = 0.0
      }
      else{
        termFrequencyOfDoc = candidatePLofDoc.map(tuple => tuple._2).head
      }

      docLength = documentLength(doc)._1
      docDistinctTkn = documentLength(doc)._2

      val result_token = + (log2((termFrequencyOfDoc + 1) / (docLength + docDistinctTkn)) * (log2(nDocs) - log2(documentFrequency.getOrElse(token, 1.0))))
      result += result_token

      //println("docID: " + doc + ", tkn: " + token + ", result: " + result_token)

    })

    return result

  }


  def log2(x:Double):Double= math.log(x)/math.log(2)


  /*Input a eg content of doc, output a list of tokens without stopwords and stemmed*/
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


  /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
  def invertedTFIndexReturnTF(token:String,doc:Int)={
    val res = invertedTFIndexCmp.getOrElse(token,List()).filter(_._1==doc)
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

  // Compress Postings Lists of one token: Results are Posting Lists with Doc IDs as encoded gaps
  def compress(uncompressed: Iterator[(Int, Int)]): ListBuffer[(Array[BitSet], Int)] = {

    var compressedPostings = new scala.collection.mutable.ListBuffer[(Array[BitSet], Int)]
    var gap = 0
    var gap_encoded = Array[BitSet]()

    // compress the first Posting in the List
    var current: (Int, Int) = uncompressed.next()
    gap = current._1 - 0
    gap_encoded = IntegerCompression.VBEncode(Array(gap))
    val firstPostingCmp = (gap_encoded, current._2)
    compressedPostings += firstPostingCmp

    // compress the reset of the Posting List
    while (uncompressed.hasNext){
      val next = uncompressed.next()
      gap = next._1 - current._1
      gap_encoded = IntegerCompression.VBEncode(Array(gap))
      val newNext = (gap_encoded, next._2)
      compressedPostings += newNext
      current = next
    }
    compressedPostings

  }

  // Uncompress Posting Lists for one token. Result is a list of Doc IDs with corresponding term frequencies
  def uncompress(post_lists_compressed: ListBuffer[(Array[BitSet], Int)]): List[(Int, Int)] = {

    var previous_doc_id = 0
    var current_doc_id = 0
    var gap_uncompressed = 0
    var doc_ids_uncompressed = ListBuffer[Int]()
    var posting_lists_uncompressed = ListBuffer[(Int, Int)]() // ListBuffer(Gap converted to DocId, Term Frequency)
    var counter = 1

    // For each compressed Posting List (= encoded gap + term frequency), decode gap and calculate original DocID
    post_lists_compressed.foreach(cpl => {

      gap_uncompressed = IntegerCompression.VBDecode(cpl._1, 0, Array[Int]()).head
      current_doc_id = previous_doc_id + gap_uncompressed // previous doc_id + gap = current Doc ID

      posting_lists_uncompressed += ((current_doc_id, cpl._2)) //cpl.2 is the term frequecy

      previous_doc_id = current_doc_id
    })

    return posting_lists_uncompressed.toList

  }


}


/*Map Doc Names to Interger Numbers*/
//val docMap: Map[String, Int] = parsedstream.map(d => d.name).zip(Stream from 1).map(doc_tuple =>  doc_tuple._1 -> doc_tuple._2).toMap
//docMap foreach (doc => println(doc))
