package Indexing

import ch.ethz.dal.tinyir.processing._
import main.MyTokenizer

import scala.collection.Map
import scala.collection.mutable.{BitSet, ListBuffer}
import scala.collection.parallel.mutable

class QSysDocMapAndDocShardingVBE(var wholestream:Stream[Document], chuncksize:Int = 30000) {

  private val runtime = Runtime.getRuntime

  print("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory)/1000000)
  print("** Free Memory:  " + runtime.freeMemory/1000000)
  print("** Total Memory: " + runtime.totalMemory/1000000)
  println("** Max Memory:   " + runtime.maxMemory/1000000)

  var docShards = new ListBuffer[DocShardVBE]()
  var counter=1
  while ( wholestream.nonEmpty){

    var partstream=wholestream.take(chuncksize)
    wholestream=wholestream.drop(chuncksize)
    val docShard= new DocShardVBE(partstream,counter)
    docShards += docShard
    counter=counter+chuncksize

    print("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory)/1000000)
    print("** Free Memory:  " + runtime.freeMemory/1000000)
    print("** Total Memory: " + runtime.totalMemory/1000000)
    println("** Max Memory:   " + runtime.maxMemory/1000000)
  }

  def getDocName(docID: Int): String ={
    // can only be one match for submitted Doc ID

    var loop = 0
    var filteredDoc=List[String]()
    while(loop < docShards.length) {
      val filteredDoc = docShards(loop).docMap.filter(doc_tuple => doc_tuple._2 == docID).keys.toList
      if(filteredDoc.nonEmpty){
        return filteredDoc.head
      }
      loop = loop + 1
    }
    return filteredDoc.head
  }


  def documentFrequency(token:String) = docShards.toList.map(x=>x.documentFrequency.getOrElse(token,0)).sum

  def documentLength(doc:Int)={
    var loop = 0
    var notFound = true
    var res=(0,0)
    while(res==(0,0) && loop<docShards.length){
      res=docShards(loop ).documentLength.getOrElse(doc,(0,0))
      loop=loop+1
    }
    res
  }

  val nDocs=docShards.toList.map(x=>x.documentLength.size).sum

  def query(queryID:Int,querystring:String): Map[(Int, Int), String]={
    val tokenList= MyTokenizer.tokenListFiltered(querystring)

    // For each token and shard, get all docs with their term frequencies and uncompress the Gaps back to DocIDs
    var candidatePostLists = Map[String, List[(Int, Int)]]()
    tokenList.foreach(token => {
      for (loop <- docShards.indices){
        if(candidatePostLists == null || candidatePostLists.getOrElse(token,null) == null) {
          // add a new entry into the Candidate Posting List
          candidatePostLists += (token -> docShards(loop).uncompress(docShards(loop).invertedTFIndexCmp.getOrElse(token, new ListBuffer[(Array[BitSet], Int)])))
        }
        else{
          // Append Posting List for a token that is already in the Candidate Posting List
          val existingPostingList: List[(Int, Int)] = candidatePostLists(token)
          val newPostingList: List[(Int, Int)] = (docShards(loop).uncompress(docShards(loop).invertedTFIndexCmp.getOrElse(token, new ListBuffer[(Array[BitSet], Int)])))
          candidatePostLists += (token -> (existingPostingList ++ newPostingList))
        }
      }
    })

    //candidatePostLists.foreach(x => println(x))

    // Get distinct DocIDs for all candidate posting lists
    var candidateDocIDs = ListBuffer[Int]()
    candidatePostLists.map(cPl => (cPl._2.foreach(pair => {
      candidateDocIDs += pair._1
    })))
    val candidateDocIDsDistinct: List[Int] = candidateDocIDs.toList.distinct

    // Call the scoring method, sort them by score, take only the best 100, and return them in the right output format
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

      val result_token = + (log2((termFrequencyOfDoc + 1) / (docLength + docDistinctTkn)) * (log2(nDocs) - log2(documentFrequency(token))))
      result += result_token

    })

    return result

  }

  def log2(x:Double):Double= math.log(x)/math.log(2)


 /* /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
  def invertedTFIndexReturnTF(token:String,doc:Int)={
    var loop =0
    var res= Seq[(Array[BitSet], Int)]()

    while(res.isEmpty && loop<docShards.length){
      // res=docShards(loop).invertedTFIndex.getOrElse(token,List())
      res = docShards(loop).invertedTFIndexCmp.getOrElse(token,List()).filter(_._1==doc)
      loop=loop+1

    }
    res.map(pair=>pair._2).sum
  }
*/

}