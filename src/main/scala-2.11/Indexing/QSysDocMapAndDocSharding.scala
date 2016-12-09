package Indexing

import ch.ethz.dal.tinyir.processing._
import com.github.aztek.porterstemmer.PorterStemmer
import main.MyTokenizer

import scala.collection.mutable.ListBuffer

class QSysDocMapAndDocSharding(var wholestream:Stream[Document], chuncksize:Int = 30000) {

  private val runtime = Runtime.getRuntime()
  import runtime.{ totalMemory, freeMemory, maxMemory }
  print("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory)/1000000)
  print("** Free Memory:  " + runtime.freeMemory/1000000)
  print("** Total Memory: " + runtime.totalMemory/1000000)
  println("** Max Memory:   " + runtime.maxMemory/1000000)

  var docShards = new ListBuffer[DocShard]()
  var counter=1
  while ( ! wholestream.isEmpty){

    var partstream=wholestream.take(chuncksize)
    wholestream=wholestream.drop(chuncksize)
    val docShard= new DocShard(partstream,counter)
    docShards+=docShard
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
      val filteredDoc = docShards(loop).docMap.filter(doc_tuple => (doc_tuple._2 == docID)).map(relevant_doc => relevant_doc._1).toList
      if(filteredDoc.length>0){
        return filteredDoc.head
      }
      loop = loop + 1
    }
    return filteredDoc.head
  }


  def documentFrequency(token:String)=docShards.toList.map(x=>x.documentFrequency.getOrElse(token,0)).sum

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
    var candidateDocs=Seq[Int]()
    for (loop <- 0 until docShards.length) {
      val candidateDocsShard=tokenList.flatMap(token=>docShards(loop).invertedTFIndex.getOrElse(token,List())).map(pair=>pair._1).distinct
      candidateDocs=candidateDocs.union(candidateDocsShard)
    }
    println(candidateDocs.map(candidateDoc=>(candidateDoc,scoring(tokenList,candidateDoc))).sortBy(-_._2))
    val res=candidateDocs
    val res1=candidateDocs.map(candidateDoc=>(candidateDoc,scoring(tokenList,candidateDoc))).sortBy(-_._2).zip(Stream from 1).take(100)
    val res2=res1.map(result_tuple => ((queryID, result_tuple._2), getDocName(result_tuple._1._1))).toMap
    res2

  }
  //$ is special sign in regular expression


  /*tfTuples returns for an input stream of documents the list of postings including termfrequencies*/


  def log2(x:Double):Double= math.log(x)/math.log(2)

  def scoring(queryTokenList:Seq[String],doc:Int)={
    //val scorings=queryTokenList.map(token=>invertedTFIndexReturnTF(token,doc))
    //val scorings=queryTokenList.map(token=>math.log(invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))

    //the following function normalized the tf with the document length. Hence longer docs are not favourte. Used
    val scorings=queryTokenList.map(token=>log2((invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))*
      (log2(nDocs)-log2(documentFrequency(token))))

    val result=scorings.sum
    result
  }

  /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
  def invertedTFIndexReturnTF(token:String,doc:Int)={
    var loop =0
    var res=List[(Int,Int)]()

    while(res.isEmpty && loop<docShards.length){
      // res=docShards(loop).invertedTFIndex.getOrElse(token,List())
      res=docShards(loop).invertedTFIndex.getOrElse(token,List()).filter(_._1==doc)
      loop=loop+1

    }
    res.map(pair=>pair._2).sum
  }
}