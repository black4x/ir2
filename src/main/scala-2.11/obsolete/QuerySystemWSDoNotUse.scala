package obsolete

import ch.ethz.dal.tinyir.processing._


//obsolete. do not use

class QuerySystemWSDoNotUse(var wholestream:Stream[Document], chuncksize:Int = 30000) {
   /*
    var count=0
    var docShards = new ListBuffer[DocShard]()
    while ( ! wholestream.isEmpty){
      var partstream=wholestream.take(chuncksize)
      wholestream=wholestream.drop(chuncksize)
      count = count + 1
      println(count)
      val docShard= new DocShard(partstream)
      docShards+=docShard
      
    }  

  
    def documentFrequency(token:String)=docShards.toList.map(x=>x.documentFrequency.getOrElse(token,0)).sum
    
    def documentLength(doc:String)={
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

    def query(queryID: Int, querystring:String): Map[(Int, Int), String] = {
      val tokenList= tokenListFiltered(querystring)
      var candidateDocs=Seq[String]()
      for (loop <- 0 until docShards.length) {
        val candidateDocsShard=tokenList.flatMap(token=>docShards(loop).invertedTFIndex.getOrElse(token,List())).map(pair=>pair._1).distinct
        candidateDocs=candidateDocs.union(candidateDocsShard)
      }
      candidateDocs.map(candidateDoc=>(candidateDoc,scoring(tokenList,candidateDoc))).sortBy(-_._2).zip(Stream from 1).take(100)
        .map(result_tuple => ((queryID, result_tuple._2), result_tuple._1._1)).toMap
    }
    
    
    /*Input a eg content of doc, output a list of tokens without stopwords and stemmed*/
    def tokenListFiltered(doccontent: String) = StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))
    
    /*tfTuples returns for an input stream of documents the list of postings including termfrequencies*/
    
    
    def log2(x:Double):Double= math.log(x)/math.log(2)
    
    def scoring(queryTokenList:Seq[String],doc:String)={
      //val scorings=queryTokenList.map(token=>invertedTFIndexReturnTF(token,doc))
      //val scorings=queryTokenList.map(token=>math.log(invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))
      
      //the following function normalized the tf with the document length. Hence longer docs are not favourte. Used 
      val scorings=queryTokenList.map(token=>log2((invertedTFIndexReturnTF(token,doc)+1.0)/(documentLength(doc)._1+documentLength(doc)._2))*
                                             (log2(nDocs)-log2(documentFrequency(token))))
                                      
      val result=scorings.sum
      result
    }
    
    /*Given a document and a token, this function returns the term frequency. Works also if token is not in the doc => tf=0*/
    def invertedTFIndexReturnTF(token:String,doc:String)={
      var loop =0
      var res=List[(String,Int)]()
      
      while(res.isEmpty && loop<docShards.length){
         res=docShards(loop).invertedTFIndex.getOrElse(token,List()).filter(_._1==doc)
         loop=loop+1
         
      }
      res.map(pair=>pair._2).sum
    }
    */
}