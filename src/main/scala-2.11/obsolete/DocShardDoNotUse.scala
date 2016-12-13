package obsolete



class DocShardDoNotUse() {
   /* val myStopWatch = new StopWatch()
    myStopWatch.start
    /*creates the inverted index*/
    val invertedTFIndex  =tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
    myStopWatch.stop
    println("Time elapsed to create invertedTFIndex:%s".format(myStopWatch.stopped))
    
    myStopWatch.start
    val vocabulary=invertedTFIndex.keys
    myStopWatch.stop
    println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))
    
    myStopWatch.start
    val documentFrequency=invertedTFIndex.mapValues(list=>list.length) //Map from word (===token) to its document frequency
    myStopWatch.stop
    println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
   //    println(documentFrequency.toList.sortBy(-_._2))
    
    myStopWatch.start
    //stores doc0->(doclength,#distinct words)
    val documentLength = partstream.map(d =>(d.name,(tokenListFiltered(d.content).length,tokenListFiltered(d.content).distinct.length))).toMap //Map from doc to its length
    myStopWatch.stop
    println("Time elapsed to create documentLength:%s".format(myStopWatch.stopped)) 
    val nDocs=documentLength.size
        
    def tfTuples ={partstream.flatMap(d =>tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTuple(tk,d.name, lst.length)})}
      
    def tokenListFiltered(doccontent: String) = StopWords.filterOutSW(Tokenizer.tokenize(doccontent)).map(v=>PorterStemmer.stem(v))
*/
}