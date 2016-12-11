package Indexing

import ch.ethz.dal.tinyir.compression.IntegerCompression
import ch.ethz.dal.tinyir.processing._
import ch.ethz.dal.tinyir.util.StopWatch
import main.MyTokenizer

import scala.collection.Map
import scala.collection.mutable.{BitSet, ListBuffer}

class DocShardVBE(var partstream:Stream[Document], startindex:Int) {

    println("Start Index of shard: " + startindex )

    val myStopWatch = new StopWatch()
    myStopWatch.start

    //    val docMap: Map[String, Int] = partstream.map(d => d.name).zip(Stream from 1).map(doc_tuple =>  doc_tuple._1 -> doc_tuple._2).toMap
    //    val documentLength = partstream.map(d =>(getDocID(d.name),(MyTokenizer.tokenListFiltered(d.content).length,MyTokenizer.tokenListFiltered(d.content).distinct.length))).toMap //Map from doc to its length

    var docMap=Map[String, Int]()
    var documentMapWithTokenCount=Map[Int, (Int, Int)]()
    var counter=startindex
    var numberOfAllTokens = 0;

    partstream.foreach { doc =>
        docMap += ((doc.name, counter))
        counter = counter + 1
        val tokensInDoc = MyTokenizer.tokenListFiltered(doc.content)
        val tokensNumber = tokensInDoc.length
        documentMapWithTokenCount += (getDocID(doc.name) -> (tokensNumber, tokensInDoc.distinct.length))
        numberOfAllTokens += tokensNumber
    }
    myStopWatch.stop
    println("Time elapsed to create docMap and documentLength:%s documentLength.size:%d".format(myStopWatch.stopped,documentMapWithTokenCount.size))
    val nDocs=documentMapWithTokenCount.size


    /*creates the inverted index*/
    var invertedTFIndex:Map[String,List[(Int,Int)]] = tfTuples.groupBy(_.term).mapValues(_.map(tfT => (tfT.doc, tfT.count)).toList.sorted)
    myStopWatch.stop
    println("Time elapsed to create invertedTFIndex:%s size:".format(myStopWatch.stopped))

    /*Variable Byte Encoding of InvertedTFIndex */
    val invertedTFIndexCmp = invertedTFIndex.mapValues(uncompressed => compress(uncompressed.iterator))

    // delete old inverted Index, and hopefully save memory
    invertedTFIndex = null


    myStopWatch.start
    val vocabulary=invertedTFIndexCmp.keys
    myStopWatch.stop
    //    println("Time elapsed to create vocabulary:%s".format(myStopWatch.stopped))

    myStopWatch.start
    val documentFrequency = invertedTFIndexCmp.mapValues(list=>list.length) //Map from word (===token) to its document frequency
    myStopWatch.stop
    //    println("Time elapsed to create documentFrequency:%s".format(myStopWatch.stopped))
    //    println(documentFrequency.toList.sortBy(-_._2))



    def tfTuples = partstream.flatMap(d =>MyTokenizer.tokenListFiltered(d.content).groupBy(identity).map{ case (tk,lst) => TfTupleDocID(tk,getDocID(d.name), lst.length)})

    def getDocID(docName: String): Int = {
        // must always find an entry
        return docMap.getOrElse(docName,0)
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