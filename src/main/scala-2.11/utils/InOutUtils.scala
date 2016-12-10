package utils

import java.io.InputStream

import scala.collection.mutable.ListBuffer


/**
  * Created by Ralph on 30/11/16.
  */
object InOutUtils {

  def getCodeValueMapSingleQuery(forNumber: String, inputStream: InputStream): Map[(Int, String), Int] = {
    scala.io.Source.fromInputStream(inputStream).getLines()
      .filterNot(_ startsWith forNumber)
      .map(_ split " ")
      // 51 0 AP880311-0301 1
      .collect { case Array(number, ignore, name, relevance) => ((number.toInt, name.filter(_.isLetterOrDigit)), relevance.toInt) }
      .toMap
  }

  def getCodeValueMapAll(inputStream: InputStream): Map[(Int, String), Int] = {
    scala.io.Source.fromInputStream(inputStream).getLines()
      .map(_ split " ")
      // 51 0 AP880311-0301 1
      .collect { case Array(number, ignore, name, relevance) => ((number.toInt, name.filter(_.isLetterOrDigit)), relevance.toInt) }
      .toMap
  }

  def getValidationQueries(inputStream: InputStream): List[(Int, String)] = {
    // Using Tipster Parse does not work as the text file contains incorrect XML Tags
    // So we have the search the file line by line for the topics and query IDs
    // this crashes as there are invalid xml tags!!
    //val query_tipster_parse = new TipsterParse(DocStream.getStream("data/questions-descriptions.txt"))

    var queries = ListBuffer[(Int, String)]()
    var queryNr: String = ""
    var queryTitle: String = ""
    var found: Int = 0
    val lines = scala.io.Source.fromInputStream(inputStream).getLines()
    lines.foreach(line => {
      if (line.contains("<num>")) {
        queryNr = line.replaceAll("<num> Number:  ", "").replaceAll("<num> Number: ","").replaceAll("<num>  Number:  ", "")
        found += 1
      }

      if (line.contains("<title>")) {
        queryTitle = line.replaceAll("<title>", "").replaceAll("Topic: ", "").replaceAll("Topic:  ", "")
        found += 1
      }

      if (found == 2) {
        val queryNrInt: Int = queryNr.replaceAll("""(?m)\s+$""","").toInt
        queries += ((queryNrInt, queryTitle))
        found = 0
      }

    })
    //testQueries.foreach(x => println(x))
    return queries.toList

  }

  def getTestQueries(inputStream: InputStream): List[(Int, String)] = {
    var queries = ListBuffer[(Int, String)]()
    var queryNr: String = ""
    var queryTitle: String = ""
    var found: Int = 0
    val lines = scala.io.Source.fromInputStream(inputStream).getLines()
    lines.foreach(line => {
      if (line.contains("<num>")) {
        queryNr = line.replaceAll("<num> Number: ", "")
        found += 1
      }

      if (line.contains("<title>")) {
        queryTitle = line.replaceAll("<title> Topic: ", "")
        found += 1
      }

      if (found == 2) {
        val queryNrInt: Int = queryNr.replaceAll("""(?m)\s+$""","").toInt
        queries += ((queryNrInt, queryTitle))
        found = 0
      }

    })
    queries.foreach(x => println(x))
    return queries.toList

  }

  def saveResults(result: Map[(Int, Int), String], filename: String) = {
    import java.io._
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    var result_line = new String
    result foreach { case (key, docName) => {
      result_line = key._1 + " " + key._2 + " " + docName + "\n"
      bw.write(result_line)
    }
    }
    bw.close()
  }



}
