package utils

import java.io.InputStream


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

  def getTestQueries(inputStream: InputStream): List[(Int, String)] = {
    // Using Tipster Parse does not work as the text file contains incorrect XML Tags
    // So we have the search the file line by line for the topics and query IDs
    // this crashes as there are invalid xml tags!!
    //val query_tipster_parse = new TipsterParse(DocStream.getStream("data/questions-descriptions.txt"))

    //dummy. query 999 does not exist, just for test
    return List((51, "Airbus Subsidies"), (52, "South African Sanctions"))

  }

}
