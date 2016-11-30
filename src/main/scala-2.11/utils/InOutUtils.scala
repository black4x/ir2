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
      .collect { case Array(number, ignore, name, relevance) => ((number.toInt, name), relevance.toInt) }
      .toMap
  }

  def getCodeValueMapAll(inputStream: InputStream): Map[(Int, String), Int] = {
    scala.io.Source.fromInputStream(inputStream).getLines()
      .map(_ split " ")
      // 51 0 AP880311-0301 1
      .collect { case Array(number, ignore, name, relevance) => ((number.toInt, name), relevance.toInt) }
      .toMap
  }


}
