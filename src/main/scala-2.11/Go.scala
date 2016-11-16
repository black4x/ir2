import ch.ethz.dal.tinyir.io.TipsterStream

object Go extends App {

  val tipster = new TipsterStream ("data")
  println("Number of files in zips = " + tipster.length)


  var length : Long = 0
  var tokens : Long = 0
  for (doc <- tipster.stream.take(10000)) {
    length += doc.content.length
    tokens += doc.tokens.length
  }
  println("Final number of characters = " + length)
  println("Final number of tokens     = " + tokens)

}
