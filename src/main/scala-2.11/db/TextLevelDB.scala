package db

import java.io.File
import java.nio.ByteBuffer

import org.iq80.leveldb.{Options, WriteBatch}
import search.DocItem

object TextLevelDB extends App {


  val options = new Options
  //options.cacheSize(1000 * 1048576); // 1000MB cache
  options.createIfMissing(true)
  val db = org.iq80.leveldb.impl.Iq80DBFactory.factory.open(new File("example"), options)
  try {

    var value = Map[Int, List[Int]]()
    value += (1 -> List(2, 3))

    val bb = java.nio.ByteBuffer.allocate(8)

    for ((k, v) <- value){
      db.put("dd".getBytes, "dsf".getBytes)
    }

    val iterator = db.iterator
    try {
      iterator.seekToFirst
      while (iterator.hasNext) {
        val key1 = new String(iterator.peekNext().getKey)
        //val value1 = List(iterator.peekNext().getValue).map(_=> new Int()
        //println(key1 + " " + value1)
        iterator.next
      }

    } finally {
      // Make sure you close the iterator to avoid resource leaks.
      iterator.close()
    }

    // Use the db in here....
  } finally {
    // Make sure you close the db to shutdown the
    // database and avoid resource leaks.
    db.close()
  }


}
