package utils

import ch.ethz.dal.tinyir.alerts.ScoredResult

import scala.collection.mutable.PriorityQueue

class TopResults(val n: Int) {

  // get top n results (or m<n, if not enough docs processed)
  def results = heap.toList.sortBy(res => -res.score)

  // heap and operations on heap
  private val heap = new PriorityQueue[ScoredResult]()(Ordering.by(score))

  private def score(res: ScoredResult) = -res.score

  def add(res: ScoredResult): Boolean = {
    if (heap.size < n) {
      // heap not full
      heap += res
      true
    } else if (heap.head.score < res.score) {
      heap.dequeue
      heap += res
      true
    } else false
  }
}
