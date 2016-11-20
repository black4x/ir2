package utils

import ch.ethz.dal.tinyir.processing.XMLDocument

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by Ralph on 20/11/16.
  */

//todo adjust and finish. just copied it over from at the last poject and added placeholder methods
class Scoring(val reuters_validate: Stream[XMLDocument], val result_classifier: Map[String, ListBuffer[String]]) {


  def calculatePrecision(): Double = {

    1.0
  }

  def calculateRecall(): Double = {

    1.0
  }


  def calculateF1(): Double = {


    val nr_of_docs = reuters_validate.size.toDouble
    val F1_total = reuters_validate.map(doc => calculateF1PerDoc(doc)).sum / nr_of_docs
    return F1_total

  }

  private def calculateF1PerDoc(vali_doc: XMLDocument): Double = {

    val labels_correct = vali_doc.codes
    //println("codes of validation doc " + labels_correct + " " + labels_correct.size)

    // Just in case we check if there is a result for validation document
    if (!result_classifier.exists(_._1 == vali_doc.name)) {
      println("No result found for Doc: " + vali_doc.name)
      return 0.0
    }

    val labels_predicted = result_classifier(vali_doc.name)
    val nr_labels_predicted = labels_predicted.size.toDouble

    if (nr_labels_predicted == 0) return 0.0

    // Count how many predicted labels are actually in the validation document
    var nr_labels_classified_correct = 0.0
    labels_predicted.foreach(label_predicted => {
      if (labels_correct.contains(label_predicted)) {
        nr_labels_classified_correct += 1
      }
    })

    //println("correct prediction: " + nr_labels_classified_correct)
    //println("total labels in doc: " + labels_correct.size)

    val precision = nr_labels_classified_correct / nr_labels_predicted
    val recall = nr_labels_classified_correct / labels_correct.size

    //println("per and recall " + precision + " " + recall)
    if (precision + recall == 0) {
      return 0.0
    }
    else {
      return (2 * precision * recall) / (precision + recall)
    }

  }

  def calculateAvgPrecision(): Double = {
    1.0

  }

  def calculateMAP(): Double = {
    1.0

  }



}
