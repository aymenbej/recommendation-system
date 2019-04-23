package com.DataVectis.MainRecommendation

import java.io.{File, PrintWriter}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import Console.withOut

class ClaculateRMSE{

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map {
      x => ((x.user, x.product), x.rating)
    } .join(data.map(x => ((x.user, x.product), x.rating)))
      .values

    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

}