package com.DataVectis.MainRecommendation

import java.io.{File, PrintWriter}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import Console.withOut

object MainRecommendation {

  def main(args: Array[String]): Unit = {

    val appProperties = new prop

    val spark = SparkSession.builder.
      master(appProperties.getProp("master"))
      .appName(appProperties.getProp("appName"))
      .getOrCreate()

    //val ratingsFile = "src/main/data/ratings.csv"

    //Importing Ratings Data
    val dfImportRatings = spark.read.format("com.databricks.spark.csv").
      option("header", true).
      load(appProperties.getProp("ratingsDataInput"))

    val ratingsDF = dfImportRatings.select(
      dfImportRatings.col("userId"),
      dfImportRatings.col("movieId"),
      dfImportRatings.col("rating"),
      dfImportRatings.col("timestamp"))

    /*
    val moviesFile = "src/main/data/movies_metadata.csv"
    val df2 = spark.read.format("com.databricks.spark.csv").option("header", "true").load(moviesFile)
    val moviesDF = df2.select(df2.col("id"), df2.col("original_title"))
    moviesDF.show(false)
  */

    // Split ratings RDD into training RDD & test RDD
    val splits = ratingsDF.randomSplit(Array(
      appProperties.getProp("trainRate").toDouble,
      1-appProperties.getProp("trainRate").toDouble),
      seed = 12345L)

    val (trainingData, testData) = (splits(0), splits(1))
/*
    val numTraining = trainingData.count()

    val numTest = testData.count()

    println("Training: " + numTraining + " test: " + numTest)
*/

    //RDD of ratings for training
    val ratingsRDD = trainingData.rdd.map(row => {

      val userId = row.getString(0)

      val movieId = row.getString(1)

      val ratings = row.getString(2)

      Rating(userId.toInt, movieId.toInt, ratings.toDouble)

    })

    //RDD of ratings for testing
    val testRDD = testData.rdd.map(row => {

      val userId = row.getString(0)

      val movieId = row.getString(1)

      val ratings = row.getString(2)

      Rating(userId.toInt, movieId.toInt, ratings.toDouble)

    })

    //Instance from class RecomModeling , SavingData & ClaculateRMSE
    val recomMod = new RecomModeling
    val savingData = new SavingData
    val claculateRMSE = new ClaculateRMSE

    /*
    //creating Model
    val model = recomMod.getModelTraing(
      ratingsRDD,
      appProperties.getProp("rank").toInt,
      appProperties.getProp("numIterations").toInt,
      appProperties.getProp("lamda").toDouble,
      appProperties.getProp("alpha").toDouble
    )
    */

    val model = new ALS()
      .run(ratingsRDD)

    //result on the test Data
    val predictions = recomMod.getPredictionTest(model,testRDD)
    //saving test Data with predicted Ratings
    savingData.saveTestModelData(predictions,
      appProperties.getProp("DataOutput"),
      appProperties.getProp("testDataWithPredictions"),
      appProperties.getProp("Format"))

    //evaluating the model with RMSE
    val RMSE = claculateRMSE.getRMSE(model,testRDD)
    println("Test RMSE: = " + RMSE)



    //Predicting Ratings from Data
    //Importing Data to be predicted
    val dfImportDataPred = spark.read.format("com.databricks.spark.csv").
      option("header", true).
      load(appProperties.getProp("ratingsDataInput"))

    val ratingsDFTest = dfImportDataPred.select(
      dfImportDataPred.col("userId"),
      dfImportDataPred.col("movieId"))

    val ratingsDFPrediction = ratingsDFTest.withColumn("rating",ratingsDFTest.col("userId")*0)

    //RDD of ratings for Data to be predicted
    val predRDD = ratingsDFPrediction.rdd.map(row => {

      val userId = row.getString(0)

      val movieId = row.getString(1)

      val ratings = row.getDouble(2)

      Rating(userId.toInt, movieId.toInt, ratings.toDouble)

    })

    //Predictions
    val newPrediction = model.predict(predRDD.map(x => (x.user,x.product)))
    //saving Data with Predicted ratings
    savingData.savePredictionsData(newPrediction,
      appProperties.getProp("DataOutput"),
      appProperties.getProp("predictedData"),
      appProperties.getProp("Format")
    )

  }
}