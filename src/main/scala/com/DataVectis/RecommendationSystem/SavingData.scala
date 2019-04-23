package com.DataVectis.MainRecommendation

import java.io.{File, PrintWriter}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import Console.withOut

class SavingData{

  def saveTestModelData(data:RDD[((Int,Int),(Double,Double))],filePath:String,fileName:String,fileFormat:String): Unit ={

    val outCapture = new ByteArrayOutputStream
    withOut(outCapture) {
      println("userId,movieId,ratings,predictedRatings")
      println(data.toLocalIterator.toList.foreach(println))
    }
    val result = new String(outCapture.toByteArray)

    val pw = new PrintWriter(
      new File(filePath+"/"+fileName+fileFormat)
    )
    pw.write(result.replace(")","").
                    replace("(","").
                    replace(",",";")
    )
    pw.close

  }

  def savePredictionsData(data:RDD[Rating],filePath:String,fileName:String,fileFormat:String): Unit ={

    val outCapture = new ByteArrayOutputStream
    withOut(outCapture) {
      println("userId,movieId,predictedRating")
      println(data.toLocalIterator.toList.foreach(println))
    }
    val result = new String(outCapture.toByteArray)

    val pw = new PrintWriter(
      new File(filePath+"/"+fileName+fileFormat)
    )
    pw.write(result.replace("Rating","").
      replace(")","").
      replace("(","").
      replace(",",";"))
    pw.close

  }

}