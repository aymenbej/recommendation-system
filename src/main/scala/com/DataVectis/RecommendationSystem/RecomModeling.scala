package com.DataVectis.MainRecommendation

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

class RecomModeling{

  def getModelTraing(ratingsRDD:RDD[Rating],rank:Int,numIterations:Int,lambda:Double,alpha:Double)={

    //creating model for recommendations
    new ALS().setIterations(numIterations)
      .setBlocks(-1)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(12345L)
      .setImplicitPrefs(false)
      .run(ratingsRDD)
  }

  //get the test data with real ratings and predicted ratings
  def getPredictionTest(modelRecom: MatrixFactorizationModel,testRDD:RDD[Rating]): RDD[((Int,Int),(Double,Double))] ={

    val predictions = modelRecom.predict(testRDD.map(x => (x.user,x.product)))
    val predictionsData = predictions.map {
      x => ((x.user, x.product), x.rating)
    }.join(testRDD.map(x => ((x.user, x.product), x.rating)))

    predictionsData
  }

}