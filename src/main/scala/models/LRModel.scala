package models

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object LRModel {

  def train(df : DataFrame): Unit = {


    val cols = Array("appOrSiteIndex","bidFloor", "timestampIndex", "sizeIndex", "osIndex", "mediaIndex")


    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
    val featureDf = assembler.transform(df)
    featureDf.printSchema()




    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("pred")
    val labelDf = indexer.fit(featureDf).transform(featureDf)


    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

    // train logistic regression model with training data set
    val logisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)
      //.setWeightCol("classWeightCol")
    val logisticRegressionModel = logisticRegression.fit(trainingData)

    // run model with test data set to get predictions
    // this will add new columns rawPrediction, probability and prediction
    val predictionDf = logisticRegressionModel.transform(testData)
    predictionDf.show(10)
    println("LOGISTIC RESULTS")
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("accuracy")

    // measure the accuracy
    val accuracy = evaluator.evaluate(predictionDf)
    println(accuracy)

    // evaluate the model
    val predictionsAndLabels = predictionDf.select("prediction", "pred").rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new MulticlassMetrics(predictionsAndLabels)

    val confusionMatrix = metrics.confusionMatrix
    val predictionValues = predictionDf.select ("label", "prediction", "rawPrediction")
    println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")
    val nbTrue = testData.filter(df("label")=== 1.0).count()
    val nbFalse = testData.filter(df("label")=== 0.0).count()
    println(s" NB FALSE : $nbFalse AND NB TRUE : $nbTrue")

  }

  def balanceDataset(dataset: DataFrame): DataFrame = {
    val datasetSize = dataset.count
    val numNegatives = dataset.filter(dataset("label") === 0.0).count

    val balancingRatio = numNegatives.toDouble/datasetSize.toDouble
    val calculateWeights = udf { d: Int =>
      if (d == 0.0) {
        balancingRatio
      }
      else {
        (1.0 - balancingRatio)
      }
    }
    dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
  }

}
