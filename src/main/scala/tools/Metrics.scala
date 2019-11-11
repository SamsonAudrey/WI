package tools

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

object Metrics {

  def show (predictionDf : DataFrame): Unit = {
    println("METRICS : \n")

    val predictionsAndLabels = predictionDf.select("prediction", "label").rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("accuracy")
    // measure the accuracy
    val accuracy = evaluator.evaluate(predictionDf)


    val metrics = new MulticlassMetrics(predictionsAndLabels)

    val confusionMatrix = metrics.confusionMatrix
    // compute the false positive rate per label


    println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")


    println(s"ACCURACY      : ${(accuracy*100).toString } %")
    //                                      TP                 /        TP               +          FP
    println(s"RAPPEL : ${ ( (confusionMatrix.apply(0,0) / (confusionMatrix.apply(0,0)+confusionMatrix.apply(0,1)) )*100 ).toString } %")
    //                                      TP                 /        TP               +          FN
    println(s"PRECISION         : ${ ( (confusionMatrix.apply(0,0) / (confusionMatrix.apply(0,0)+confusionMatrix.apply(1,0)) )*100 ).toString } %")



  }
}
