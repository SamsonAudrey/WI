import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

object Metrics {

  def show (predictionDf : DataFrame): Unit = {

    val predictionsAndLabelsN2 = predictionDf.select("prediction", "label").rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("accuracy")
    // measure the accuracy
    val accuracy2 = evaluator2.evaluate(predictionDf)


    val metricsN2 = new MulticlassMetrics(predictionsAndLabelsN2)

    val confusionMatrixN2 = metricsN2.confusionMatrix
    // compute the false positive rate per label


    println(s" Confusion Matrix\n ${confusionMatrixN2.toString}\n")


    println(s"ACCU : ${accuracy2*100}")
    println(s"RENTA : ${(confusionMatrixN2.apply(1,1)/(confusionMatrixN2.apply(1,1)+confusionMatrixN2.apply(0,1)))*100}")
    println(s"FINDS : ${(confusionMatrixN2.apply(1,1)/(confusionMatrixN2.apply(1,1)+confusionMatrixN2.apply(1,0)))*100}")
  }
}
