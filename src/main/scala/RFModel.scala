import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer, _}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.math.BigDecimal

object RFModel  {


    def load(): PipelineModel = {
        println("LOADING MODEL")

        val loadedModel: PipelineModel = PipelineModel.read.load("random-forest-model")
        loadedModel
    }


      // Assume that the DF is clean
      def train(df :DataFrame, sc : SparkContext ) {

        println("NB ROWS : " + df.count())

        //_________________________DATAFRAMES PREPARTION ________________________________
        // Choice of the features used to predict
        val cols = Array("appOrSiteIndex","bidFloor", "timestampIndex", "sizeIndex", "osIndex", "mediaIndex")

        // Adding the features column to the DF
        val assembler = new VectorAssembler()
          .setInputCols(cols)
          .setOutputCol("features")
        val featureDf = assembler.transform(df)


        // Creation of the prediction column linked to the label
        val indexer = new StringIndexer()
          .setInputCol("label")
          .setOutputCol("pred")




        // Spliting Data in test and train set
        val seed = 5043
        val Array(pipelineTrainingData, pipelineTestingData) = df.randomSplit(Array(0.75, 0.25), seed)
         //_______________________________________________________________________________________________________

          //_________________________________________CREATING CLASSIFIER __________________________________________

        val trueRate = 0.965
        val falseRate = 0.035

        println("RF RESULTS: ")

        println(s"  TRAINING : TRUE RATE : ${trueRate} AND FALSE RATE : $falseRate")
        val randomForestClassifier = new RandomForestClassifier().setImpurity("gini")
          .setMaxDepth(9)
          .setNumTrees(45)
          .setFeatureSubsetStrategy("auto")
          .setSeed(seed)
          .setThresholds(Array(trueRate,falseRate))
          .setMaxBins(100)

    //___________________________________________________________________________________________________________

    //_______________________________________CREATING MODEL ____________________________________________________

        val stages = Array(assembler, indexer, randomForestClassifier)

        // build pipeline
        val pipeline = new Pipeline().setStages(stages)
        val pipelineModel : PipelineModel = pipeline.fit(pipelineTrainingData)

        // test model with test data
        val predictionDf = pipelineModel.transform(pipelineTestingData)
        predictionDf.show(10)
    //_____________________________________________________________________________________________________________


    //____________________________________METRICS________________________________________________________________
        val predictionsAndLabelsN = predictionDf.select("prediction", "label").rdd
          .map(row => (row.getDouble(0), row.getDouble(1)))

        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setMetricName("accuracy")
        // measure the accuracy
        val evaluatorROC = new BinaryClassificationEvaluator()
          .setMetricName("areaUnderROC")
          .setLabelCol("label")
        val areaROC = evaluatorROC.evaluate(predictionDf)
        val accuracy = evaluator.evaluate(predictionDf)


        val metricsN = new MulticlassMetrics(predictionsAndLabelsN)

        val confusionMatrixN = metricsN.confusionMatrix
        // compute the false positive rate per label


        println(s" Confusion Matrix\n ${confusionMatrixN.toString}\n")
        val nbTrue = pipelineTestingData.filter(df("label")=== 1.0).count().toDouble
        val nbFalse = pipelineTestingData.filter(df("label")=== 0.0).count().toDouble

        println(s"ACCU  : ${accuracy*100}")
          println(s"AreaROC  : ${areaROC*100}")
        println(s"RENTA : ${(confusionMatrixN.apply(1,1)/(confusionMatrixN.apply(1,1)+confusionMatrixN.apply(0,1)))*100}")
        println(s"FINDS : ${(confusionMatrixN.apply(1,1)/nbTrue)*100}")


        println(s" NB FALSE : $nbFalse AND NB TRUE : $nbTrue")
    // _______________________________________________________________________________________________



    // ___________________________________ SAVE MODEL ________________________________________________
        pipelineModel.write.overwrite().save("random-forest-model")
    // _______________________________________________________________________________________________









      }
    def predict(df :DataFrame, model : PipelineModel, sc : SparkContext  ): Unit = {

        val predictionDf = model.transform(df)




    }
}
