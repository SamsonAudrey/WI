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

        println("LOADING MODEL \n")
        val pathToModel = "random-forest-model"
        val loadedModel: PipelineModel = PipelineModel.read.load(pathToModel)
        loadedModel
    }


      // Assume that the DF is clean
      def train(df :DataFrame ) {

        val nbTree = 40
        val depth = 8
        val maxBins = 100
        val trueRate = 0.965
        val falseRate = 0.035
        val seed = 5043
        val impurity = "gini"

        println("NB ROWS : " + df.count())

        //_________________________DATAFRAMES PREPARTION ________________________________
        // Choice of the features used to predict
        val cols = Array("appOrSiteIndex","bidFloor", "timestampIndex", "sizeIndex", "osIndex", "mediaIndex")

        // Adding the features column to the DF
        val assembler = new VectorAssembler()
          .setInputCols(cols)
          .setOutputCol("features")



        // Creation of the prediction column linked to the label
        val indexer = new StringIndexer()
          .setInputCol("label")
          .setOutputCol("pred")




        // Spliting Data in test and train set

        val Array(pipelineTrainingData, pipelineTestingData) = df.randomSplit(Array(0.75, 0.25), seed)
         //_______________________________________________________________________________________________________

          //_________________________________________CREATING CLASSIFIER __________________________________________



        println("RF RESULTS: ")


        val randomForestClassifier = new RandomForestClassifier()
          .setImpurity(impurity)
          .setMaxDepth(depth)
          .setNumTrees(nbTree)
          .setFeatureSubsetStrategy("auto")
          .setSeed(seed)
          .setThresholds(Array(trueRate,falseRate))
          .setMaxBins(maxBins)

    //___________________________________________________________________________________________________________



    //_______________________________________CREATING MODEL ____________________________________________________

        val stages = Array(assembler, indexer, randomForestClassifier)

        // build pipeline
        val pipeline = new Pipeline().setStages(stages)
        val pipelineModel : PipelineModel = pipeline.fit(pipelineTrainingData)

        // test model with test data
        val predictionDf = pipelineModel.transform(pipelineTestingData)

    //_____________________________________________________________________________________________________________


    //____________________________________METRICS________________________________________________________________
       Metrics.show(predictionDf)
    // _______________________________________________________________________________________________


    // ___________________________________ SAVE MODEL ________________________________________________
        pipelineModel.write.overwrite().save("random-forest-model")
    // _______________________________________________________________________________________________


      }



    def predict(df :DataFrame, model : PipelineModel  ): DataFrame = {

        println("START PREDICTION \n")
        val predictionDf = model.transform(df)
        println("PREDICTION COMPLETED  \n ")
        predictionDf


    }
}
