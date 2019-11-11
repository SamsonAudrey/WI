package models

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import tools.Metrics

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
        df.show(10)
        //_________________________DATAFRAMES PREPARTION ________________________________
        // Choice of the features used to predict
        val cols = Array("appOrSite","bidFloor", "timestamp", "size", "os", "media" , "type" ,"exchange", "IAB1", "IAB2", "IAB3", "IAB4", "IAB5", "IAB6", "IAB7", "IAB8", "IAB9", "IAB10", "IAB11", "IAB12", "IAB13", "IAB14", "IAB15", "IAB16", "IAB17", "IAB18", "IAB19", "IAB20", "IAB21", "IAB22", "IAB23", "IAB24", "IAB25", "IAB26")

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
          //.setThresholds(Array(trueRate,falseRate))
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
