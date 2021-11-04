import classifier._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Helper. _
import utils.{ComplexityMetrics, Mediator}

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

case class ParametersPerFold(mostImpFeatures: Array[String], redPerc: Double)

object Launcher extends Serializable {

  // @TODO: convert the following as app's parameters
  // GMs as baseline (GMbsl).
  val gmBsl = Map("susy" -> 0.76124)

  // list of suggested reduction percentages
  val redPercList: Array[Double] = Array(0.99, 0.98, 0.97, 0.96, 0.95, 0.94, 0.93, 0.92, 0.91, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1)

  // predictive loss threshold (PLT)
  val plt = 0.01

  //  here data is already split following the 5fcv scheme (if not, use `MLUtils.kFold()` after loading the dataset)
  val k = 5  // this is also used for the `RPR hyperparametrization (stage3)` @NOTE: there should be another parameter for stage 3

  val origColClassName = "Class"
  val numColClassName = "numClass"
  // The StringIndex will set 0.0 to the majority class, and 1.0 to the minority
  val numClassMaj = 0.0
  val numClassMin = 1.0


  def main(args: Array[String]): Unit = {
    val paramStr = "hostLocation, rootDir, headerFile, inputFile, testFile, datasetName, timestamp, fold, numPartitions, inputDelimiterAtrr, minClassName, majClassName, it, overPercentage"
    val parameters = parseArgs(args, paramStr)
    val mediator = new Mediator()
    mediator.setMediatorWithParams(parameters)
    val spark = initialize(mediator)

    // find the optimal parameters for each original fold
    val paramPerFold = Array.ofDim[ParametersPerFold](k)
    val featArr: ArrayBuffer[String] = ArrayBuffer()
    val redPercs: ArrayBuffer[Double] = ArrayBuffer()
    val t1 = Instant.now
    for (f <- 0 until k) {
        println("INFO: Processing ORIGINAL fold: " + (f + 1))
        paramPerFold(f) = findRecommendedParamPerFold(spark, mediator, f + 1)
        redPercs ++= collection.mutable.ArrayBuffer(paramPerFold(f).redPerc)
        featArr ++= collection.mutable.ArrayBuffer(paramPerFold(f).mostImpFeatures: _*)
    }


    // recommended percentage reduction (RPR) based on aggregation criteria
    val rpr = if (mediator.jarFile.contains("_min")) redPercs.min else redPercs.toArray.sortWith(_ > _).drop(redPercs.length / 2).head
    if (rpr == Double.MaxValue) {
      println("INFO: this dataset cannot be reduce with the PLT and the following percentage of reduction:")
      println(redPercList.mkString(", "))
      System.exit(1)
    }

    // find the features that were selected as important in more than the 50% of the k k.
    val recommFeats = featArr
      .map(e => (e, 1))
      .groupBy(_._1)
      .filter(_._2.length > (1 + k/2))
      .keys
      .toArray


    val recommTime = getRuntimeMilis(
      t1,
      Instant.now
    )
  }


  def findRecommendedParamPerFold(
      spark: SparkSession,
      mediator: Mediator,
      f: Int
  ): ParametersPerFold = {
    mediator.fold = f

    // read data and index categorical features using StringIndexer
    // get the most important features
    var (traFS, _, mif) = {
      readAndIndexDataByFold(
        spark,
        mediator
      )
    }
    traFS.persist()

    // remove original class column and idx
    traFS = traFS.drop(origColClassName, "idx")

    // drop duplicates
    var origTra = traFS.count()
    println("origTra " + origTra)
    traFS = traFS.dropDuplicates()
    traFS.persist()
    val afterDropTraNum = traFS.count()

    // `RPR hyperparametrization (stage3)`
    val numCVFolds = k
    println("INFO: plt parameter: " + plt)
    println("INFO: criterion: " +  (if (mediator.jarFile.contains("_min")) "minimum" else "media"))
    val schema = traFS.schema
    val predColumns = traFS.columns
    val splits = MLUtils.kFold(traFS.rdd, numCVFolds, mediator.seed)
    traFS.unpersist()
    val gmBslTst: Double = gmBsl(mediator.dataset)

    val redPercs = splits.zipWithIndex.map {
      case ((tra, tst), splitIndex) =>
        val subTra = spark.createDataFrame(tra, schema).cache()
        val subTst = spark.createDataFrame(tst, schema).cache()

        val (numMin, numMaj) = getInstancesByClass(subTra)

        var redPercIndex = 0; var gmRed = gmBslTst; var redPerc = 0.0; var gmRedInRange, gmRedIsLtBsl = false
        do {
          redPerc = redPercList(redPercIndex)

          val traRed = uniformReduction(
            mediator,
            redPerc,
            subTra,
            numMin,
            numMaj
          )
          // convert columns in vector (VectorAssembler)
          val assmbldTraData = getColumnsAssembled(traRed, predColumns)
          val assmbldTstData = getColumnsAssembled(subTst, predColumns)

          subTra.unpersist()
          subTst.unpersist()

          // running DT and get the GMred
          gmRed = classifier.runClassifier(
            spark,
            mediator,
            cls = "DT",
            traData = assmbldTraData,
            tstData = assmbldTstData,
            valData = null
          ).predsTst.GM()

          gmRedIsLtBsl = gmRed < gmBslTst
          gmRedInRange = 1 - (gmRed / gmBslTst) > plt
          redPercIndex += 1

        } while (gmRedIsLtBsl & !gmRedInRange & (redPercIndex < redPercList.length))

        if (!gmRedIsLtBsl) {
          println("INFO: gmRed GTE gmBslTst")
        } else if (gmRedInRange) {
          println( "INFO: gmRed is in range of gmBslTst. Diff: " + (1 - (gmRed / gmBslTst)) )
        } else if (redPercIndex == redPercList.length) {
          println("INFO: all reduction percentages were analyzed.")
          redPerc = Double.MaxValue
        } else {
          println("ERROR: UNEXPECTED REASON")
        }

        redPerc
    }
    println("redPercs for FOLD = " + f + " : " + redPercs.mkString(", "))

    // aggregate the redPercs for this fold
    val redPerc = if (mediator.jarFile.contains("_min")) redPercs.min else redPercs.sortWith(_ > _).drop(redPercs.length / 2).head

    ParametersPerFold.apply(
      mostImpFeatures = mif,
      redPerc = redPerc
    )
  }



  def getInstancesByClass(df: DataFrame): (Long, Long) = {
    val numMin = df.filter(df(numColClassName).contains(numClassMin)).count()

    val numMaj = df.count() - numMin

    (numMin, numMaj)
  }

  def uniformReduction(          mediator: Mediator,
                                perc: Double,
                                numTraData: DataFrame,
                                numMin: Long,
                                numMaj: Long
                              ): DataFrame = {
    val rndSamplePerc = ((1.0 - perc) * 100) - (((1.0 - perc) * 100) % 0.01)
    println("Taking a sample of " + rndSamplePerc + " %")
    mediator.rrPerc = perc
    val samplePerc = 1.0 - perc

    // final qty of instances for each class
    val finalQty = math
      .ceil((numTraData.count() * samplePerc) / 2)
      .toInt
    val seed = mediator.seed

    getBalancedSample(
      finalQty,
      seed = mediator.seed,
      numMin = numMin,
      numMaj = numMaj,
      numTraData
    )
  }

  def getBalancedSample(
                         finalQty: Int,
                         seed: Int,
                         numMin: Double,
                         numMaj: Double,
                         trainingDataset: DataFrame
                       ): DataFrame = {
    val fracMin = finalQty.toDouble / numMin
    val fracMaj = finalQty.toDouble / numMaj
    val minSample = trainingDataset
      .filter(trainingDataset(numColClassName).contains(numClassMin))
      .sample(
        withReplacement = if (numMin < finalQty) true else false,
        fraction = fracMin,
        seed
      )
    val majSample = trainingDataset
      .filter(trainingDataset(numColClassName).contains(numClassMaj))
      .sample(
        withReplacement = if (numMaj < finalQty) true else false,
        fraction = fracMaj,
        seed
      )

   minSample.union(majSample)

  }

}
