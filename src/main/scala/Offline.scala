package uni.mjinor.cancer

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, MultilabelClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.language.postfixOps

object Offline {

  val SPARK: SparkSession = SparkSession.builder().appName("Make Offline Model Of Breast Cancer").getOrCreate()

  val HDFS_PATH: String = "hdfs://user/mjinor/Breast_Cancer/"

  val ROOT_PATH: String = "file:///Users/mjinor/IdeaProjects/Breast_Cancer/"

  val FILE_NAME: String = "data.csv"

  def main(args: Array[String]): Unit = {

    SPARK.sparkContext.setLogLevel("WARN") // For Debug

    val df : DataFrame = loadData()

    var new_df: DataFrame = preprocess(df)

    val numeric_features: Array[String] = getNumericFeatures(new_df)

    val numeric_df = new_df.select(numeric_features.map(m => col(m)):_*)

    new_df = correlation(numeric_df)

    var columns = new_df.columns

    columns = columns.slice(0,columns.length - 1)

    val assembler = new VectorAssembler().setInputCols(columns).setOutputCol("features")

    new_df = assembler.transform(new_df)

    val Array(training, test) = new_df.randomSplit(Array[Double](0.8, 0.2), 18)

    val rf = new RandomForestClassifier().setLabelCol("diagnosis").setFeaturesCol("features").setNumTrees(5)

    val model = rf.fit(training)

    val rf_predictions = model.transform(test)

    val multi_evaluator = new MulticlassClassificationEvaluator().setLabelCol("diagnosis").setMetricName("accuracy")

    println("------------------------------------------------------------")

    println("Random Forest classifier Accuracy:" + multi_evaluator.evaluate(rf_predictions))

    println("------------------------------------------------------------")

    SPARK.stop()

  }

  def loadData(): DataFrame = {

    SPARK.read.format("csv").option("header","true").option("inferSchema","true").load(ROOT_PATH + FILE_NAME)

  }

  def correlation(df: DataFrame,showPercents:Boolean = false) : DataFrame = {

    val n : Int = df.columns.length
    val threshold : Double = 0.39
    var selects: List[String] = List[String]()
    for (i <- 0 until n) {
      var mean_corr: Double = 0.0
      val col_name : Array[String] = df.columns.slice(i,i+1).map(name => name)
      val Xs : RDD[Row] = df.select(df.columns.slice(i,i+1).map(name => col(name)):_*).rdd
      val seriesX: RDD[Double] = Xs.map(_.get(0).toString.toDouble)
      for(j <- 0 until n) {
        if (j != i) {
          val Ys : RDD[Row] = df.select(df.columns.slice(j,j+1).map(name => col(name)):_*).rdd
          val seriesY: RDD[Double] = Ys.map(_.get(0).toString.toDouble)
          val correlation = Statistics. corr(seriesX, seriesY, "pearson")
          mean_corr += correlation
        }
      }
      mean_corr = mean_corr / (n - 1)
      if (mean_corr > threshold) {
        selects ::= col_name(0)
      }
      if (showPercents)
        println(s"${col_name(0)} => Correlation is: $mean_corr")
    }
    df.select(selects.map(col): _*)
  }

  def preprocess(df: DataFrame) : DataFrame = {

    val condition : Column = when(col("diagnosis").equalTo("M"),1).otherwise(0)
    df.withColumn("binary_diagnosis",condition).drop("diagnosis","_c32").withColumnRenamed("binary_diagnosis","diagnosis")

  }

  def getNumericFeatures(df: DataFrame) : Array[String] = {

    val types: List[DataType] = List(IntegerType,DoubleType)
    var numeric_features: List[String] = List[String]()
    for (elem <- df.columns) {
      val x = df.select(elem).schema.toArray
      if (types.contains(x(0).dataType)) {
        numeric_features ::= elem
      }
    }
    numeric_features.toArray

  }

}
