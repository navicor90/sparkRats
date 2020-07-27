import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object sparkApplicationDatasetGenerator extends TileRats {

  def main(args:Array[String]): Unit = {
    var generateInfecteds = false
    if (args.length > 0) {
      generateInfecteds = args(0).toBoolean //true
    }

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Rats Application")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val totalRats = 100000
    val infectedRats = 100
    if(generateInfecteds){
      runDatasetCreation(spark, totalRats,infectedRats)
    }else{
      runDatasetCreation(spark, totalRats,0)
    }
  }


    def runDatasetCreation(spark:SparkSession,totalRats:Int, infectedRats:Int)={
      Random.setSeed(3)
      import spark.implicits._
      val sc = spark.sparkContext
      def createData(n_total : Int, radius:Int) : DataFrame = {
        val df = sc.range(0,n_total).map(id => {
          val locs = randomLocationCloserTo(10,10,radius)
          (id, locs._1, locs._2 )
        }).toDF("id","latitude","longitude")
        return df
      }
      val df = createData(totalRats,100)
      df.write
        .mode("overwrite")
        .option("header", "true")
        .parquet("dataSource/susceptibleRats.parquet")

      if(infectedRats>0){
        df.filter(col("id")<infectedRats).select("id")
          .write
          .mode("overwrite")
          .option("header", "true")
          .parquet("dataSource/infectedRats.parquet")
      }
    }

}
