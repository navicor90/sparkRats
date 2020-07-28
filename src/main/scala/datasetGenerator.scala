import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object datasetGenerator extends TileRats {

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
        .parquet("dataSource/allRats.parquet")

      if(infectedRats>0){
        df.filter(col("id")<infectedRats).select("id")
          .write
          .mode("overwrite")
          .option("header", "true")
          .parquet("dataSource/infectedRats.parquet")
      }
    }

}
