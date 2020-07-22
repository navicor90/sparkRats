import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object sparkApplicationDatasetGenerator extends TileRats {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Rats Application")

    Random.setSeed(3)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    def createData(n_total : Int,n_infected:Int, radius:Int) : DataFrame = {
      val today = new Timestamp((new java.util.Date()).getTime)
      val df = sc.range(0,n_total).map(id => {
          val locs = randomLocationCloserTo(10,10,radius)
          val infectedDate = if (id < n_infected) today else null
          (id, locs._1, locs._2 ,infectedDate)
        }).toDF("id","latitude","longitude","infectedDate")
      return df
    }
    val df = createData(100000,1000,100)
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue","")
      .option("delimiter",",")
      .csv("ratsData")

  }

}
