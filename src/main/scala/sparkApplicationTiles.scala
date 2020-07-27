import java.sql.Timestamp

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import scala.util.Random
import org.apache.spark.sql
import org.apache.log4j.Logger
import org.apache.log4j.Level

object sparkApplicationTiles extends TileRats {

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

    val rats_df = spark
      .read
      .format("parquet")
      .option("header", "true")
      .load("dataSource/healthyRats.parquet/*.parquet").toDF()

    val infected_df = spark
      .read
      .format("parquet")
      .option("header", "true")
      .load("dataSource/infectedRats.parquet/*.parquet").toDF()

    val tileMetersSize = 1
    val rats = ratsFromDataframe(rats_df, tileMetersSize)

    val today = new Timestamp((new java.util.Date()).getTime)
    val infectedRats = addRemoveDate(ratsFromDataframe(infected_df, tileMetersSize, today), 0.05)

    println("rats")
    spark.time(rats.show())

    val newInfected = getNewInfected(rats = rats,
                             infectedRats = infectedRats,
                             infectionDistance = 10,
                             infectionProbability = 0.00005)

    println("newInfected")
    spark.time(newInfected.show())
    val newInfectedWithRemovedDate = addRemoveDate(newInfected,0.05)

    println("newSickWithRemoveDate")
    spark.time(newInfectedWithRemovedDate.show())

    val infectedUnion = newInfectedWithRemovedDate
      .union(infectedRats)

    infectedUnion.write
      .mode("overwrite")
      .option("header", "true")
      .parquet("dataSource/infectedUnion.parquet")

    println("============ New Sicks: ============")
    println(infectedUnion.count())
    println("====================================")

    infectedUnion.as("r")
        .groupBy("r.tile_x","r.tile_y")
        .agg(count("*").as("infected"))
        .write
        .mode("overwrite")
        .parquet("export")

  }

}
