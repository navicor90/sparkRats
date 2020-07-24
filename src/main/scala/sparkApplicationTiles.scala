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

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("ratsData/*.csv").toDF()

    val rats = ratsFromDataframe(df)
    println("rats")
    spark.time(rats.show())
    val ratsWithTiles = assignTiles(rats=rats,tileMetersSize=1)
    println("ratsWithTiles")
    spark.time(ratsWithTiles.show())
    val newSick = getNewSick(rats = ratsWithTiles,
                             infectionDistance = 10,
                             infectionProbability = 0.00005)
    println("newSick")
    spark.time(newSick.show())
    val newSickWithRemovedDate = getDeadOrRecoveredRats(newSick,0.05)
    println("newSickWithRemovedDate")
    spark.time(newSickWithRemovedDate.show())

    println("============ New Sicks: ============")
    println(newSick.count())
    println("====================================")

    val tiles = ratsWithTiles.select("id","infectedDate","tile_x","tile_y").as("r")
      .join(broadcast(newSick.select("id","infectedDate")).as("ns"), col("r.id")===col("ns.id"), "left")
      .select($"r.tile_x",
             $"r.tile_y",
             when($"ns.infectedDate".isNotNull || $"r.infectedDate".isNotNull,lit(1))
               .otherwise(0)
               .as("isInfected"))
        .groupBy("r.tile_x","r.tile_y")
        .agg(sum("isInfected").as("infecteds"),count("*").as("totals"))

    val tilesDf = tiles.toDF()
      //.withColumn("infectedDate", $"infectedDate".cast(sql.types.StringType))
    tilesDf.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue","")
      .option("delimiter",",")
      .csv("sicks.export")

  }

  def ratsFromDataframe(df : DataFrame) : Dataset[Rat] = {
    df.withColumn("tile_x",lit(null))
      .withColumn("tile_y",lit(null))
      .withColumn("recoveredDate",lit(null))
      .withColumn("deadDate",lit(null))
      .as(rat_encoder)
  }

}
