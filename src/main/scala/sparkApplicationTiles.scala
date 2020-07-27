import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.joda.time.DateTime
import sparkApplicationDatasetGenerator.runDatasetCreation

object sparkApplicationTiles extends TileRats {
  val infectedUnionParquet = "dataSource/infectedUnion.parquet"
  val datePattern = "yyyy-MM-dd hh:mm"
  val dateFormat = new SimpleDateFormat(datePattern)

  def main(args:Array[String]): Unit = {
    val allRatsParquet = "susceptibleRats"
    var initialInfectedRatsParquet:String = "infectedRats"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Rats Application")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    Random.setSeed(3)

    runDatasetCreation(spark,100000,100)
    runSimulation(spark,allRatsParquet,initialInfectedRatsParquet)

    for( i <- 1 to 5){
      println("<<<< Iteration "+i+" >>>>")
      Thread.sleep(60*1000) // wait for 60 seconds
      runSimulation(spark,allRatsParquet,null)
    }
  }

  def runSimulation(spark:SparkSession,allRatsParquet:String,initialInfectedRatsParquet:String) = {
    val tileMetersSize = 1
    val ratsAll = getRatsAll(spark, allRatsParquet, tileMetersSize)
    println("rats")
    spark.time(ratsAll.show())

    val infectedRats = getInfectedRats(spark, ratsAll, initialInfectedRatsParquet, tileMetersSize)
    println("infectedRats")
    spark.time(infectedRats.show())

    val susceptibleRats = ratsAll.join(infectedRats, Seq("id"),"left_anti").as(rat_encoder)
    println("susceptibleRats")
    spark.time(susceptibleRats.show())

    val newInfected = getNewInfected(rats = susceptibleRats,
      infectedRats = infectedRats,
      infectionDistance = 10,
      infectionProbability = 0.00005)

    println("newInfected")
    spark.time(newInfected.show())

    val newInfectedWithRemovedDate = addRemoveDate(newInfected,0.05)
    println("newInfectedWithRemovedDate")
    spark.time(newInfectedWithRemovedDate.show())

    val infectedUnion = newInfectedWithRemovedDate
      .union(infectedRats)

    println("infectedUnion")
    spark.time(infectedUnion.show())

    println("============ New Sick: ============")
    println(infectedUnion.count())
    println("====================================")

    val updatedSusceptibleRats = susceptibleRats.join(infectedUnion.select("id"), Seq("id"),"left_anti").as(rat_encoder)
    createStats(updatedSusceptibleRats, infectedUnion)

    infectedUnion
      .select("id","infectedDate","deadDate","recoveredDate")
      .write
      .mode("overwrite")
      .option("header", "true")
      .parquet(infectedUnionParquet)
  }

  def getInfectedRats(spark:SparkSession, ratsAll:Dataset[Rat], initialInfectedRatsParquet:String, tileMetersSize:Int):Dataset[Rat] = {
    var infectedRats = spark.createDataFrame(spark.sparkContext.emptyRDD[Rat]).as(rat_encoder)
    if(initialInfectedRatsParquet != null){
      val infectedDf = spark
        .read
        .format("parquet")
        .option("header", "true")
        .load(s"dataSource/$initialInfectedRatsParquet.parquet/*.parquet")
        .toDF()
        .join(ratsAll,"id")

      val yesterday = new DateTime(new java.util.Date()).minusDays(1).toDate()
      val sqlYesterday = new Timestamp(yesterday.getTime)
      infectedRats = addRemoveDate(ratsFromDataframe(infectedDf, tileMetersSize, sqlYesterday), 0.05)
    }else{
      infectedRats = spark
        .read
        .format("parquet")
        .option("header", "true")
        .load(s"$infectedUnionParquet/*.parquet")
        .cache()
        .toDF()
        .join(ratsAll
          .select("id","latitude","longitude", "tile_x","tile_y"),"id")
        .select("id","latitude","longitude",
          "tile_x","tile_y","infectedDate","deadDate", "recoveredDate")
        .as(rat_encoder)
    }
    infectedRats
  }

  def getRatsAll(spark:SparkSession, allRatsParquet:String, tileMetersSize:Int) :Dataset[Rat] = {
    val rats_df = spark
      .read
      .format("parquet")
      .option("header", "true")
      .load(s"dataSource/$allRatsParquet.parquet/*.parquet").toDF()
    ratsFromDataframe(rats_df, tileMetersSize)
  }

  def createStats(rats:Dataset[Rat], infectedRats:Dataset[Rat]) = {
    val dateDir = dateFormat.format(new Date())
    infectedRats.as("r")
      .groupBy("r.tile_x","r.tile_y")
      .agg(count("*").as("infected"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(s"export/$dateDir/infected_tiles.parquet")

    rats.union(infectedRats).agg(
      min("latitude") as "min_latitude",
      max("latitude") as "max_latitude",
      min("longitude") as "min_longitude",
      max("longitude") as "max_longitude",
      min("tile_x") as "min_tile_x",
      max("tile_x") as "max_tile_x",
      min("tile_y") as "min_tile_y",
      max("tile_y") as "max_tile_y",
      count("*") as "total")
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(s"export/$dateDir/border_info.parquet")

    infectedRats
      .groupBy(date_format(col("infectedDate"),datePattern).as("dateRep"))
      .agg(count("*").as("qty_infected"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(s"export/$dateDir/infecteds_history.parquet")
  }

}
