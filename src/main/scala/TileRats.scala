import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.sql.functions.{broadcast, col, lit, max, min, round, udf, rand, when}
import org.apache.commons.math3.distribution.GammaDistribution
import scala.util.Random

case class Rat (id: Long,
                latitude: Double,
                longitude: Double,
                tile_x: Option[Int],
                tile_y: Option[Int],
                infectedDate:Timestamp,
                deadDate:Timestamp,
                recoveredDate:Timestamp)

case class TileArea (x_min:Int,
                     x_max:Int,
                     y_min:Int,
                     y_max:Int)

class TileRats {
  val EARTH = 6378.137 //radius of the earth in kilometer
  val PI = Math.PI

  val rat_encoder = Encoders.product[Rat]
  val tile_area_encoder = Encoders.product[TileArea]

  def assignTiles(rats: Dataset[Rat], tileMetersSize: Int) : Dataset[Rat] = {
    val tileSize = (tileMetersSize / ((2 * PI / 360) * EARTH)) / 1000 //1 meter in degree
    val ratsWithAgg = rats
      .crossJoin(broadcast(rats.agg(
        min("latitude") as "min_latitude",
        max("latitude") as "max_latitude",
        min("longitude") as "min_longitude",
        max("longitude") as "max_longitude")))
      .withColumn("tile_x", round((col("latitude")-col("min_latitude")) / tileSize)
        .cast(sql.types.IntegerType))
      .withColumn("tile_y", round((col("longitude")-col("min_longitude"))/tileSize)
        .cast(sql.types.IntegerType))

    return ratsWithAgg.select("id","latitude","longitude",
        "tile_x","tile_y","infectedDate","deadDate", "recoveredDate").as(rat_encoder)
  }

  def getInfectedTiles(rats:Dataset[Rat], infectionDistance:Int) : Dataset[TileArea] = {
    val infectedAreas = rats
      .filter("infectedDate is not null")
      .withColumn("x_min", col("tile_x") - lit(infectionDistance))
      .withColumn("x_max", col("tile_x") + lit(infectionDistance))
      .withColumn("y_min", col("tile_y") - lit(infectionDistance))
      .withColumn("y_max", col("tile_y") + lit(infectionDistance))
      .select("x_min","x_max","y_min","y_max")
      .dropDuplicates()
    return infectedAreas.as(tile_area_encoder)
  }

  def getNewSick(rats:Dataset[Rat],infectionDistance:Int, infectionProbability:Double, seed:Option[Int] = null) : Dataset[Rat] = {
    val infectedAreas = getInfectedTiles(rats,infectionDistance)
    val strSeed = if(seed!=null) seed.get.toString() else ""
    val today = new Timestamp((new java.util.Date()).getTime)
    val x_restriction = col("ia.x_min") <= col("r.tile_x") &&
      col("ia.x_max") >= col("r.tile_x")
    val y_restriction = col("ia.y_min") <= col("r.tile_y") &&
      col("ia.y_max") >= col("r.tile_y")
    val newSick = rats.as("r")
      .filter(s"infectedDate is null and rand($strSeed) <= $infectionProbability")
      .join(infectedAreas.as("ia"),
        x_restriction && y_restriction,
        "inner")
      .select(col("id")).distinct
      .join(rats,"id")
      .withColumn("infectedDate", lit(today))
    return newSick.select("id","latitude","longitude",
      "tile_x","tile_y","infectedDate","deadDate", "recoveredDate").as(rat_encoder)
  }



  def getDeadOrRecoveredRats(infectedRats:Dataset[Rat], deadProbability:Double,seed:Option[Int]=null) : Dataset[Rat] = {
    val gammaRecovered = new GammaDistribution(21.5, 1.2)
    val gammaDeads = new GammaDistribution(18, 1)
    var randF = rand()
    if(seed!=null){
      gammaRecovered.reseedRandomGenerator(seed.get)
      gammaDeads.reseedRandomGenerator(seed.get)
      randF = rand(seed.get)
    }

    val day_ms = 86400000
    val today = new java.util.Date()
    def randomDeadDate = () => {
        new Timestamp(today.getTime()+Math.round(gammaDeads.sample*day_ms))
    }
    def randomRecoveredDate = () => {
      new Timestamp(today.getTime()+Math.round(gammaRecovered.sample*day_ms))
    }
    val randomDeadDateUdf = udf(randomDeadDate)
    val randomRecoveredDateUdf = udf(randomRecoveredDate)
    //spark.udf.register("randGamma",randGamma)

    infectedRats
      .withColumn("isDead", when(randF <= deadProbability,1).otherwise(0))
      .withColumn("deadDate" , when(col("isDead") === 1, randomDeadDateUdf()))
      .withColumn("recoveredDate" , when(col("isDead") =!= 1, randomRecoveredDateUdf()))
      .as(rat_encoder)
  }

  def randomLocationCloserTo(x0:Double, y0:Double, radius:Double): (Double,Double) = {
    // Convert radius from meters to degrees
    val radiusInDegrees = radius / 111000f;
    val u = Random.nextDouble
    val v = Random.nextDouble
    val w = radiusInDegrees * Math.sqrt(u)
    val t = 2 * PI * v
    val x = w * Math.cos(t)
    val y = w * Math.sin(t)
    val new_x = x / Math.cos(Math.toRadians(y0))
    (new_x + x0 , y+y0)
  }


}
