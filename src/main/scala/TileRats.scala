import java.sql.Timestamp

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.sql.functions.{broadcast, col, lit, max, min, rand, floor, to_timestamp, udf, unix_timestamp, when}
import org.apache.commons.math3.distribution.GammaDistribution

import scala.util.Random

case class Rat (id: Long,
                latitude: Double,
                longitude: Double,
                tile_x: Int,
                tile_y: Int,
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
  val VALID_INFECTED_RATS = "(deadDate is null or deadDate >= current_timestamp()) and " +
    "(recoveredDate is null or recoveredDate >= current_timestamp())"

  def ratsFromDataframe(df : DataFrame, tileMetersSize: Int, infectedDate:Timestamp=null) : Dataset[Rat] = {
    val tileSize = (tileMetersSize / ((2 * PI / 360) * EARTH)) / 1000 //1 meter in degree

    df.crossJoin(broadcast(df.agg(
        min("latitude") as "min_latitude",
        max("latitude") as "max_latitude",
        min("longitude") as "min_longitude",
        max("longitude") as "max_longitude")))
      .withColumn("tile_x", floor((col("latitude")-col("min_latitude")) / tileSize)
        .cast(sql.types.IntegerType))
      .withColumn("tile_y", floor((col("longitude")-col("min_longitude"))/tileSize)
        .cast(sql.types.IntegerType))
      .withColumn("infectedDate",lit(infectedDate))
      .withColumn("recoveredDate",lit(null))
      .withColumn("deadDate",lit(null))
      .select("id","latitude","longitude",
        "tile_x","tile_y","infectedDate","deadDate", "recoveredDate")
      .as(rat_encoder)
  }

  def getInfectedTiles(rats:Dataset[Rat], infectionDistance:Int) : Dataset[TileArea] = {
    val infectedAreas = rats.filter(VALID_INFECTED_RATS)
      .withColumn("x_min", col("tile_x") - lit(infectionDistance))
      .withColumn("x_max", col("tile_x") + lit(infectionDistance))
      .withColumn("y_min", col("tile_y") - lit(infectionDistance))
      .withColumn("y_max", col("tile_y") + lit(infectionDistance))
      .select("x_min","x_max","y_min","y_max")
      .dropDuplicates()
    return infectedAreas.as(tile_area_encoder)
  }

  def getNewInfected(rats:Dataset[Rat], infectedRats:Dataset[Rat],infectionDistance:Int, infectionProbability:Double, seed:Option[Int] = null) : Dataset[Rat] = {
    val infectedAreas = broadcast(getInfectedTiles(infectedRats,infectionDistance))

    val strSeed = if(seed!=null) seed.get.toString() else ""
    val today = new Timestamp((new java.util.Date()).getTime)

    val x_restriction = col("ia.x_min") <= col("r.tile_x") &&
      col("ia.x_max") >= col("r.tile_x")
    val y_restriction = col("ia.y_min") <= col("r.tile_y") &&
      col("ia.y_max") >= col("r.tile_y")

    rats.as("r")
      .filter(s"rand($strSeed) <= $infectionProbability")
      .join(infectedAreas.as("ia"),
        x_restriction && y_restriction,
        "inner")
      .select(col("id")).distinct
      .join(rats,"id")
      .withColumn("infectedDate", lit(today))
      .select("id","latitude","longitude",
      "tile_x","tile_y","infectedDate","deadDate", "recoveredDate")
      .as(rat_encoder)
  }



  def addRemoveDate(infectedRats:Dataset[Rat], deadProbability:Double,seed:Option[Int]=null) : Dataset[Rat] = {
    val gammaRecovered = new GammaDistribution(21.5, 1.2)
    val gammaDeads = new GammaDistribution(18, 1)
    var randF = rand()
    if(seed!=null){
      gammaRecovered.reseedRandomGenerator(seed.get)
      gammaDeads.reseedRandomGenerator(seed.get)
      randF = rand(seed.get)
    }

    val day_s = 86400
    def randomDeadDate = () => {
      gammaDeads.sample
    }
    def randomRecoveredDate = () => {
      gammaRecovered.sample
    }
    val randomDeadDateUdf = udf(randomDeadDate)
    val randomRecoveredDateUdf = udf(randomRecoveredDate)
    //spark.udf.register("randGamma",randGamma)
    //new Timestamp(today.getTime()+Math.round(gammaDeads.sample*day_ms))
    val infectedUnixTime = unix_timestamp(col("infectedDate"),"yyyy-MM-dd HH:mm:ss.SSS")
    infectedRats
      .withColumn("deadRandomMs", infectedUnixTime + (lit(day_s)*randomDeadDateUdf()))
      .withColumn("recoveredRandomMs", infectedUnixTime + (lit(day_s)*randomRecoveredDateUdf()))
      .withColumn("isDead", when(randF <= deadProbability,1).otherwise(0))
      .withColumn("deadDate" , when(col("isDead") === 1, to_timestamp(col("deadRandomMs"))))
      .withColumn("recoveredDate" , when(col("isDead") =!= 1, to_timestamp(col("recoveredRandomMs"))))
      .select("id","latitude","longitude",
      "tile_x","tile_y","infectedDate","deadDate", "recoveredDate")
      .as(rat_encoder)
  }

  def updateRatsPosition(ratsOrigin:Dataset[Rat], ratsNewPosition:Dataset[Rat]):Dataset[Rat] = {
    ratsOrigin.as("or")
      .join(ratsNewPosition.as("new"), col("id"),"inner")
      .select("or.id","new.latitude","new.longitude",
        "new.tile_x","new.tile_y","or.infectedDate","or.deadDate", "or.recoveredDate")
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
