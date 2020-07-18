import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}
import java.sql.Timestamp
import scala.util.Random;

object sparkApplication {
  val EARTH = 6378.137 //radius of the earth in kilometer
  val PI = Math.PI
  val M = (1 / ((2 * PI / 360) * EARTH)) / 1000 //1 meter in degree

  case class Rat (id: Long,
                  latitude: Double,
                  longitude: Double,
                  infectedDate:Timestamp,
                  deadDate:Timestamp,
                  recoveredDate:Timestamp)

  case class Area (latitude_min:Double,
                   latitude_max:Double,
                   longitude_min:Double,
                   longitude_max:Double)

  val rat_encoder = Encoders.product[Rat]
  val area_encoder = Encoders.product[Area]

  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Rats Application")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val precision = 10

    def createData(n_total : Int,n_infected:Int, radius:Int) : Dataset[Rat] = {
      val today = new Timestamp((new java.util.Date()).getTime)
      val df = sc.range(0,n_total).map(id => {
          val locs = randomLocationCloserTo(10,10,radius)
          val is_infected = if (id < n_infected) today else null
          (id, locs._1, locs._2, is_infected , null, null)
        }).toDF("id","latitude","longitude", "infectedDate", "deadDate", "recoveredDate")
      return df.as[Rat]
    }

    val n_infected = 10
    val rats = createData(100000,1000,100)
    val infectedAreas = getInfectedAreas(rats, 10)
    val newSick = getNewSick(rats = rats,
                                  infectedAreas = infectedAreas,
                                  infectionProbability = 0.00005)

    println("============ New Sicks: ============")
    println(newSick.count())
    println("====================================")
  }

  def getInfectedAreas(rats:Dataset[Rat], infectionDistance:Double) : Dataset[Area] = {
    val distanceInDegrees = infectionDistance*M
    val infectedAreas = rats
      .filter("infectedDate is not null")
      .withColumn("latitude_min", col("latitude") - distanceInDegrees)
      .withColumn("latitude_max", col("latitude") + distanceInDegrees)
      .withColumn("longitude_min", col("longitude") - (lit(distanceInDegrees) / cos(col("latitude_min")* PI/180)))
      .withColumn("longitude_max", col("longitude") + (lit(distanceInDegrees) / cos(col("latitude_max")* PI/180)))
      .select("latitude_min","latitude_max","longitude_min","longitude_max")
    return infectedAreas.as(area_encoder)
  }

  def getNewSick(rats:Dataset[Rat], infectedAreas:Dataset[Area], infectionProbability:Double) : Dataset[Rat] = {
    val latitude_restriction = col("ia.latitude_min") <= col("r.latitude") &&
                               col("ia.latitude_max") >= col("r.latitude")
    val longitude_restriction = col("ia.longitude_min") <= col("r.longitude") &&
                                col("ia.longitude_max") >= col("r.longitude")
    val newSick = rats.as("r")
                      .filter(s"infectedDate is null and rand() <= $infectionProbability")
                      .join(infectedAreas.as("ia"),
                            latitude_restriction && longitude_restriction,
                        "inner")
                      .select(col("id")).distinct
                      .join(rats,"id")
    return newSick.as(rat_encoder)
  }

  def getDeadOrRecoveredRats(infectedRats:Dataset[Rat]) : Dataset[Rat] = {
    return null
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
