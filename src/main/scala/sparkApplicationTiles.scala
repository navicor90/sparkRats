import java.sql.Timestamp

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
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
    def createData(n_total : Int,n_infected:Int, radius:Int) : Dataset[Rat] = {
      val today = new Timestamp((new java.util.Date()).getTime)
      val df = sc.range(0,n_total).map(id => {
          val locs = randomLocationCloserTo(10,10,radius)
          val is_infected = if (id < n_infected) today else null
          (id, locs._1, locs._2 ,null, null,is_infected, null, null)
        }).toDF("id","latitude","longitude","tile_x","tile_y","infectedDate", "deadDate", "recoveredDate")
      return df.as(this.rat_encoder)
    }
    val rats = createData(100000,1000,100)

    val ratsWithTiles = assignTiles(rats=rats,tileMetersSize=1)
    val infectedTiles = getInfectedTiles(ratsWithTiles, 10)
    val newSick = getNewSick(rats = ratsWithTiles,
                             infectedAreas = infectedTiles,
                             infectionProbability = 0.00005)

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

    val df = tiles.toDF()
      //.withColumn("infectedDate", $"infectedDate".cast(sql.types.StringType))
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue","")
      .option("delimiter",",")
      .csv("sicks.export")

  }

}
