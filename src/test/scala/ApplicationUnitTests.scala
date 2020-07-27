import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random

class ApplicationUnitTests extends AnyFlatSpec with SparkSessionTestWrapper {
  val dateHourFormat = new SimpleDateFormat("yyyy-MM-dd hh")
  val today = new Timestamp((new java.util.Date()).getTime)
  import spark.implicits._

  it should "assign correctly tiles - ratsFromDataframe method" in {
    val df = Seq((1,33.13,-66.155),
                 (2,33.14,-66.14)).toDF("id","latitude","longitude")
    val tr = new TileRats()
    val ratsWithTiles = tr.ratsFromDataframe(df, 1)
    assert(ratsWithTiles.count()==2)
    val rat1 = ratsWithTiles.collect().apply(0)
    assert(rat1.tile_x==0)
    assert(rat1.tile_y==0)
    val rat2 = ratsWithTiles.collect().apply(1)
    assert(rat2.tile_x==1113)
    assert(rat2.tile_y==1670)
  }

  it should "assign correctly infected dates - ratsFromDataframe method" in {
    val df = Seq((1,33.13,-66.155,0,0),
                 (2,33.14,-66.14,1113,1670))
              .toDF("id","latitude","longitude","tile_x","tile_y")
    val tr = new TileRats()
    val ratsWithTiles = tr.ratsFromDataframe(df, 1, today)
    assert(ratsWithTiles.count()==2)
    val rat1 = ratsWithTiles.collect().apply(0)
    assert(dateHourFormat.format(rat1.infectedDate).equals(dateHourFormat.format(today)))
    val rat2 = ratsWithTiles.collect().apply(1)
    assert(dateHourFormat.format(rat2.infectedDate).equals(dateHourFormat.format(today)))
  }

  it should "create infected Areas from infected Rats" in {
    val someDay = new Timestamp(dateHourFormat.parse("2020-07-13 14").getTime)
    val somePosteriorDay = new Timestamp(dateHourFormat.parse("2020-07-28 14").getTime)

    val tr = new TileRats()
    val ds = Seq((1,33.13,-66.155,0,0,someDay,null,somePosteriorDay),
                 (2,33.14,-66.14,1113,1670,someDay,null,somePosteriorDay))
      .toDF("id","latitude","longitude","tile_x","tile_y","infectedDate","deadDate","recoveredDate")
      .as(tr.rat_encoder)

    val infectedAreas = tr.getInfectedTiles(ds, 4)
    assert(infectedAreas.count()==2)
    val tile1 = infectedAreas.filter($"id"===1).collect().apply(0)
    assert(tile1.x_min==(-4))
    assert(tile1.x_max==4)
    assert(tile1.y_min==(-4))
    assert(tile1.y_max==(4))
    val tile2 = infectedAreas.filter($"id"===2).collect().apply(0)
    assert(tile2.x_min==1109)
    assert(tile2.x_max==1117)
    assert(tile2.y_min==1666)
    assert(tile2.y_max==1674)
  }

  it should "create infected Areas avoiding dead rats" in {
    val someDay = new Timestamp(dateHourFormat.parse("2020-07-13 14").getTime)
    val somePosteriorDay = new Timestamp(dateHourFormat.parse("2020-07-28 14").getTime)
    val somePreviousDay = new Timestamp(dateHourFormat.parse("2020-06-28 14").getTime)

    val tr = new TileRats()
    val ds = Seq((1,33.13,-66.155,0,0,someDay,null,somePosteriorDay),
      (2,33.14,-66.14,1113,1670,someDay,null,somePosteriorDay),
      (3,33.1390,-66.1490,1100,1300,someDay,null,somePreviousDay))
      .toDF("id","latitude","longitude","tile_x","tile_y","infectedDate","deadDate","recoveredDate")
      .as(tr.rat_encoder)

    val infectedAreas = tr.getInfectedTiles(ds, 4)
    assert(infectedAreas.count()==2)
  }

  it should "get new infected Rats from another ones(prob 0.4)" in {
    val seed = 3
    Random.setSeed(seed)

    val ratsList = List(Rat(1,33.13,-66.155,0,0,null,null,null),
      Rat(2,33.14,-66.1450005,1113,1665,null,null,null),
      Rat(3,33.14,-66.1450008,1113,1663,null,null,null))

    val infectedList = List(Rat(4,33.14,-66.14,1113,1670,today,null,null))

    val rats = spark.createDataset(ratsList).as[Rat]
    val infected = spark.createDataset(infectedList).as[Rat]
    val tr = new TileRats()
    val newSick = tr.getNewInfected(rats, infected, 10, 0.5, Some(seed))
    val expectedSick = Rat(3, 33.14, -66.1450008, 1113, 1663, today, null, null)
    assert(newSick.count() == 1)
    assert(newSick.collect().apply(0).id == expectedSick.id)
    assert(newSick.collect().apply(0).tile_x == expectedSick.tile_x)
    assert(newSick.collect().apply(0).tile_y == expectedSick.tile_y)
    val formattedDate = dateHourFormat.format(newSick.collect().apply(0).infectedDate)
    assert(formattedDate.equals(dateHourFormat.format(expectedSick.infectedDate)))
  }

  it should "add Remove dates in infected rats " in {
    val seed = 3
    Random.setSeed(seed)
    val someDay = new Timestamp(dateHourFormat.parse("2020-07-13 14").getTime)

    val rats_list = List(Rat(1,33.13,-66.155,0,0,someDay,null,null),
                          Rat(2,33.14,-66.1450005,1113,1665,someDay,null,null),
                          Rat(3,33.14,-66.1450008,1113,1663,someDay,null,null),
                          Rat(4,33.14,-66.14,1113,1670,someDay,null,null))

    val infectedRats = spark.createDataset(rats_list).as[Rat]
    val tr = new TileRats()
    val removedRats = tr.addRemoveDate(infectedRats,0.05, Some(seed))

    assert(removedRats.filter($"recoveredDate".isNotNull).count() == 3)

    val expectedDate = dateHourFormat.parse("2020-08-01 07")
    val deadRat = removedRats.filter($"deadDate".isNotNull).collect().apply(0)
    assert(dateHourFormat.format(deadRat.deadDate).equals(dateHourFormat.format(expectedDate)))
  }

}
