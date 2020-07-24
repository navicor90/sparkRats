import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Random

class ApplicationUnitTests extends AnyFlatSpec with SparkSessionTestWrapper {
  val today = new Timestamp((new java.util.Date()).getTime)
  import spark.implicits._

  it should "assign correctly tiles assignTiles method" in {
    val rats_list = List(Rat(1,33.13,-66.155,null,null,null,null,null),
                         Rat(2,33.14,-66.14,null,null,today,null,null))
    val ds = spark.createDataset(rats_list).as[Rat]
    val tr = new TileRats()
    val ratsWithTiles = tr.assignTiles(ds, 1)
    val expectedResult1 = Rat(1,33.13,-66.155,Some(0),Some(0),null,null,null)
    val expectedResult2 = Rat(2,33.14,-66.14,Some(1113),Some(1670),today,null,null)
    assert(ratsWithTiles.count()==2)
    assert(ratsWithTiles.collect().apply(0) == expectedResult1)
    assert(ratsWithTiles.collect().apply(1) == expectedResult2)
  }

  it should "get the infected tiles from infected Rats" in {
    val rats_list = List(Rat(1,33.13,-66.155,Some(0),Some(0),null,null,null),
                         Rat(1,33.13,-66.1550005,Some(0),Some(5),null,null,null),
                         Rat(2,33.14,-66.14,Some(1113),Some(1670),today,null,null))
    val ds = spark.createDataset(rats_list).as[Rat]
    val tr = new TileRats()
    val infectedTiles = tr.getInfectedTiles(ds, 10)
    val expectedTile = TileArea(1103,1123,1660,1680)
    assert(infectedTiles.count()==1)
    assert(infectedTiles.collect().apply(0)==expectedTile)
  }

  it should "get new Sick from infected rats with prob 0.4" in {
    val seed = 3
    Random.setSeed(seed)

    val rats_list = List(Rat(1,33.13,-66.155,Some(0),Some(0),null,null,null),
                        Rat(2,33.14,-66.1450005,Some(1113),Some(1665),null,null,null),
                        Rat(3,33.14,-66.1450008,Some(1113),Some(1663),null,null,null),
                        Rat(4,33.14,-66.14,Some(1113),Some(1670),today,null,null))

    val ds = spark.createDataset(rats_list).as[Rat]
    val tr = new TileRats()
    val newSick = tr.getNewSick(ds, 10, 0.5, Some(seed))
    val expectedSick = Rat(3,33.14,-66.1450008,Some(1113),Some(1663),today,null,null)
    assert(newSick.count() == 1)
    assert(newSick.collect().apply(0).id == expectedSick.id)
    assert(newSick.collect().apply(0).tile_x == expectedSick.tile_x)
    assert(newSick.collect().apply(0).tile_y == expectedSick.tile_y)
    val dateFormat = new SimpleDateFormat("yyyy-mm-dd hh")
    val formattedDate = dateFormat.format(newSick.collect().apply(0).infectedDate)
    assert(formattedDate.equals(dateFormat.format(expectedSick.infectedDate)))
  }

  it should "add dead and recovered dates in infected rats " in {
    val seed = 3
    Random.setSeed(seed)

    val rats_list = List(Rat(1,33.13,-66.155,Some(0),Some(0),today,null,null),
      Rat(2,33.14,-66.1450005,Some(1113),Some(1665),today,null,null),
      Rat(3,33.14,-66.1450008,Some(1113),Some(1663),today,null,null),
      Rat(4,33.14,-66.14,Some(1113),Some(1670),today,null,null))

    val ds = spark.createDataset(rats_list).as[Rat]
    val tr = new TileRats()
    val removedRats = tr.getDeadOrRecoveredRats(ds,0.05,Some(seed))
    assert(removedRats.filter($"deadDate".isNotNull).count() == 1)
    assert(removedRats.filter($"recoveredDate".isNotNull).count() == 3)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh")
    val expectedDate = dateFormat.parse("2020-08-21 02")
    assert(dateFormat.format(removedRats.collect().apply(0).recoveredDate).equals(dateFormat.format(expectedDate)))
  }




}
