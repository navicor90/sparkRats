import java.sql.Timestamp

import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec

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
  

}
