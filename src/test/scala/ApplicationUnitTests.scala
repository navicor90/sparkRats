import java.sql.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import sparkApplication._

class ApplicationUnitTests extends AnyFlatSpec with SparkSessionTestWrapper {
  val today = new Timestamp((new java.util.Date()).getTime)
  import spark.implicits._

  it should "add meters correctly in getInfectedAreas" in {
    val rats_list = List(Rat(1,33.13,-66.14,null,null,null),
                         Rat(1,33.13,-66.14,today,null,null))
    import spark.implicits._
    val ds = spark.createDataset(rats_list).as[Rat]
    val areas = getInfectedAreas(ds,5)
    val expectedResult = Area(33.129955084235796,33.13004491576421,-66.14005363504815,-66.13994636489697)
    assert(areas.count()==1 && areas.first()==expectedResult)
  }

  it should "calculate correctly bound in getNewSick" in {
    val rats_list = List(Rat(1,33.14,-66.15,null,null,null),
                         Rat(2,33.139955084235790,-66.15,null,null,null),
                         Rat(3,33.14,-66.13994636489696,null,null,null))

    val areas_list = List(Area(33.139955084235796,33.14004491576421,-66.15005363504815,-66.14994636489697))

    val ratsDs = spark.createDataset(rats_list).as[Rat]
    val areasDs = spark.createDataset(areas_list).as[Area]
    val newSick = getNewSick(ratsDs,areasDs,1)
    val expectedResult = Rat(1,33.14,-66.15,null,null,null)
    assert(newSick.count()==1 && newSick.first()==expectedResult)
  }
}
