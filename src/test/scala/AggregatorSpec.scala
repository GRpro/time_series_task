import java.time.LocalDate

import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ListBuffer

class AggregatorSpec extends WordSpec with Matchers {

  "deserialize -> serialize" should {

    "result in the same value" in {
      val str = "2018-01-01:3"
      val deserialized = Aggregator.deserialize(str)
      val serialized = Aggregator.serialize(deserialized)
      serialized shouldBe str
    }
  }

  "merge" should {
    "correctly merge records" in {
      val record1 = Aggregator.deserialize("2018-01-01:3")
      val record2 = Aggregator.deserialize("2018-01-01:5")
      val records = Iterable(record1, record2)
      val result = Aggregator.merge(records)
      val expected = Record(LocalDate.parse("2018-01-01"), 8)
      result shouldBe expected
    }
  }

  "stringToDate -> dateToString" should {

    "result in the same value" in {
      val str = "2018-01-01"
      val date = Aggregator.stringToDate(str)
      val str1 = Aggregator.dateToString(date)
      str1 shouldBe str
    }
  }

  "nextRecord" should {
    "ignore record of invalid format" in {
      val it = Iterator(
        "invalid",
        "invalid",
        "2018-01-07:1"
      )
      val record = Aggregator.nextRecord(it)
      record shouldBe Some(Record(LocalDate.parse("2018-01-07"), 1))
    }
  }

  "processRecords" should {

    "correctly aggregate results" in {
      val records = Array(
        Iterator(
          "2018-01-01:1",
          "2018-01-05:1",
          "2018-01-06:1"
        ),
        Iterator(
          "not-valid-record", // should ignore this
          "2018-01-02:1",
          "2018-01-05:1",
          "2018-01-07:1"
        ),
        Iterator(
          "2018-01-01:1",
          "2018-01-02:1",
          "2018-01-03:1"
        ),
        Iterator(
          "2018-01-03:2",
          "not-valid-record", // should ignore this
          "2018-01-05:1",
          "2018-01-06:1"
        ),
        Iterator(
          "2018-01-07:1",
          "not-valid-record", // should ignore this
          "2018-01-08:1",
          "2018-01-10:1"

        ),
        Iterator(
          "2018-01-01:1",
          "2018-01-05:1",
          "2018-01-06:1"
        ),
        Iterator(
          "2018-01-04:1",
          "2018-01-05:1",
          "2018-01-06:1"
        )
      )

      val res = ListBuffer.empty[String]
      Aggregator.processRecords(res += _)(records)
      assert(res ===
        ListBuffer(
          "2018-01-01:3",
          "2018-01-02:2",
          "2018-01-03:3",
          "2018-01-04:1",
          "2018-01-05:5",
          "2018-01-06:4",
          "2018-01-07:2",
          "2018-01-08:1",
          "2018-01-10:1"
        )
      )
    }
  }
}
