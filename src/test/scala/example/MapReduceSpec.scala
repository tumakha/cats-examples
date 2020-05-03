package example

import cats.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * @author Yuriy Tumakha
 */
class MapReduceSpec extends AnyFlatSpec with Matchers {

  "MapReduce" should "apply multiple MapReduce in single pass" in {
    def mapF(s: String): Char = s.head

    def reduceF(c1: Char, c2: Char): Char = if (c1 > c2) c1 else c2

    def count[A] = MapReduce[A, Int](_ => 1)

    val biggestFirsChar = MapReduce(mapF, reduceF)
    val totalChars = MapReduce[String, Int](_.length)
    val sum = MapReduce[Double, Double](identity)
    val average = (sum, count[Double]).mapN(_ / _)
    val multiMapReduce = (biggestFirsChar, totalChars, count[String]).mapN((_, _, _))

    val fruits: Seq[String] = Seq("apple", "banana", "cherry")
    multiMapReduce(fruits) shouldBe ('c', 17, 3)

    val numbers: List[Double] = List(1, 2, 3, 4)
    sum(numbers) shouldBe 10
    average(numbers) shouldBe 2.5
  }

}
