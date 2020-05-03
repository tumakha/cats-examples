package example

import cats._
import cats.implicits._

trait MapReduce[I, O] {
  type R

  def reducer: Semigroup[R]

  def map: I => R

  def mapResult: R => O

  def apply(input: Seq[I]): O = mapResult(input.map(map).reduce(reducer.combine))
}

object MapReduce {
  def apply[I, O, _R](_reducer: Semigroup[_R], _map: I => _R, _mapResult: _R => O): MapReduce[I, O] =
    new MapReduce[I, O] {
      override type R = _R

      override def reducer = _reducer

      override def map = _map

      override def mapResult = _mapResult
    }

  def apply[I, O](map: I => O)(implicit r: Semigroup[O]): MapReduce[I, O] =
    MapReduce[I, O, O](r, map, identity)

  def apply[I, O](map: I => O, reduce: (O, O) => O): MapReduce[I, O] = {
    val reducer = new Semigroup[O] {
      override def combine(x: O, y: O): O = reduce(x, y)
    }
    MapReduce(map)(reducer)
  }

  implicit def mapReduceApply[I] =
    new Apply[({type F[X] = MapReduce[I, X]})#F] {
      override def map[A, B](f: MapReduce[I, A])(fn: A => B): MapReduce[I, B] =
        MapReduce(f.reducer, f.map, f.mapResult.andThen(fn))

      override def ap[A, B](ff: MapReduce[I, (A) => B])(fa: MapReduce[I, A]): MapReduce[I, B] =
        MapReduce(ff.reducer product fa.reducer,
          i => (ff.map(i), fa.map(i)),
          (t: (ff.R, fa.R)) => ff.mapResult(t._1)(fa.mapResult(t._2))
        )
    }

}

object MultiMapReduce extends App {

  val fruits: Seq[String] = Seq("apple", "banana", "cherry")

  def mapF(s: String): Char = s.head

  def reduceF(c1: Char, c2: Char): Char = if (c1 > c2) c1 else c2

  val biggestFirsChar = MapReduce(mapF, reduceF)
  val totalChars = MapReduce[String, Int](_.length) // (Semigroup[Int]) reduce by _ + _
  def count[A] = MapReduce[A, Int](_ => 1)

  val multiMapReduce = (biggestFirsChar, totalChars, count[String]).mapN((_, _, _))
  println(multiMapReduce(fruits))

  val sum = MapReduce[Double, Double](identity)
  val average = (sum, count[Double]).mapN(_ / _)
  println(sum(List(1, 2, 3, 4)))
  println(average(List(1, 2, 3, 4)))

}