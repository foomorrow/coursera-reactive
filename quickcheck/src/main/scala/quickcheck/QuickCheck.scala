package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  lazy val genHeap: Gen[H] =  arbitrary[Int].flatMap(v => oneOf(const(empty),genHeap).map(insert(v,_)))

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }
  lazy val genMap: Gen[Map[Int,Int]] = for {
    k <- arbitrary[Int]
    v <- arbitrary[Int]
    m <- oneOf(const(Map.empty[Int,Int]), genMap)
  } yield m.updated(k, v)

  property("gen2") = forAll { (x: Int,y: Int) =>
    val xy = insert(y,insert(x,empty))
    val yx = insert(x,insert(y,empty))
    if(x > y){
      findMin(yx) == y && findMin(xy) == y
    }else{
      findMin(yx) == x && findMin(xy) == x
    }
  }
  property("delete") = forAll( (a: Int) => {
    isEmpty(deleteMin(insert(a, empty)))
  })

  property("sort and check list") = forAll((numList: List[Int]) => {
    def validate(heap: H, source: List[Int]): Boolean = {
      if(isEmpty(heap)) source.isEmpty
      else !source.isEmpty && findMin(heap) == source.head && validate(deleteMin(heap), source.tail)
    }
    validate(numList.foldLeft(empty)((acc, a) => insert(a, acc)), numList.sorted)
  })
}
