package calculator

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._

import TweetLength.MaxTweetLength

@RunWith(classOf[JUnitRunner])
class CalculatorSuite extends FunSuite with ShouldMatchers {

  /******************
   ** TWEET LENGTH **
   ******************/

  def tweetLength(text: String): Int =
    text.codePointCount(0, text.length)

  test("tweetRemainingCharsCount with a constant signal") {
    val result = TweetLength.tweetRemainingCharsCount(Var("hello world"))
    assert(result() == MaxTweetLength - tweetLength("hello world"))

    val tooLong = "foo" * 200
    val result2 = TweetLength.tweetRemainingCharsCount(Var(tooLong))
    assert(result2() == MaxTweetLength - tweetLength(tooLong))
  }

  test("tweetRemainingCharsCount with a supplementary char") {
    val result = TweetLength.tweetRemainingCharsCount(Var("foo blabla \uD83D\uDCA9 bar"))
    assert(result() == MaxTweetLength - tweetLength("foo blabla \uD83D\uDCA9 bar"))
  }


  test("colorForRemainingCharsCount with a constant signal") {
    val resultGreen1 = TweetLength.colorForRemainingCharsCount(Var(52))
    assert(resultGreen1() == "green")
    val resultGreen2 = TweetLength.colorForRemainingCharsCount(Var(15))
    assert(resultGreen2() == "green")

    val resultOrange1 = TweetLength.colorForRemainingCharsCount(Var(12))
    assert(resultOrange1() == "orange")
    val resultOrange2 = TweetLength.colorForRemainingCharsCount(Var(0))
    assert(resultOrange2() == "orange")

    val resultRed1 = TweetLength.colorForRemainingCharsCount(Var(-1))
    assert(resultRed1() == "red")
    val resultRed2 = TweetLength.colorForRemainingCharsCount(Var(-5))
    assert(resultRed2() == "red")
  }
  /** Polynomial **/
  test("x^2+2x+1,x should be -1"){
    val a = Var(1.0)
    val b = Var(2.0)
    val c = Var(1.0)
    val delta = Polynomial.computeDelta(a,b,c)
    assert(0 == delta())
    val result = Polynomial.computeSolutions(a,b,c,delta)
    assert(Set(-1).equals(result()))
  }
  test("x^2+3x,x should be 0"){
    val a = Var(1.0)
    val b = Var(3.0)
    val c = Var(0.0)
    val delta = Polynomial.computeDelta(a,b,c)
    assert(9 == delta())
    val result = Polynomial.computeSolutions(a,b,c,delta)
    assert(Set(0,-3).equals(result()))
  }
  test("7x^2+9x+2,x should be -2/7 -14"){
    val a = Var(7.0)
    val b = Var(9.0)
    val c = Var(2.0)
    val delta = Polynomial.computeDelta(a,b,c)
    assert(25 == delta())
    val result = Polynomial.computeSolutions(a,b,c,delta)
    assert(Set(-1*2.0/7,-1).equals(result()))
  }
  /** Calculator **/
  test("a = 1") {
    val result = Calculator.computeValues(
      Map(
      "a" -> Var(Literal(1))
      )
    ).getOrElse("a",NoSignal())
    assert(result() == 1)
  }
  test("a = 1 + 2, b = a + 6") {
    val result = Calculator.computeValues(
      Map(
        "a" -> Var(Plus(Literal(1),Literal(2))),
        "b" -> Var(Plus(Ref("a"),Literal(6)))
      )
    ).getOrElse("b",NoSignal())
    assert(result() == 9)
  }
  test("a = b, b = 1 ") {
    val result = Calculator.computeValues(
      Map(
        "a" -> Var(Ref("b")),
        "b" -> Var(Literal(1))
      )
    ).getOrElse("a",NoSignal())
    assert(result() == 1)
  }
  test("a = a, b = a ") {
    val result = Calculator.computeValues(
      Map(
        "a" -> Var(Ref("a")),
        "b" -> Var(Ref("b"))
      )
    )
    val resultA = result.getOrElse("a",NoSignal())
    val resultB = result.getOrElse("b",NoSignal())
    assert(Double.NaN.equals(resultA()))
    assert(Double.NaN.equals(resultB()))
  }
  test("a = b + 1, b = a + 2") {
    val result = Calculator.computeValues(
      Map(
        "a" -> Var(Plus(Ref("b"),Literal(1))),
        "b" -> Var(Plus(Ref("a"),Literal(2)))
      )
    )
    val resultA = result.getOrElse("a",NoSignal())
    val resultB = result.getOrElse("b",NoSignal())
    assert(Double.NaN.equals(resultA()))
    assert(Double.NaN.equals(resultB()))
  }
}
