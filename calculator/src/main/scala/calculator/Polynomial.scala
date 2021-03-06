package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Var{
      math.pow(b(),2)  - 4 * a() * c()
    }
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Var {
      if(delta()>=0) Set((-1*b()+math.sqrt(delta()))/(2*a()),(-1*b()-math.sqrt(delta()))/(2*a()))
      else Set()
    }
  }
}
