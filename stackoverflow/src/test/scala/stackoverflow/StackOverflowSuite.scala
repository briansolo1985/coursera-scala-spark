package stackoverflow

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("cluster") {
    val vectors = StackOverflow.sc.parallelize(List((550000, 13), (200000, 12), (50000, 16)))
    val means = Array((125000, 14), (550000, 13))
    val results = testObject.clusterResults(means, vectors)
    testObject.printResults(results)
    assert (results.contains("Haskell", 100.0, 1, 13))
    assert (results.contains("C#", 50.0, 2, 14))
  }

  test("vectorPostings") {
    val input = StackOverflow.sc.parallelize(
      List(
        (Posting(1, 1, None, None, 0, Some("Java")), 14),
        (Posting(1, 1, None, None, 0, None), 5),
        (Posting(1, 1, None, None, 0, Some("Scala")), 25),
        (Posting(1, 1, None, None, 0, Some("JavaScript")), 3)
      )
    )

    val expected = Array((50000, 14), (500000, 25), (0, 3))

    val output = testObject.vectorPostings(input).collect

    assert( output.sameElements(expected) )
  }

}
