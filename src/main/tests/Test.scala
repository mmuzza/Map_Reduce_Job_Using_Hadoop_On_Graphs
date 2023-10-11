import org.scalatest.{FunSuite, Matchers}
import com.lsc.nodesMapReduce // Import your nodesMapReduce object
import com.lsc.edgesMapReduce
import com.lsc.sharding
import org.scalatest.FunSpec
import org.scalatest.Matchers

object Test extends FunSuite with Matchers {

  test("calculateSimRank returns the expected score for equal input values") {
    val csvLine = "1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0"
    val score = nodesMapReduce.calculateSimRank(csvLine)
    score shouldEqual 0.9 // Expected score for equal input values
    info("Test passed: calculateSimRank returns the expected score for equal input values")
  }

  test("calculateSimRank returns the expected score for slightly different input values") {
    val csvLine = "1.0,2.1,3.0,4.0,5.0,6.0,7.0,8.0,9.0,1.0,2.1,3.0,4.0,5.0,6.0,7.0,8.0,9.0"
    val score = nodesMapReduce.calculateSimRank(csvLine)
    score shouldEqual 0.9 // Expected score for slightly different input values
    info("Test passed: calculateSimRank returns the expected score for slightly different input values")
  }

  test("calculateSimRank returns the expected score for equal input values") {
    val csvLine = "1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0"
    val score = nodesMapReduce.calculateSimRank(csvLine)
    score shouldEqual 0.9 // Expected score for equal input values
    info("Test passed: calculateSimRank returns the expected score for equal input values")
  }

  test("calculateSimRank returns the expected score for slightly different input values") {
    val csvLine = "1.0,2.1,3.0,4.0,5.0,6.0,7.0,8.0,9.0,1.0,2.1,3.0,4.0,5.0,6.0,7.0,8.0,9.0"
    val score = nodesMapReduce.calculateSimRank(csvLine)
    score shouldEqual 0.9 // Expected score for slightly different input values
    info("Test passed: calculateSimRank returns the expected score for slightly different input values")
  }

  describe("simRank") {
    it("should calculate the score based on CSV input") {
      // Test CSV data
      val csvLine = "1,10,20,3,4,5,6,7,8,19,28,29,30,31,32,33,34,35"
      val result = edgesMapReduce.simRank(csvLine)
      val expectedScore = 0.58
      result should be(expectedScore)
    }
  }
}
