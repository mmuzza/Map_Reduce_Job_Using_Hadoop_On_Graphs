import org.scalatest.{FunSuite, Matchers}
import com.lsc.nodesMapReduce // Import your nodesMapReduce object

object Test extends FunSuite with Matchers {

  test("calculateSimRank returns the expected score for equal input values") {
    val csvLine = "1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0"
    val score = nodesMapReduce.calculateSimRank(csvLine)
    score shouldEqual 1.0 // Expected score for equal input values
    info("Test passed: calculateSimRank returns the expected score for equal input values")
  }

  test("calculateSimRank returns the expected score for slightly different input values") {
    val csvLine = "1.0,2.1,3.0,4.0,5.0,6.0,7.0,8.0,9.0,1.0,2.1,3.0,4.0,5.0,6.0,7.0,8.0,9.0"
    val score = nodesMapReduce.calculateSimRank(csvLine)
    score shouldEqual 0.92 // Expected score for slightly different input values
    info("Test passed: calculateSimRank returns the expected score for slightly different input values")
  }

  // Add more test cases as needed to cover different scenarios

}