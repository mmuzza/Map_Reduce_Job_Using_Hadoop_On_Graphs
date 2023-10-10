//import cats.implicits.catsSyntaxEq
//import com.lsc.edgesMapReduce
//import com.lsc.SimRankMapReduce
//import org.slf4j.LoggerFactory
////import com.lsc.sharding
//import org.scalatest.funsuite.AnyFunSuite // Use AnyFunSuite from the funsuite package
//
//class generateFile extends AnyFunSuite { // Extend AnyFunSuite
//
//  private val logger = LoggerFactory.getLogger(this.getClass)
//
////  test("Test simRank function with sample input") {
////    val csvLine = "sample input line" // Replace with your sample input
////    val result = edgesMapReduce.simRank(csvLine)
////    assert(result === 0.0) // Replace with your expected result
////  }
//
//  test("Test simRank function for Nodes"){
//
//    val line = "1,4,11,1,57,0,1,8,0.287921434533922,1,4,11,1,57,0,1,8,0.287921434533922"
//    val score = SimRankMapReduce.calculateSimRank(line)
//    assert(score === 0.9)
//
//    logger.info("Test passed successfully!")
//  }
//
//}'

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