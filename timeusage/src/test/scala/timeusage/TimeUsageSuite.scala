package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("column classification") {
    val columnNames = List("t0201", "t180101", "t180201", "t180501", "t180301", "t0601")

    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columnNames)

    println(primaryNeedsColumns)
    println(workColumns)
    println(otherColumns)
  }

//  Set()
//  Set(t15457, t04261, t16237, t18149, t07124, t09245, t0771, t07657, t16157, t14817, t18849, t0720, t08359, t15181, t09149, t02346, t18420, t18282, t14834, t06744, t07355, t09987, t15136, t16404, t04943, t1698, t07672, t10598, t16896, t16516, t0718, t1565, t18759, t04674, t18903, t09808, t099, t12949, t04381, t14448, t04249, t09818, t04175, t18868, t16499, t15871, t09278, t07280, t18706, t07929, t13971, t14853, t16567, t09339, t04551, t10412)
//
//  Set()
//  Set(t0201, t0601, t180201)

}
