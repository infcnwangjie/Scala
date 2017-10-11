package inspur

import org.apache.spark.sql.SparkSession
import java.lang.StringBuilder

/**
  * Created by zhouyongjin on 2017/8/14.
  */
object TstSparkHive {
  private val TABLE_NAME = "tb_spark_hive"

  case class Record(no: Int, name: String)

  case class Record1(no: Int, c2: String, c3: String, c4: String, c5: String, c6: String, c7: String, c8: String, c9: String,
                     c10: String, c11: String, c12: String, c13: String, c14: String, c15: String, c16: String, c17: String,
                     c18: String, c19: String, c20: String, c21: String, c22: String, c23: String, c24: String, c25: String,
                     c26: String, c27: String, c28: String, c29: String, c30: String, c31: String, c32: String, c33: String,
                     c34: String, c35: String, c36: String, c37: String, c38: String, c39: String, c40: String, c41: String,
                     c42: String, c43: String, c44: String, c45: String, c46: String, c47: String, c48: String, c49: String,
                     c50: String, c51: String, c52: String, c53: String, c54: String, c55: String, c56: String, c57: String,
                     c58: String, c59: String, c60: String, c61: String, c62: String, c63: String, c64: String, c65: String,
                     c66: String, c67: String, c68: String, c69: String, c70: String, c71: String, c72: String, c73: String,
                     c74: String, c75: String, c76: String, c77: String, c78: String, c79: String, c80: String, c81: String,
                     c82: String, c83: String, c84: String, c85: String, c86: String, c87: String, c88: String, c89: String,
                     c90: String, c91: String, c92: String, c93: String, c94: String, c95: String, c96: String, c97: String,
                     c98: String, c99: String, c100: String, c101: String, c102: String, c103: String, c104: String, c105: String,
                     c106: String, c107: String, c108: String, c109: String, c110: String, c111: String, c112: String, c113: String,
                     c114: String, c115: String, c116: String, c117: String, c118: String, c119: String, c120: String, c121: String,
                     c122: String, c123: String, c124: String, c125: String, c126: String, c127: String, c128: String, c129: String,
                     c130: String, c131: String, c132: String, c133: String, c134: String, c135: String, c136: String, c137: String,
                     c138: String, c139: String, c140: String, c141: String, c142: String, c143: String, c144: String, c145: String,
                     c146: String, c147: String, c148: String, c149: String, c150: String, c151: String, c152: String, c153: String,
                     c154: String, c155: String, c156: String, c157: String, c158: String, c159: String, c160: String, c161: String,
                     c162: String, c163: String, c164: String, c165: String, c166: String, c167: String, c168: String, c169: String,
                     c170: String, c171: String, c172: String, c173: String, c174: String, c175: String, c176: String, c177: String,
                     c178: String, c179: String, c180: String, c181: String, c182: String, c183: String, c184: String, c185: String,
                     c186: String, c187: String, c188: String, c189: String, c190: String, c191: String,
                     c192: String, c193: String, c194: String, c195: String, c196: String, c197: String, c198: String, c199: String)


  def testSql(): Unit = {
    val spark = SparkSession
      .builder()
      // .master("local")
      .appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("USE default").show

    val recordsDF = spark.createDataFrame((1 to 10).map(i => Record(i, s"name_$i")))
    /**
      * 向hive写数据
      * 1、sql
      * 2、insertInto()
      */
    recordsDF.createOrReplaceTempView("records")

    spark.sql(s"INSERT INTO ${TABLE_NAME} SELECT * FROM records")
    recordsDF.write.insertInto(TABLE_NAME)

    /**
      * 从hive表中读
      * 1、sql
      * 2、table
      */
    spark.sql(s"SELECT * FROM ${TABLE_NAME}").show

    spark.read.table(TABLE_NAME)
      .filter(s"${TableStruct.TB_SPARK_HIVE.NUMBER} = 10")
      .select(TableStruct.TB_SPARK_HIVE.NAME)
      .show
  }

  def insertIntoSql(): Unit = {
    val spark = SparkSession
      .builder()
      // .master("local")
      .appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("USE default").show


    val recordsDF = spark.createDataFrame((1 until 10000).map(i => Record(i, s"name_$i")))
    /**
      * 向hive写数据
      * 1、sql
      * 2、insertInto()
      */
    recordsDF.createOrReplaceTempView("records")

    recordsDF.write.insertInto(TABLE_NAME)


  }

  def test(): Unit = {
    var stb = new StringBuilder()
    stb.append("(");
    for (i <- 1 to 200) {
      if (i == 1)
        stb.append("no :Int,")
      else if (i == 200) {
        stb.append("c199:String")
      } else {
        stb.append(s"c${i}:String,")
      }
    }
    stb.append(")")

    println(stb.toString)
  }

  def main(args: Array[String]): Unit = {
    var start = System.currentTimeMillis();
    //insertIntoSql
    test
    var end = System.currentTimeMillis()
    println("cost time:" + (end - start))
  }
}
