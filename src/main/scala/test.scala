import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config._
import java.util.Properties


object test {
  def main(args: Array[String]): Unit = {

    println("Hello Imran !!!!!")
//    val spark = SparkSession.builder().master("local[*]").appName("Sample").getOrCreate()
    val sparkSession = SparkSession.builder
        .master("yarn")
      .appName("DBDataLoad")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.compress.output", "true")
      .config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      .config("mapred.output.compression.type", "BLOCK")
      .config("parquet.compression", "SNAPPY")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val rdd1 = sc.parallelize(1 to 10)
    rdd1.collect().foreach(println)

    val prop = ConfigFactory.load()
    val plocal = prop.getConfig("local").getString("filepath")
    println("#########################prop.getString(\"local.filepath\")#########")
   println(plocal)

    prop.getString("userName")

    println(s"################################## prop.getConfig(local) is ${prop.getConfig("local")}")
    println(s"################################## prop.getString(userName)) is ${prop.getString("userName")}") // which gives direct value... Imran
      //################################## prop.getConfig(local) is Config(SimpleConfigObject({"filepath":"file:///storage/home/mx9uusr/test.txt"}))
       //    println(s"################################## prop.getConfig(local.filepath) is ${prop.getConfig("local.filepath")}") this shows error..
    val pathLocal = "file:///storage/home/mx9uusr/test.txt"
    val pathHdfs = "hdfs://devnameservice/acceptance/mx9/QA/test.txt"


    val fileLocal = sc.textFile(pathHdfs)
    val wordsLocal = fileLocal.flatMap(line=>line.split(" "))
//    wordsLocal.map(word => (word,1)).reduceByKey(_+_).foreach(println)
    wordsLocal.map(word => (word,word.length)).foreach(println)
    val l: (String, Int) = wordsLocal.map(word => (word,word.length)).reduce((x, y)=>if(x._2>y._2) x else y)
//    readme.map(line => (line,line.split(" ").size)).reduce((a,b) => if (a._2 > b._2) a else b)
    println(s"===============lenghiest word is ================ $l")
//    wordsLocal.foreach(println)
    //wordsLocal.map(line => (line,line.split(" ").size)).reduce((a,b) => if (a._2 > b._2) a else b)
    val fileHdfs = sc.textFile(pathHdfs)
    val wordsHdfs = fileLocal.flatMap(line=>line.split(" "))



    println("**********************Reading SFTP FIles ****************")
      val hiveContext = sparkSession.sqlContext

      hiveContext.sql("show databases").show()
      /* val df = sparkSession.read.format("com.springml.spark.sftp").option("host","secureftp.sdihealth.com")
        .option("username","T1269").option("password","WAIdSY0v").option("fileType","csv").option("inferSchema","true").option("header", "true").load("/In/NON_RETAIL_SUMMARY_2013M05.csv")
*/
//    df.show()


   /* option("host", "SFTP_HOST").
      option("username", "SFTP_USER").
      option("password", "****").
      option("fileType", "csv").
      option("inferSchema", "true").
      load("/ftp/files/sample.csv")*/

    import sparkSession.implicits._
   /* val table_df: DataFrame =  sparkSession.read.
      format("com.springml.spark.sftp").
      option("host", "secureftp.sdihealth.com").
      option("username", "T1269").
      option("password","WAIdSY0v").
      option("header", "true").
      option("codec", "zip").
      option("fileType", "csv").
      option("inferSchema", "true").
      load("/In/NON_RETAIL_SUMMARY_2013M05.csv")
    println(table_df.count())*/

      /*val hiveConnection: DataFrame = sparkSession.read .format("jdbc").
      option("url", "jdbc:hive2://cdts10hdbe01d.rxcorp.com:10000/default").
      option("dbtable", "abc") .
      option("user", "mx9uusr") .
      option("password", "Mx9uacc1") .option("driver", "org.apache.hadoop.hive.jdbc.HiveDriver") .load()


    println(hiveConnection.count())*/

//    jdbc:hive2://localhost:10000

    println("Oracle connecting .....")

/*   val oracleConnection: DataFrame = sparkSession.read .format("jdbc").
//      option("url", "jdbc:oracle:thin:/@//DB-ROKMDM01:1521/ROKMDM01.cegedim.com").
     option("url", "jdbc:oracle:thin:/@//DB-ROKMDM01:1521").
      option("dbtable", "EBX_OWNER.OK_F13_CODES") .
      option("user", "EBX_RO") .
      option("password", "Welcome1#") .option("driver", "oracle.jdbc.driver.OracleDriver") .load()

    oracleConnection.show(5)*/
val connectionprop = new Properties()
//    connectionprop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    connectionprop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    println("===========last statement============")
   /* val oracleConnection: DataFrame = sparkSession.read.format("jdbc").
      option("url", "jdbc:oracle:thin:/@//162.44.114.73/UONM").
      option("dbtable", "ONMDBAUACC.ONM_DIM_PROVIDER_HIST") .

     option("driver", "oracle.jdbc.driver.OracleDriver") .load()
    oracleConnection.show(5)*/


//    sparkSession.read.jdbc("jdbc:oracle:thin:/@//162.44.114.73/UONM","ONMDBAUACC.ONM_DIM_PROVIDER_HIST",connectionprop)


//    val connectionUrl = "jdbc:sqlserver://<server_ip_or_url>;databaseName=<sqlserver_database_name>;user=<username>;password=<password>;"

    val df = sparkSession.read.jdbc("jdbc:sqlserver://SYDSSQL151T:1433;database=UACC_DF5_AU9;user=au9uusr;password=aklfg8*(75d","VW_AU9_DIMASSET_PERIOD",connectionprop)
df.show(5)

  }

}
