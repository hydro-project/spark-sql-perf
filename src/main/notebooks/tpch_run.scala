// Databricks notebook source
// TPCH runner (from spark-sql-perf) to be used on existing tables
// edit the main configuration below

//val scaleFactors = Seq(1, 10, 100, 1000) //set scale factors to run
val scaleFactors = Seq(1) //set scale factors to run
val format = "parquet" //format has have already been generated
val baseLocation = s"/opt/spark/apps/data"

def perfDatasetsLocation(scaleFactor: Int, format: String) = 
  s"${baseLocation}/tpch/sf${scaleFactor}_${format}"
  //s"s3a://my-bucket/tpch/sf${scaleFactor}_${format}"

//val resultLocation = "s3a://my-bucket/results"
val resultLocation = s"${baseLocation}/results"
val iterations = 1
def databaseName(scaleFactor: Int, format: String) = s"tpch_sf${scaleFactor}_${format}"
val randomizeQueries = false //to use on concurrency tests

// Experiment metadata for results, edit if outside Databricks
val configuration = "default" //use default when using the out-of-box config
val runtype = "TPCH run" // Edit
val workers = 1 //10 // Edit to the number of worker
val workerInstanceType = "my_VM_instance" // Edit to the instance type

// Make sure spark-sql-perf library is available (use the assembly version)
import com.databricks.spark.sql.perf.tpch._
import org.apache.spark.sql.functions._

// default config (for all notebooks)
var config : Map[String, String] = Map (
  "spark.sql.broadcastTimeout" -> "7200", // Enable for SF 10,000
  "spark.sql.adaptive.enabled" -> "false", // Ignore adaptive execution for plain physical plan
  //"spark.sql.codegen" -> "false",
  //"spark.sql.codegen.wholeStage" -> "false",
)
// Set the spark config
for ((k, v) <- config) spark.conf.set(k, v)
// Print the custom configs first
for ((k,v) <- config) println(k, spark.conf.get(k))
// Print all for easy debugging
print(spark.conf.getAll)

val tpch = new TPCH(sqlContext = spark.sqlContext)

// filter queries (if selected)
import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import org.apache.commons.io.IOUtils

/* val query_no_to_run = (1 to 1) //(1 to 22)
val queries = query_no_to_run.map { q =>
  val queryContent: String = IOUtils.toString(
    getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
  new Query(s"Q$q", spark.sqlContext.sql(queryContent), description = s"TPCH Query $q",
    executionMode = CollectResults)
}
 */
val scaleFactor = 1
// .repartition(1)
sql(s"USE ${databaseName(scaleFactor, format)}")

val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
val forders = spark.table("orders").filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")
val nation = spark.table("nation")
val supplier = spark.table("supplier")
val lineitem = spark.table("lineitem")
val customer = spark.table("customer")
val my_df = spark.table("region").filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
val q5_original = new Query(name="Q5 orig", my_df, description = "Q5 orig",
    executionMode = CollectResults)

val my_df_single_base_partition = spark.table("region").repartition(1).filter($"r_name" === "ASIA")
      .join(nation.repartition(1), $"r_regionkey" === nation("n_regionkey"))
      .join(supplier.repartition(1), $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem.repartition(1), $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders.repartition(1), $"l_orderkey" === forders("o_orderkey"))
      .join(customer.repartition(1), $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)

val q5_single_base = new Query(name="Q5 1 base", my_df_single_base_partition, description = "Q5 1 base",
    executionMode = CollectResults)

val queries = Seq(q5_original, q5_single_base)

// COMMAND ----------

val experiment = tpch.runExperiment(
   queries,
   iterations = iterations,
   resultLocation = resultLocation,
   tags = Map(
   "runtype" -> runtype,
   "date" -> java.time.LocalDate.now.toString,
   "database" -> databaseName(scaleFactor, format),
   "scale_factor" -> scaleFactor.toString,
   "spark_version" -> spark.version,
   "system" -> "Spark",
   "workers" -> workers.toString,
   "workerInstanceType" -> workerInstanceType,
   "configuration" -> configuration
   ),
   includeBreakdown = false
  )
  println(s"Running SF $scaleFactor")
  experiment.waitForFinish(36 * 60 * 60) //36hours
  val summary = experiment.getCurrentResults
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)
  summary.show(9999, false)

/* scaleFactors.foreach{ scaleFactor =>
  println("DB SF " + databaseName(scaleFactor, format))
  sql(s"USE ${databaseName(scaleFactor, format)}")
  val experiment = tpch.runExperiment(
   queries,
   iterations = iterations,
   resultLocation = resultLocation,
   tags = Map(
   "runtype" -> runtype,
   "date" -> java.time.LocalDate.now.toString,
   "database" -> databaseName(scaleFactor, format),
   "scale_factor" -> scaleFactor.toString,
   "spark_version" -> spark.version,
   "system" -> "Spark",
   "workers" -> workers.toString,
   "workerInstanceType" -> workerInstanceType,
   "configuration" -> configuration
   ),
   includeBreakdown = false
  )
  println(s"Running SF $scaleFactor")
  experiment.waitForFinish(36 * 60 * 60) //36hours
  val summary = experiment.getCurrentResults
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)
  summary.show(9999, false)
} */