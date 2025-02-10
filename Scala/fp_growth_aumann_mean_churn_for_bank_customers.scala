import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.collection.immutable.ListMap
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.types.IntegerType
import scala.math._


def getCandidatesUDF = udf((itemset: mutable.Seq[String]) => {
	
	var candidatesList = new ListBuffer[String]()
	for(item <- itemset){

        if((item.contains("_"))){
        		candidatesList += item	
        }
		
	}
	candidatesList
})


def get_quant_values_UDF(input_Data_Array: Array[(Seq[String],Int)]) = udf( (variables: Seq[String]) => {

	var quant_values = new ListBuffer[Int]()
	for(input_Data <- input_Data_Array){
		val variables_are_inside = variables.forall(input_Data._1.contains)
		if(variables_are_inside){
			quant_values += input_Data._2
		}	
	}
	quant_values
})

def get_counter_quant_values_UDF(input_Data_Array: Array[(Seq[String],Int)]) = udf( (variables: Seq[String]) => {

	var quant_values = new ListBuffer[Int]()
	for(input_Data <- input_Data_Array){
		val variables_are_inside = variables.forall(input_Data._1.contains)
		if(!(variables_are_inside)){
			quant_values += input_Data._2
		}	
	}
	quant_values
})


def mean(itemset: Seq[Int]): Double = itemset.sum.toDouble / itemset.size

def get_variance(itemset: Seq[Int]): Double = {
  val mean = itemset.sum.toDouble / itemset.size
  val squaredDiffs = itemset.map(x => math.pow(x - mean, 2))
  squaredDiffs.sum / itemset.size
}


def calculate_difference_in_summary_statistics = udf((quant_values: Seq[Int], counter_quant_values: Seq[Int]) => {

 	val mean_quant_values = mean(quant_values)
 	val mean_counter_quant_values = mean(counter_quant_values)

	val variance_quant_values = get_variance(quant_values)
	val variance_counter_quant_values = get_variance(counter_quant_values)
	val z = ((mean_quant_values - mean_counter_quant_values)) / sqrt((variance_quant_values/quant_values.length) + ((variance_counter_quant_values/counter_quant_values.length)))


//	if(z < -1.96 || z > 1.96){
//		-100
//	}else{
//		mean_quant_values - mean_counter_quant_values
//	}

	mean_quant_values - mean_counter_quant_values

})

def mean_ = udf((itemset: Seq[Int]) => {
	itemset.sum.toDouble / itemset.size
})



val MINSUP = 0.05


var df = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/churn_for_bank_customers/churn_for_bank_customers.csv").cache()
df = df.withColumn("quant_variable", col("CreditScore"))
df = df.withColumn("temp", array("*"))
df = df.withColumn("variables", getCandidatesUDF($"temp"))
df = df.select($"variables", $"quant_variable").cache


val fpgrowth = new FPGrowth().setItemsCol("variables").setMinSupport(MINSUP)
val model = fpgrowth.fit(df)
var frequent_itemsets = model.freqItemsets
frequent_itemsets.show()

val df_set = df.select("variables", "quant_variable").rdd.map(row => (row(0).asInstanceOf[Seq[String]], row(1).asInstanceOf[Int])).collect
val inputDataDFHashmapBC = sc.broadcast(df_set)


frequent_itemsets = frequent_itemsets.withColumn("quant_values", get_quant_values_UDF(inputDataDFHashmapBC.value)($"items"))
frequent_itemsets = frequent_itemsets.withColumn("counter_quant_values", get_counter_quant_values_UDF(inputDataDFHashmapBC.value)($"items"))

frequent_itemsets = frequent_itemsets.withColumn("difference_in_summary_statistics", calculate_difference_in_summary_statistics($"quant_values", $"counter_quant_values"))

frequent_itemsets = frequent_itemsets.filter(size($"counter_quant_values") > 0 )
frequent_itemsets = frequent_itemsets.withColumn("mean", mean_($"quant_values"))


frequent_itemsets = frequent_itemsets.orderBy(desc("difference_in_summary_statistics")).limit(10).cache
frequent_itemsets.select($"items", $"freq", $"difference_in_summary_statistics", $"mean").show(false)


// rules saving
frequent_itemsets = frequent_itemsets.withColumn("mean", ceil($"mean"))
frequent_itemsets.select($"items", $"mean").limit(10).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(" ", "").replace("),", " $\\Rightarrow$ ").replace(","," $\\wedge$ ").replace("[","").replace("]", "")).saveAsTextFile("XXX/results/aumann/churn_for_bank_customers/latex__score__" + MINSUP)


// distributions saving
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/aumann/churn_for_bank_customers/aumann__score__1__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/aumann/churn_for_bank_customers/aumann__score__1__" + MINSUP + "__counter_set")

frequent_itemsets = frequent_itemsets.withColumn("id", monotonically_increasing_id()).filter($"id" > 0).drop("id")
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/aumann/churn_for_bank_customers/aumann__score__2__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/aumann/churn_for_bank_customers/aumann__score__2__" + MINSUP + "__counter_set")
