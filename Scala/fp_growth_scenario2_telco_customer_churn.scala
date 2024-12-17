import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.collection.immutable.ListMap
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._



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


def calculate_KLD_between_discrete_and_expo = udf((itemset: mutable.Seq[Int]) => {

	val own_distribution = itemset.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / itemset.size)
	val own_distribution_sorted = ListMap(own_distribution.toSeq.sortWith(_._1 < _._1):_*)
	val average = itemset.sum/itemset.size.toDouble
	val lambda = 1/average
	val own_distribution_list = own_distribution_sorted.toList
	var KLD = 0.0
	for (p <- own_distribution_list) { 
		val temp = p._2 * (breeze.numerics.log(p._2) - breeze.numerics.log(lambda) + (lambda/2) * (2 * p._1 + 1))
		KLD = KLD + temp
	}
	KLD
})

def calculate_lambda = udf((itemset: mutable.Seq[Int]) => {

	val average = itemset.sum/itemset.size.toDouble
	val lambda = 1/average
	lambda
})

def calculate_KLD_between_expo = udf((p: Double, q: Double) => {
	breeze.numerics.log(p) - breeze.numerics.log(q) + (q/p) - 1
})

def mean_ = udf((itemset: Seq[Int]) => {
	itemset.sum.toDouble / itemset.size
})



val MINSUP = 0.05


var df = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/telco_customer_churn/telco_customer_churn.csv").cache()
df = df.withColumn("quant_variable", col("tenure"))
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

frequent_itemsets = frequent_itemsets.withColumn("quant_values_KLD", calculate_KLD_between_discrete_and_expo($"quant_values"))
frequent_itemsets = frequent_itemsets.filter($"quant_values_KLD" <= 0.3)
frequent_itemsets = frequent_itemsets.withColumn("counter_quant_values_KLD", calculate_KLD_between_discrete_and_expo($"counter_quant_values"))
frequent_itemsets = frequent_itemsets.filter($"counter_quant_values_KLD" <= 0.3)

frequent_itemsets = frequent_itemsets.withColumn("lambda_set", calculate_lambda($"quant_values"))
frequent_itemsets = frequent_itemsets.withColumn("lambda_counterSet", calculate_lambda($"counter_quant_values"))
frequent_itemsets = frequent_itemsets.withColumn("KLD_between_expo", calculate_KLD_between_expo($"lambda_counterSet", $"lambda_set"))

frequent_itemsets = frequent_itemsets.filter(size($"counter_quant_values") > 0 )
frequent_itemsets = frequent_itemsets.withColumn("mean", mean_($"quant_values"))


frequent_itemsets = frequent_itemsets.orderBy(desc("KLD_between_expo")).limit(10).cache
frequent_itemsets.select($"items", $"freq", $"KLD_between_expo", $"mean").show(false)




// rules saving
frequent_itemsets = frequent_itemsets.withColumn("mean", ceil($"mean"))
frequent_itemsets.select($"items", $"mean").limit(10).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(" ", "").replace("),", " $\\Rightarrow$ ").replace(","," $\\wedge$ ").replace("[","").replace("]", "")).saveAsTextFile("XXX/results/scenario2/telco_customer_churn/latex__tenure__" + MINSUP)


// distributions saving
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/telco_customer_churn/tenure__1__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/telco_customer_churn/tenure__1__" + MINSUP + "__counter_set")

frequent_itemsets = frequent_itemsets.withColumn("id", monotonically_increasing_id()).filter($"id" > 0).drop("id")
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/telco_customer_churn/tenure__2__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/telco_customer_churn/tenure__2__" + MINSUP + "__counter_set")
