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


def get_variance(numbers: Seq[Int]): Double = {
  val mean = numbers.sum/numbers.size.toDouble
  val squaredDiffs = numbers.map(x => math.pow(x - mean, 2))
  squaredDiffs.sum / (numbers.size-1)
}


def calculate_KLD_between_discrete_and_normal = udf((itemset: mutable.Seq[Int]) => {

	val own_distribution = itemset.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / itemset.size)
	val own_distribution_sorted = ListMap(own_distribution.toSeq.sortWith(_._1 < _._1):_*)
	val average = itemset.sum/itemset.size.toDouble
	val lambda = 1/average
	val own_distribution_list = own_distribution_sorted.toList
	var KLD = 0.0
	val mean = itemset.sum.toDouble / itemset.size
	val variance = get_variance(itemset)
	var i = 0
	for (p <- own_distribution_list) { 
		if(own_distribution_list.size-1 > i){
			val temp = p._2 *(breeze.numerics.log(p._2)-(breeze.numerics.log(1/(sqrt(variance)*sqrt(2*Pi)))-0.5*((math.pow(-mean+own_distribution_list(i+1)._1, 3) - math.pow(-mean+p._1, 3))/(3*variance))))
			KLD = KLD + temp
			i += 1
		}
	}
	KLD
})


def calculate_KLD_between_normal = udf((p: Seq[Int], q: Seq[Int]) => {

	val mean_p = p.sum/p.size.toDouble
	val variance_p = get_variance(p)

	val mean_q = q.sum/q.size.toDouble
	val variance_q = get_variance(q)

	//0.5*((math.pow(mean_q-mean_p, 2)/math.pow(variance_q, 2)) + (math.pow(variance_p, 2)/math.pow(variance_q, 2)) - (breeze.numerics.log(math.pow(variance_p, 2)/math.pow(variance_q, 2))) - 1)
	0.5*((math.pow(mean_p-mean_q, 2)/math.pow(variance_q, 2)) + (math.pow(variance_p, 2)/math.pow(variance_q, 2)) + (breeze.numerics.log(math.pow(variance_q, 2)/math.pow(variance_p, 2))) - 1)
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

frequent_itemsets = frequent_itemsets.withColumn("quant_values_KLD", calculate_KLD_between_discrete_and_normal($"quant_values"))
frequent_itemsets = frequent_itemsets.filter($"quant_values_KLD" <= 0.15)
frequent_itemsets = frequent_itemsets.withColumn("counter_quant_values_KLD", calculate_KLD_between_discrete_and_normal($"counter_quant_values"))
frequent_itemsets = frequent_itemsets.filter($"counter_quant_values_KLD" <= 0.15)

frequent_itemsets = frequent_itemsets.withColumn("KLD_between_normal", calculate_KLD_between_normal($"counter_quant_values", $"quant_values"))

frequent_itemsets = frequent_itemsets.filter(size($"counter_quant_values") > 0 )
frequent_itemsets = frequent_itemsets.withColumn("mean", mean_($"quant_values"))


frequent_itemsets = frequent_itemsets.orderBy(desc("KLD_between_normal")).limit(10).cache

frequent_itemsets.select($"items", $"freq", $"KLD_between_normal", $"mean").show(false)



// rules saving
frequent_itemsets = frequent_itemsets.withColumn("mean", ceil($"mean"))
frequent_itemsets.select($"items", $"mean").limit(10).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(" ", "").replace("),", " $\\Rightarrow$ ").replace(","," $\\wedge$ ").replace("[","").replace("]", "")).saveAsTextFile("XXX/results/scenario2/churn_for_bank_customers/latex__score__" + MINSUP)


// distributions saving
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/churn_for_bank_customers/score__1__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/churn_for_bank_customers/score__1__" + MINSUP + "__counter_set")

frequent_itemsets = frequent_itemsets.withColumn("id", monotonically_increasing_id()).filter($"id" > 0).drop("id")
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/churn_for_bank_customers/score__2__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/churn_for_bank_customers/score__2__" + MINSUP + "__counter_set")

