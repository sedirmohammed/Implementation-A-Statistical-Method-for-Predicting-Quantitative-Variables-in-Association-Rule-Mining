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

def calculate_KLD = udf((quant_values: Seq[Int], counter_quant_values: Seq[Int]) => {


 	var counter_quant_values_distribution = counter_quant_values.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / counter_quant_values.size)
 	counter_quant_values_distribution = ListMap(counter_quant_values_distribution.toSeq.sortWith(_._1 < _._1):_*)

 	var quant_values_distribution = quant_values.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / quant_values.size)
 	quant_values_distribution = ListMap(quant_values_distribution.toSeq.sortWith(_._1 < _._1):_*)

	var KLD = 0.0
	for (p <- counter_quant_values_distribution) { 
 		if(quant_values_distribution.contains(p._1) == true){
			val temp = p._2 * (breeze.numerics.log(p._2/quant_values_distribution(p._1)))
			KLD = KLD + temp
		}
	}
	KLD
})

def mean_ = udf((itemset: Seq[Int]) => {
	itemset.sum.toDouble / itemset.size
})


val MINSUP = 0.05


var df = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/telco_customer_churn/telco_customer_churn.csv").cache()
df = df.withColumn("quant_variable", col("tenure")).drop("tenure").drop("churn")
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
frequent_itemsets = frequent_itemsets.withColumn("KLD", calculate_KLD($"quant_values", $"counter_quant_values"))

frequent_itemsets = frequent_itemsets.filter(size($"counter_quant_values") > 0 )
frequent_itemsets = frequent_itemsets.withColumn("mean", mean_($"quant_values"))

frequent_itemsets = frequent_itemsets.orderBy(desc("KLD")).limit(10).cache
frequent_itemsets.select($"items", $"freq", $"KLD", $"mean").show(false)




// rules saving
frequent_itemsets = frequent_itemsets.withColumn("mean", ceil($"mean"))
frequent_itemsets.select($"items", $"mean").limit(10).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(" ", "").replace("),", " $\\Rightarrow$ ").replace(","," $\\wedge$ ").replace("[","").replace("]", "")).saveAsTextFile("XXX/results/scenario1/telco_customer_churn/latex__tenure__" + MINSUP)


// distributions saving
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario1/telco_customer_churn/tenure__1__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario1/telco_customer_churn/tenure__1__" + MINSUP + "__counter_set")

frequent_itemsets = frequent_itemsets.withColumn("id", monotonically_increasing_id()).filter($"id" > 0).drop("id")
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario1/telco_customer_churn/tenure__2__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario1/telco_customer_churn/tenure__2__" + MINSUP + "__counter_set")
