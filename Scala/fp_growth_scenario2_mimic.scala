import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.collection.immutable.ListMap
import org.apache.spark.ml.fpm.FPGrowth

val NUMPARTITIONS = 1440
val MINSUP = 0.025
//val MINSUP = 0.05
//val MINSUP = 0.075



def getCandidatesUDF = udf((itemset: mutable.Seq[mutable.Seq[Int]]) => {
	
	var candidatesList = new ListBuffer[Int]()
	for(item <- itemset){

        for(subitem <- item){
			candidatesList += subitem
		}
	}
	candidatesList
})

var patientsDF = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/mimic/all_patients.csv").drop("_c0").filter($"hospital_expire_flag" === 0).drop("hospital_expire_flag").cache()
var diagnoses_icd_DF = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/mimic/diagnoses_icd_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var procedures_icd_DF = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/mimic/procedures_icd_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var insurance_DF = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/mimic/insurance_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var admissions_type_DF = spark.read.option("header", true).option("inferSchema", true).csv("XXX/data/mimic/admissions_type_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()


var proceduresMap: Map[Int, String] = procedures_icd_DF.select("icd_code_pro_id", "icd_code_pro").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var proceduresMapBC = sc.broadcast(proceduresMap)

var diagnosesMap: Map[Int, String] = diagnoses_icd_DF.select("icd_code_dia_id", "icd_code_dia").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var diagnosesMapBC = sc.broadcast(diagnosesMap)

var admissions_typeMap: Map[Int, String] = admissions_type_DF.select("admission_type_id", "admission_type").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var admissions_typeMapBC = sc.broadcast(admissions_typeMap)

var insuranceMap: Map[Int, String] = insurance_DF.select("insurance_id", "insurance").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var insuranceMapBC = sc.broadcast(insuranceMap)



val temp = patientsDF
	.withColumn("insurance_id", $"insurance_id" + 10)
	.withColumn("admission_type_id", $"admission_type_id" + 100)
    .withColumn("icd_code_pro_id", $"icd_code_pro_id" + 1000)
    .withColumn("icd_code_dia_id", $"icd_code_dia_id" + 20000)
	.groupBy("hadm_id")
	.agg(collect_set("icd_code_pro_id") as("procedures_List"), collect_set("icd_code_dia_id") as("diagnoses_List"), collect_set("insurance_id")(0) as("insurance_id"), collect_set("admission_type_id")(0) as("admission_type_id"))
	.withColumn("procedures_List", sort_array($"procedures_List"))
	.withColumn("diagnoses_List", sort_array($"diagnoses_List"))

patientsDF = patientsDF.drop("insurance_id").drop("admission_type_id").drop("icd_code_pro_id").drop("icd_code_dia_id").distinct().join(temp, "hadm_id")


patientsDF = patientsDF.withColumn("variables", array(array($"insurance_id"), array($"admission_type_id"), array($"gender"), $"procedures_List", $"diagnoses_List"))
	.drop("insurance_id", "admission_type_id", "gender", "procedures_List", "diagnoses_List")
	.withColumn("variables", getCandidatesUDF($"variables"))
	.withColumn("basics", $"icu_los")
	.drop("icu_los")
	.cache


val fpgrowth = new FPGrowth().setItemsCol("variables").setMinSupport(MINSUP)
val model = fpgrowth.fit(patientsDF)

model.freqItemsets.show()
var frequent_itemsets = model.freqItemsets.cache



val patientsDFSet= patientsDF.select("variables", "basics").rdd.map(row => (row(0).asInstanceOf[Seq[Int]], row(1).asInstanceOf[Int])).collect
val inputDataDFHashmapBC = sc.broadcast(patientsDFSet)


def get_quant_values_UDF(input_Data_Array: Array[(Seq[Int],Int)]) = udf( (variables: Seq[Int]) => {

	var quant_values = new ListBuffer[Int]()
	for(input_Data <- input_Data_Array){
		val variables_are_inside = variables.forall(input_Data._1.contains)
		if(variables_are_inside){
			quant_values += input_Data._2
		}	
	}
	quant_values
})

def get_counter_quant_values_UDF(input_Data_Array: Array[(Seq[Int],Int)]) = udf( (variables: Seq[Int]) => {

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


def translateUDF(proceduresMap: Map[Int, String], diagnosesMap: Map[Int, String], admissions_typeMap: Map[Int, String], insuranceMap: Map[Int, String]) = udf((itemset: mutable.Seq[Int]) => {

	var rules = new ListBuffer[String]()

	for(item <- itemset){
		if(item == 0){
			rules += "female"
		}
		if(item == 1){
			rules += "male"
		}
        if(item >= 10 && item < 20){
			rules += insuranceMap(item-10)
		}
        if(item >= 100 && item < 200){
			rules += admissions_typeMap(item-100)
		}
		if(item >= 1000 && item < 20000){
			rules += proceduresMap(item-1000) + "_procedure"
		}
		if(item >= 20000){
			rules += diagnosesMap(item-20000) + "_diagnose"
		}
	}
	rules
})

def mean_ = udf((itemset: Seq[Int]) => {
	itemset.sum.toDouble / itemset.size
})


frequent_itemsets = frequent_itemsets.withColumn("quant_values", get_quant_values_UDF(inputDataDFHashmapBC.value)($"items"))
frequent_itemsets = frequent_itemsets.withColumn("counter_quant_values", get_counter_quant_values_UDF(inputDataDFHashmapBC.value)($"items"))

frequent_itemsets = frequent_itemsets.withColumn("quant_values_KLD", calculate_KLD_between_discrete_and_expo($"quant_values"))
frequent_itemsets = frequent_itemsets.filter($"quant_values_KLD" <= 0.15)
frequent_itemsets = frequent_itemsets.withColumn("counter_quant_values_KLD", calculate_KLD_between_discrete_and_expo($"counter_quant_values"))
frequent_itemsets = frequent_itemsets.filter($"counter_quant_values_KLD" <= 0.15)

frequent_itemsets = frequent_itemsets.withColumn("lambda_set", calculate_lambda($"quant_values"))
frequent_itemsets = frequent_itemsets.withColumn("lambda_counterSet", calculate_lambda($"counter_quant_values"))
frequent_itemsets = frequent_itemsets.withColumn("KLD_between_expo", calculate_KLD_between_expo($"lambda_counterSet", $"lambda_set"))
frequent_itemsets = frequent_itemsets.withColumn("mean", mean_($"quant_values"))


frequent_itemsets = frequent_itemsets.withColumn("counter_freq", size($"counter_quant_values")).orderBy(desc("KLD_between_expo"))
frequent_itemsets = frequent_itemsets.withColumn("rules", translateUDF(proceduresMapBC.value, diagnosesMapBC.value, admissions_typeMapBC.value, insuranceMapBC.value)($"items"))
frequent_itemsets = frequent_itemsets.limit(10).cache
frequent_itemsets.select($"rules", $"freq", $"KLD_between_expo", $"mean").show(10, false)



// rules saving
frequent_itemsets = frequent_itemsets.withColumn("mean", ceil($"mean"))
frequent_itemsets.select($"rules", $"mean").limit(10).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(" ", "").replace("),", " $\\Rightarrow$ ").replace(","," $\\wedge$ ").replace("[","").replace("]", "")).saveAsTextFile("XXX/results/scenario2/mimic/latex__los__" + MINSUP)

// distributions saving
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/mimic/los__1__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/mimic/los__1__" + MINSUP + "__counter_set")

frequent_itemsets = frequent_itemsets.withColumn("id", monotonically_increasing_id()).filter($"id" > 0).drop("id")
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/mimic/los__2__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("XXX/results/scenario2/mimic/los__2__" + MINSUP + "__counter_set")