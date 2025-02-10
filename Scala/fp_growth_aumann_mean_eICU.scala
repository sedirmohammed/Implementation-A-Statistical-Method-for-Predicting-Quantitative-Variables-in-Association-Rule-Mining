import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.math._
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions.ceil

val NUMPARTITIONS = 4
//val MINSUP = 0.025
//val MINSUP = 0.05
val MINSUP = 0.075



def getCandidatesUDF = udf((itemset: mutable.Seq[mutable.Seq[Int]]) => {
	
	var candidatesList = new ListBuffer[Int]()
	for(item <- itemset){

        for(subitem <- item){
			candidatesList += subitem
		}
	}
	candidatesList
})

var patientsDF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/eICU/patients_icd10.csv").cache()

val diagnoses_icd_DF_schema = StructType(Seq(
  StructField("icd_code_dia_id", IntegerType, true),
  StructField("icd_code_dia", StringType, true)
))
var diagnoses_icd_DF = spark.read.option("header", true).option("inferSchema", true).schema(diagnoses_icd_DF_schema) .csv("/home/sedirmohammed/dev/dr_rer_medic/data/eICU/icd10_mapping.csv").repartition(NUMPARTITIONS).cache()
var admissions_type_DF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/mimic/admissions_type_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()


var diagnosesMap: Map[Int, String] = diagnoses_icd_DF.select("icd_code_dia_id", "icd_code_dia").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var diagnosesMapBC = sc.broadcast(diagnosesMap)

var admissions_typeMap: Map[Int, String] = admissions_type_DF.select("admission_type_id", "admission_type").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var admissions_typeMapBC = sc.broadcast(admissions_typeMap)



val temp = patientsDF
	.withColumn("admission_type_id", $"admission_type_id" + 100)
    .withColumn("icd_code_dia_id", $"icd_code_dia_id" + 20000)
	.groupBy("patientunitstayid")
	.agg(collect_set("icd_code_dia_id") as("diagnoses_List"), collect_set("admission_type_id")(0) as("admission_type_id"))
	.withColumn("diagnoses_List", sort_array($"diagnoses_List"))

patientsDF = patientsDF.drop("admission_type_id").drop("icd_code_dia_id").distinct().join(temp, "patientunitstayid")


patientsDF = patientsDF.withColumn("variables", array(array($"admission_type_id"), array($"gender"), $"diagnoses_List"))
	.drop("admission_type_id", "gender", "diagnoses_List")
	.withColumn("variables", getCandidatesUDF($"variables"))
	.withColumn("basics", $"icu_los")
	.drop("icu_los")
	.cache

val fpgrowth = new FPGrowth().setItemsCol("variables").setMinSupport(MINSUP)
val model = fpgrowth.fit(patientsDF)

model.freqItemsets.show()
var frequent_itemsets = model.freqItemsets



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


def median(s: Seq[Int]): Double = {
	val (lower, upper) = s.sortWith((l, r) => l < r).splitAt(s.size / 2)
	if (s.size % 2 == 0) (lower.last + upper.head) / 2.toDouble else upper.head
}


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


	//if(z < -1.96 || z > 1.96){
	//	-100
	//}else{
	//	mean_quant_values - mean_counter_quant_values
	//}

	mean_quant_values - mean_counter_quant_values

})


def translateUDF(diagnosesMap: Map[Int, String], admissions_typeMap: Map[Int, String]) = udf((itemset: mutable.Seq[Int]) => {

	var rules = new ListBuffer[String]()

	for(item <- itemset){
		if(item == 0){
			rules += "female"
		}
		if(item == 1){
			rules += "male"
		}
        if(item >= 100 && item < 200){
			rules += admissions_typeMap(item-100)
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
frequent_itemsets = frequent_itemsets.withColumn("counter_quant_values", get_counter_quant_values_UDF(inputDataDFHashmapBC.value)($"items")).cache()


frequent_itemsets = frequent_itemsets.withColumn("difference_in_summary_statistics", calculate_difference_in_summary_statistics($"quant_values", $"counter_quant_values"))
frequent_itemsets = frequent_itemsets.withColumn("rules", translateUDF(diagnosesMapBC.value, admissions_typeMapBC.value)($"items"))


frequent_itemsets = frequent_itemsets.filter(size($"counter_quant_values") > 0 )
frequent_itemsets = frequent_itemsets.withColumn("mean", mean_($"quant_values"))


frequent_itemsets = frequent_itemsets.orderBy(desc("difference_in_summary_statistics")).limit(10).cache

frequent_itemsets.select($"rules", $"freq", $"difference_in_summary_statistics", $"mean").show(10, false)



// rules saving
frequent_itemsets = frequent_itemsets.withColumn("mean", ceil($"mean"))
frequent_itemsets.select($"rules", $"mean").limit(10).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(" ", "").replace("),", " $\\Rightarrow$ ").replace(","," $\\wedge$ ").replace("[","").replace("]", "")).saveAsTextFile("/home/sedirmohammed/dev/dr_rer_medic/results/aumann/eICU/latex__los__" + MINSUP)



// distributions saving
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("/home/sedirmohammed/dev/dr_rer_medic/results/aumann/eICU/aumann__los__1__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("/home/sedirmohammed/dev/dr_rer_medic/results/aumann/eICU/aumann__los__1__" + MINSUP + "__counter_set")

frequent_itemsets = frequent_itemsets.withColumn("id", monotonically_increasing_id()).filter($"id" > 0).drop("id")
frequent_itemsets.select($"quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("/home/sedirmohammed/dev/dr_rer_medic/results/aumann/eICU/aumann__los__2__" + MINSUP + "__set")
frequent_itemsets.select($"counter_quant_values").limit(1).rdd.repartition(1).map(_.toString().replace("WrappedArray(","").replace(")", "").replace("[","").replace("]", "").replace(" ", "").replace(",", "\n")).saveAsTextFile("/home/sedirmohammed/dev/dr_rer_medic/results/aumann/eICU/aumann__los__2__" + MINSUP + "__counter_set")