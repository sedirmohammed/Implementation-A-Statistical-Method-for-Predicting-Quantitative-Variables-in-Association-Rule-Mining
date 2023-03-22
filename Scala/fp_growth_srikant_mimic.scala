import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.math._
import org.apache.spark.ml.fpm.FPGrowth

val NUMPARTITIONS = 1440
//val MINSUP = 0.025
val MINSUP = 0.05
//val MINSUP = 0.075
val CONFIDENCE = 0.06
val K = 3


def getCandidatesUDF = udf((itemset: mutable.Seq[mutable.Seq[Any]]) => {
	
	var candidatesList = new ListBuffer[String]()
	for(item <- itemset){

      for(subitem <- item){
				candidatesList += subitem.toString
		}
	}
	candidatesList
})

var patientsDF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/all_patients_intervals_0.05_3.csv").drop("_c0").filter($"hospital_expire_flag" === 0).drop("hospital_expire_flag").cache()
var diagnoses_icd_DF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/diagnoses_icd_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var procedures_icd_DF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/procedures_icd_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var insurance_DF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/insurance_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var admissions_type_DF = spark.read.option("header", true).option("inferSchema", true).csv("/home/sedirmohammed/dev/dr_rer_medic/data/admissions_type_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()


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

patientsDF = patientsDF.withColumn("variables", array(array($"insurance_id"), array($"admission_type_id"), array($"gender"), $"procedures_List", $"diagnoses_List", array($"icu_los")))
	.drop("insurance_id", "admission_type_id", "gender", "procedures_List", "diagnoses_List")
	.withColumn("variables", getCandidatesUDF($"variables"))
	.cache


val fpgrowth = new FPGrowth().setItemsCol("variables").setMinSupport(MINSUP).setMinConfidence(CONFIDENCE)
val model = fpgrowth.fit(patientsDF)

model.freqItemsets.show()
var frequent_itemsets = model.freqItemsets
var rules = model.associationRules




def translateUDF(proceduresMap: Map[Int, String], diagnosesMap: Map[Int, String], admissions_typeMap: Map[Int, String], insuranceMap: Map[Int, String]) = udf((itemset: mutable.Seq[String]) => {

	var rules = new ListBuffer[String]()

	for(item <- itemset){
		val item_int = item.toInt
		if(item_int == 0){
			rules += "weiblich"
		}
		if(item_int == 1){
			rules += "männlich"
		}
        if(item_int >= 10 && item_int < 20){
			rules += insuranceMap(item_int-10)
		}
        if(item_int >= 100 && item_int < 200){
			rules += admissions_typeMap(item_int-100)
		}
		if(item_int >= 1000 && item_int < 20000){
			rules += proceduresMap(item_int-1000)
		}
		if(item_int >= 20000){
			rules += diagnosesMap(item_int-20000)
		}
	}
	rules
})


def quant_value_in_consequent = udf((itemset: mutable.Seq[String]) => {
	
	var valid_rule= false

	for(item <- itemset){

		if(item.contains("_")){
			valid_rule = true
		}

	}
	valid_rule
})




rules = rules.withColumn("valid_rule", quant_value_in_consequent($"consequent")).filter($"valid_rule" === true).drop("valid_rule")
rules = rules.withColumn("rules", translateUDF(proceduresMapBC.value, diagnosesMapBC.value, admissions_typeMapBC.value, insuranceMapBC.value)($"antecedent"))
rules = rules.orderBy(desc("confidence"))
rules.show
