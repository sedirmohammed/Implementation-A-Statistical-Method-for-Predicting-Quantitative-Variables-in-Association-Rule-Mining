import scala.collection.mutable
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.WrappedArray
import scala.collection.immutable.HashSet
import util.control.Breaks._
import scala.util.control._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap 
import scala.collection.immutable.ListMap

//println(rules.take(1).mkString(" "))

val NUMPARTITIONS = 1440
val MINSUP = 2526
//1263 (2.5%)
//2526 (5%)
//3788 (7.5%)


val combinationsUdf = udf((combinationSize: Int, arr: mutable.Seq[Int]) => arr.combinations(combinationSize).toList)


val itemsetsToHashSet = (items: List[Seq[Int]]) => {

	var newHashSet: HashSet[Seq[Int]] = HashSet.empty[Seq[Int]]

	for(item <- items){
		newHashSet = newHashSet + item
	}
	newHashSet
}


val itemsToHashSet = (items: List[Int]) => {

	var newHashSet: HashSet[Int] = HashSet.empty[Int]

	for(item <- items){
		newHashSet = newHashSet + item
	}
	newHashSet
}


def itemFilterUDF(m: HashSet[Int]) = udf( (itemset: mutable.Seq[Int]) => {
	itemset.filter(el => m(el))
})


def filterCandidatesIterationUDF(hashSet: HashSet[Seq[Int]], iteration: Int) = udf( (itemset: mutable.Seq[Int]) => {
	val subsets = itemset.combinations(iteration-1).toList
	var validCandidate = true

	val Outer = new Breaks
	Outer.breakable{
		for(subset <- subsets){

			if(hashSet(subset) == false){
				validCandidate = false
				Outer.break
			}
		}
	}

	if(validCandidate){
		true
	}else{
		false
	}
})


def filterInputDataUDF(hashSet: HashSet[Seq[Int]], iteration: Int) = udf( (itemset: mutable.Seq[Int]) => {
	val subsets = itemset.combinations(iteration).toList
	var validCandidate = false

	val Outer = new Breaks
	Outer.breakable{
		for(subset <- subsets){

			if(hashSet(subset) == true){
				validCandidate = true
				Outer.break
			}
		}
	}
	if(validCandidate){
		true
	}else{
		false
	}
})


def getCandidatesUDF = udf((itemset: mutable.Seq[mutable.Seq[Int]]) => {
	
	var candidatesList = new ListBuffer[Int]()
	for(item <- itemset){

        for(subitem <- item){
			candidatesList += subitem
		}
	}
	candidatesList
})


def checkLOSContainUDF = udf((itemset: mutable.Seq[Int]) => {
	
	var los_row = false

	val Outer = new Breaks
	Outer.breakable{
		for(item <- itemset){

			if(item >= 1000 && item <= 1010){
				los_row = true
				Outer.break
			}	
		}
	}
	if(los_row){
		true
	}else{
		false
	}
})


def calculateKLconfidenceUDF = udf((itemset: mutable.Seq[Int]) => {

	val own_distribution = itemset.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / itemset.size)
	val own_distribution_sorted = ListMap(own_distribution.toSeq.sortWith(_._1 < _._1):_*)
	val average = itemset.sum/itemset.size.toDouble
	val lambda = 1/average
	val own_distribution_list = own_distribution_sorted.toList
	var KL = 0.0
	for (p <- own_distribution_list) { 
		val temp = p._2 * (breeze.numerics.log(p._2) - breeze.numerics.log(lambda) + (lambda/2) * (2 * p._1 + 1))
		KL = KL + temp
	}
	KL
})


def calculateMeanUDF = udf((itemset: mutable.Seq[Int]) => {

	val average = itemset.sum/itemset.size.toDouble
	average.round
})



def translateUDF(proceduresMap: Map[Int, String], diagnosesMap: Map[Int, String], admissions_typeMap: Map[Int, String], insuranceMap: Map[Int, String]) = udf((itemset: mutable.Seq[Int]) => {

	var rules = new ListBuffer[String]()

	for(item <- itemset){
		if(item == 0){
			rules += "weiblich"
		}
		if(item == 1){
			rules += "männlich"
		}
        if(item >= 10 && item < 20){
			rules += insuranceMap(item-10)
		}
        if(item >= 100 && item < 200){
			rules += admissions_typeMap(item-100)
		}
		if(item >= 1000 && item < 20000){
			rules += proceduresMap(item-1000)
		}
		if(item >= 20000){
			rules += diagnosesMap(item-20000)
		}
	}
	rules
})


def measure_time[FUNCTION](f: => FUNCTION, iteration: => Int) = {

	val starttime = System.nanoTime
	val function_variable = f

	println("Zeit für Iteration "  + iteration + ": " + (System.nanoTime-starttime)/1e9 + "s")	
	function_variable
}


def calculateNewKLUDF(hadm_idArray: Array[(Int,Int)]) = udf( (hadm_ids: Seq[Int], los: Seq[Int]) => {

	var hadm_idsSet: HashSet[Int] = HashSet.empty[Int]
	for(hadm_id <- hadm_ids){
		hadm_idsSet = hadm_idsSet + hadm_id
	}

 	val counterSetHadm_idArray = hadm_idArray.filter(el => !hadm_idsSet(el._1))
 	val counterSetLOSValues = counterSetHadm_idArray.map(_._2)

 	val counterSetLOSDistribution = counterSetLOSValues.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / counterSetLOSValues.size)
 	val counterSetLOSDistributionSorted = ListMap(counterSetLOSDistribution.toSeq.sortWith(_._1 < _._1):_*)

 	val innerSetLOSDistribution = los.groupBy(identity).mapValues(_.size).mapValues(_.asInstanceOf[Float] / los.size)
 	val innerSetLOSDistributionSorted = ListMap(innerSetLOSDistribution.toSeq.sortWith(_._1 < _._1):_*)//.toList

	var KL = 0.0
 	//for (p <- innerSetLOSDistributionSorted) { 
 	//	if(counterSetLOSDistributionSorted.contains(p._1) == true){
	//		val temp = p._2 * (breeze.numerics.log(p._2/counterSetLOSDistributionSorted(p._1)))
	//		KL = KL + temp
	//	}
	//}

	for (p <- counterSetLOSDistributionSorted) { 
 		if(innerSetLOSDistributionSorted.contains(p._1) == true){
			val temp = p._2 * (breeze.numerics.log(p._2/innerSetLOSDistributionSorted(p._1)))
			KL = KL + temp
		}
	}
	KL
})


def calculateLOSValuesCounterSetUDF(hadm_idArray: Array[(Int,Int)]) = udf( (hadm_ids: Seq[Int]) => {

	var hadm_idsSet: HashSet[Int] = HashSet.empty[Int]
	for(hadm_id <- hadm_ids){
		hadm_idsSet = hadm_idsSet + hadm_id
	}

 	val counterSetHadm_idArray = hadm_idArray.filter(el => !hadm_idsSet(el._1))
 	val counterSetLOSValues = counterSetHadm_idArray.map(_._2)

	counterSetLOSValues
})


var patientsDF = spark.read.option("header", true).option("inferSchema", true).csv("hdfs:///user/mohammes/all_patients.csv").drop("_c0").repartition(NUMPARTITIONS).filter($"hospital_expire_flag" === 0).drop("hospital_expire_flag").cache()
var diagnoses_icd_DF = spark.read.option("header", true).option("inferSchema", true).csv("hdfs:///user/mohammes/diagnoses_icd_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var procedures_icd_DF = spark.read.option("header", true).option("inferSchema", true).csv("hdfs:///user/mohammes/procedures_icd_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var insurance_DF = spark.read.option("header", true).option("inferSchema", true).csv("hdfs:///user/mohammes/insurance_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()
var admissions_type_DF = spark.read.option("header", true).option("inferSchema", true).csv("hdfs:///user/mohammes/admissions_type_DF.csv").drop("_c0").repartition(NUMPARTITIONS).cache()


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

val patientsDF_orig = patientsDF
	

patientsDF.withColumn("len", size($"variables"))

var proceduresMap: Map[Int, String] = procedures_icd_DF.select("icd_code_pro_id", "icd_code_pro").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var proceduresMapBC = sc.broadcast(proceduresMap)

var diagnosesMap: Map[Int, String] = diagnoses_icd_DF.select("icd_code_dia_id", "icd_code_dia").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var diagnosesMapBC = sc.broadcast(diagnosesMap)

var admissions_typeMap: Map[Int, String] = admissions_type_DF.select("admission_type_id", "admission_type").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var admissions_typeMapBC = sc.broadcast(admissions_typeMap)

var insuranceMap: Map[Int, String] = insurance_DF.select("insurance_id", "insurance").map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])).collect.toMap
var insuranceMapBC = sc.broadcast(insuranceMap)

val patientsDFSet = patientsDF_orig.select("hadm_id", "basics").rdd.map(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int])).collect
val patientsDFHashmapBC = sc.broadcast(patientsDFSet)


var currentFrequents = spark.emptyDataFrame
var previousFrequents = spark.emptyDataFrame
var itemsetHashSet: HashSet[Seq[Int]] = HashSet.empty[Seq[Int]]
var itemsetHashSetBC = sc.broadcast(itemsetHashSet)

var rules = spark.emptyDataFrame
var rule_count = " "
var patients_count = " "

var all_rules = spark.emptyDataFrame



val Outer = new Breaks
var iteration = 0

spark.time(

Outer.breakable{

while(true){
	measure_time({

	iteration += 1
	
	var candidates = spark.emptyDataFrame

	candidates = patientsDF
		.select($"hadm_id", $"basics", explode(combinationsUdf(lit(iteration), $"variables")) as "candidates").cache
		.repartition(NUMPARTITIONS, $"candidates")


	if(iteration > 1){

		candidates = candidates
			.select($"hadm_id", $"basics",$"candidates",filterCandidatesIterationUDF(itemsetHashSetBC.value, iteration)($"candidates").as("result"))
			.filter($"result" === true)
	}

	currentFrequents = candidates
		.groupBy("candidates")
		.agg(collect_list("basics").as("output"), count("*").as("support"), collect_list("hadm_id").as("hadm_ids"))
		.filter($"support" >= MINSUP)
		.cache()


	if(currentFrequents.count == 0){
		Outer.break
	}

	//if(iteration == 2){
	//	Outer.break
	//}


	val itemsets = currentFrequents
			.select("candidates")
			.rdd
			.map(r => r(0).asInstanceOf[Seq[Int]])

	itemsetHashSet = itemsetsToHashSet(itemsets.collect.toList)
	itemsetHashSetBC = sc.broadcast(itemsetHashSet)


	val items = currentFrequents
		.select("candidates")
		.rdd
		.map(r => r(0).asInstanceOf[Seq[Int]])
		.flatMap(identity)
	val itemsHashSet: HashSet[Int] = itemsToHashSet(items.collect.toList)
	val itemsHashSetBC = sc.broadcast(itemsHashSet)

	
	patientsDF = patientsDF.select($"hadm_id", $"basics", itemFilterUDF(itemsHashSetBC.value)($"variables").as("variables")).filter(size($"variables") > iteration)
	patientsDF = patientsDF.select($"hadm_id", $"basics", $"variables", filterInputDataUDF(itemsetHashSetBC.value, iteration)($"variables").as("result")).filter($"result" === true).cache


	previousFrequents = currentFrequents
		.withColumn("counter_set_KL", calculateNewKLUDF(patientsDFHashmapBC.value)($"hadm_ids", $"output"))
		.withColumn("counter_set_LOS_values", calculateLOSValuesCounterSetUDF(patientsDFHashmapBC.value)($"hadm_ids"))



	//previousFrequents
	//	.orderBy(desc("support"))
	//	//.withColumn("LOS_values", extractLOSUDF($"output"))
	//	.withColumn("LOS_Mean", calculateMeanUDF($"output"))
	//	.withColumn("KL", calculateKLconfidenceUDF($"output"))
	//	.orderBy(asc("KL"))
	//	.drop("KL")
	//	.drop("support")
	//	.drop("LOS_Mean")
	//	.show(3, false)


	//rule_count = rule_count + " " +  previousFrequents.rdd.count.toString
	//patients_count = patients_count + " " +  patientsDF.rdd.count.toString

	rules = previousFrequents
		.orderBy(desc("support"))
		.withColumnRenamed("output","LOS_values")
		.withColumn("LOS_Mean", calculateMeanUDF($"LOS_values"))
		.withColumn("KL", calculateKLconfidenceUDF($"LOS_values"))
		.orderBy(desc("KL"))
		.withColumn("rules", translateUDF(proceduresMapBC.value, diagnosesMapBC.value, admissions_typeMapBC.value, insuranceMapBC.value)($"candidates"))
		//.drop("LOS_values")
		.drop("output")
		.drop("candidates")
		.drop("hadm_ids")
	


	//println(rules.count)
	//rules.show(10, false)

	//println(previousFrequents.take(5).mkString(" "))
	if(iteration == 1){
		all_rules = rules
	}else{
		all_rules = all_rules.union(rules)
	}


	//rules.select($"LOS_Mean", $"KL", $"rules").show(10, false)
	//rules.agg(min("KL") as "min", max("KL") as "max", mean("KL") as "mean").show
	patients_count = patients_count + " " +  rules.rdd.count.toString


	}, iteration)

}
}
)


all_rules = all_rules.withColumn("rounded_score", round(col("counter_set_KL"), 4)).orderBy(desc("counter_set_KL")).cache



all_rules.orderBy(desc("counter_set_KL")).withColumn("rounded_score", round(col("counter_set_KL"), 4)).show(10, false)
all_rules.orderBy(desc("KL")).withColumn("rounded_score", round(col("KL"), 4)).show(10, false)




 all_rules.orderBy(desc("counter_set_KL")).withColumn("rounded_score", round(col("counter_set_KL"), 4)).limit(10).select(avg($"rounded_score")).show()   

