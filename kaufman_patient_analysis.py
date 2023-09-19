from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession


DATA_BASE_PATH = "100k_synthea_covid19_csv"


def read_csv(spark: SparkSession, file_name: str) -> DataFrame:
	df = spark.read.csv(
		path=f"{DATA_BASE_PATH}/{file_name}",
		header=True,
		inferSchema=True
	)
	# TODO tmp
	df.cache()
	return df


def get_covid_patients(spark: SparkSession) -> DataFrame:



def main():
	spark = SparkSession.builder.appName('abc').getOrCreate()

	# Read files
	allergies_df = read_csv(spark=spark, file_name="allergies.csv")
	care_plans_df = read_csv(spark=spark, file_name="careplans.csv")
	conditions_df = read_csv(spark=spark, file_name="conditions.csv")
	devices_df = read_csv(spark=spark, file_name="devices.csv")
	encounters_df = read_csv(spark=spark, file_name="encounters.csv")
	imaging_studies_df = read_csv(spark=spark, file_name="imaging_studies.csv")
	immunizations_df = read_csv(spark=spark, file_name="immunizations.csv")
	medications_df = read_csv(spark=spark, file_name="medications.csv")
	observations_df = read_csv(spark=spark, file_name="observations.csv")
	organizations_df = read_csv(spark=spark, file_name="organizations.csv")
	patients_df = read_csv(spark=spark, file_name="patients.csv")
	payer_transitions_df = read_csv(spark=spark, file_name="payer_transitions.csv")
	payers_df = read_csv(spark=spark, file_name="payers.csv")
	procedures_df = read_csv(spark=spark, file_name="procedures.csv")
	providers_df = read_csv(spark=spark, file_name="providers.csv")
	supplies_df = read_csv(spark=spark, file_name="supplies.csv")

	covid_patients = get_covid_patients(spark=spark)

	# Task 1.2: Data Preprocessing
	# Identify patients with COVID-19.
	# Create 4 groups: Groups A, B, C, and D (patients with COVID-19 and one of the specified conditions)
	# and Group D (patients with COVID-19 without any of these conditions).
	# Preprocess the data to ensure it's ready for the ANOVA analysis.
	# Provide code/scripts for data preprocessing.


if __name__ == "__main__":
	main()
