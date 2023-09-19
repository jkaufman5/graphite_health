from typing import List
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession


DATA_BASE_PATH = "100k_synthea_covid19_csv"


def read_csv(spark: SparkSession, file_name: str) -> DataFrame:
    df = spark.read.csv(
        path=f"{DATA_BASE_PATH}/{file_name}", header=True, inferSchema=True
    )
    #
    return df


def get_covid_patients(conditions_df: DataFrame) -> DataFrame:
    covid_patients_df = (
        conditions_df
        # .filter(F.lower(F.col("description")).like("%covid%"))
        # +---------+------------------+
        # |     CODE|       description|
        # +---------+------------------+
        # |840544004|Suspected COVID-19|
        # |840539006|          COVID-19|
        # +---------+------------------+
        .filter(F.col("code") == 840539006)
        .select("patient")
        .distinct()
    )
    #
    return covid_patients_df


def get_covid_patient_conditions(
    conditions_df: DataFrame, covid_patients_df: DataFrame
) -> DataFrame:
    covid_patient_conditions_df = conditions_df.join(
        other=covid_patients_df, on=["patient"], how="inner"
    ).select("patient", "code", "description")
    #
    return covid_patient_conditions_df


def get_patients_by_cohort(df: DataFrame) -> DataFrame:
    cohorts_df = (
        df.withColumn(
            "pregnancy",
            F.when(
                # condition=F.col("description")).like("%pregnancy%")),
                # +--------+----------------------+
                # |code    |description           |
                # +--------+----------------------+
                # |47200007|Non-low risk pregnancy|
                # |79586000|Tubal pregnancy       |
                # |72892002|Normal pregnancy      |
                # +--------+----------------------+
                condition=F.col("code").isin(47200007, 72892002, 79586000),
                value=True,
            ).otherwise(value=False),
        )
        .withColumn(
            "asthma",
            F.when(
                # condition=F.col("description")).like("%asthma%")),
                # +---------+----------------+
                # |code     |description     |
                # +---------+----------------+
                # |195967001|Asthma          |
                # |233678006|Childhood asthma|
                # +---------+----------------+
                condition=F.col("code").isin(195967001, 233678006),
                value=True,
            ).otherwise(value=False),
        )
        .withColumn(
            "smoker",
            F.when(
                # condition=F.col("description")).like("%smoke%")),
                # +---------+--------------------+
                # |code     |description         |
                # +---------+--------------------+
                # |449868002|Smokes tobacco daily|
                # +---------+--------------------+
                condition=F.col("code") == 449868002,
                value=True,
            ).otherwise(value=False),
        )
    )
    #
    # Roll up and merge groups into one cohort column
    cohorts_df = (
        cohorts_df.groupBy(F.col("patient"))
        # Max on each group will come back True only if it is True in one or more rows
        .agg(
            F.max("pregnancy").alias("pregnancy"),
            F.max("asthma").alias("asthma"),
            F.max("smoker").alias("smoker"),
        )
        # Take note that this excludes patients that meet criteria for more than one of the three cohorts
        .withColumn(
            "cohort",
            F.when(
                # pregnancy: cohort a
                condition=F.col("pregnancy") & ~F.col("asthma") & ~F.col("smoker"),
                value="a",
            )
            .when(
                # asthma: cohort b
                condition=F.col("asthma") & ~F.col("pregnancy") & ~F.col("smoker"),
                value="b",
            )
            .when(
                # smoker: cohort c
                condition=F.col("smoker") & ~F.col("pregnancy") & ~F.col("asthma"),
                value="c",
            )
            .when(
                # none of the above: cohort d
                condition=~F.col("smoker") & ~F.col("pregnancy") & ~F.col("asthma"),
                value="d",
            )
            .otherwise(
                # catch-all
                value=None
            ),
        )
    )
    # (
    #     cohorts_df
    #     .drop("patient")
    #     .groupBy("pregnancy", "asthma", "smoker", "cohort")
    #     .count()
    #     .sort(F.col("count").desc())
    # ).show()
    #
    return cohorts_df


def main():
    spark = SparkSession.builder.appName("abc").getOrCreate()

    ###########################
    # Task 1.1: Data Ingestion
    ###########################
    conditions_df = read_csv(spark=spark, file_name="conditions.csv").cache()
    # TODO remove below if not used
    #  allergies_df = read_csv(spark=spark, file_name="allergies.csv")
    #  care_plans_df = read_csv(spark=spark, file_name="careplans.csv")
    #  devices_df = read_csv(spark=spark, file_name="devices.csv")
    #  encounters_df = read_csv(spark=spark, file_name="encounters.csv")
    #  imaging_studies_df = read_csv(spark=spark, file_name="imaging_studies.csv")
    #  immunizations_df = read_csv(spark=spark, file_name="immunizations.csv")
    #  medications_df = read_csv(spark=spark, file_name="medications.csv")
    #  observations_df = read_csv(spark=spark, file_name="observations.csv")
    #  organizations_df = read_csv(spark=spark, file_name="organizations.csv")
    #  patients_df = read_csv(spark=spark, file_name="patients.csv")
    #  payer_transitions_df = read_csv(spark=spark, file_name="payer_transitions.csv")
    #  payers_df = read_csv(spark=spark, file_name="payers.csv")
    #  procedures_df = read_csv(spark=spark, file_name="procedures.csv")
    #  providers_df = read_csv(spark=spark, file_name="providers.csv")
    #  supplies_df = read_csv(spark=spark, file_name="supplies.csv")

    ###############################
    # Task 1.2: Data Preprocessing
    ###############################
    covid_patients_df = get_covid_patients(conditions_df=conditions_df)
    # 88,166 patients have Covid-19

    covid_patient_conditions_df = get_covid_patient_conditions(
        conditions_df=conditions_df, covid_patients_df=covid_patients_df
    )
    # +--------------------+---------+--------------------+
    # |             patient|     code|         description|
    # +--------------------+---------+--------------------+
    # |1b9abba6-fc17-4af...|128613002|    Seizure disorder|
    # |1b9abba6-fc17-4af...|703151001|History of single...|
    # |1b9abba6-fc17-4af...| 59621000|        Hypertension|
    # +--------------------+---------+--------------------+

    patients_by_cohort_df = get_patients_by_cohort(df=covid_patient_conditions_df)
    # +---------+------+------+------+-----+
    # |pregnancy|asthma|smoker|cohort|count|
    # +---------+------+------+------+-----+
    # |    false| false| false|     d|79492|
    # |     true| false| false|     a| 6508|
    # |    false|  true| false|     b| 1563|
    # |    false| false|  true|     c|  551|
    # |     true|  true| false|  null|   45|
    # |     true| false|  true|  null|    7|
    # +---------+------+------+------+-----+

    # Task 1.2: Data Preprocessing
    # Identify patients with COVID-19.
    # Create 4 groups: Groups A, B, C, and D (patients with COVID-19 and one of the specified conditions)
    # A: pregnancy, B: asthma, C: smokers
    # and Group D (patients with COVID-19 without any of these conditions).
    # Preprocess the data to ensure it's ready for the ANOVA analysis.
    # Provide code/scripts for data preprocessing.


if __name__ == "__main__":
    main()
