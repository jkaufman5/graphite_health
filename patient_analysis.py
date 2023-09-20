from typing import List, NoReturn
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession, Window


DATA_BASE_PATH = "100k_synthea_covid19_csv"
OUTPUT_BASE_DIR = "output_data"


def read_csv(spark: SparkSession, file_name: str) -> DataFrame:
    df = spark.read.csv(
        path=f"{DATA_BASE_PATH}/{file_name}", header=True, inferSchema=True
    )

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
        .groupBy(F.col("patient"))
        .agg(F.min(F.col("start")).alias("covid_start_date"))
    )

    return covid_patients_df


def get_covid_patient_conditions(
    conditions_df: DataFrame, covid_patients_df: DataFrame
) -> DataFrame:
    covid_patient_conditions_df = (
        conditions_df.join(other=covid_patients_df, on=["patient"], how="inner")
        # Exclude Covid and Suspected Covid
        .filter(~F.col("code").isin(840539006, 840544004))
        .withColumnRenamed(existing="code", new="symptom_code")
        .withColumnRenamed(existing="description", new="symptom_desc")
        .withColumnRenamed(existing="start", new="condition_start_date")
        .select("patient", "symptom_code", "symptom_desc", "condition_start_date")
    )

    return covid_patient_conditions_df


def get_patients_by_cohort(df: DataFrame) -> List[DataFrame]:
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
                condition=F.col("symptom_code").isin(47200007, 72892002, 79586000),
                value=True,
            ).otherwise(value=False),
        )
        .withColumn(
            "asthma",
            F.when(
                # condition=F.col("description")).like("%asthma%")),
                # +------------+----------------+
                # |symptom_code|description     |
                # +------------+----------------+
                # |195967001   |Asthma          |
                # |233678006   |Childhood asthma|
                # +------------+----------------+
                condition=F.col("symptom_code").isin(195967001, 233678006),
                value=True,
            ).otherwise(value=False),
        )
        .withColumn(
            "smoker",
            F.when(
                # condition=F.col("description")).like("%smoke%")),
                # +------------+--------------------+
                # |symptom_code|description         |
                # +------------+--------------------+
                # |449868002   |Smokes tobacco daily|
                # +------------+--------------------+
                condition=F.col("symptom_code") == 449868002,
                value=True,
            ).otherwise(value=False),
        )
    )

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
        .withColumn(
            "cohort_name",
            F.when(condition=F.col("cohort") == "a", value="pregnancy")
            .when(condition=F.col("cohort") == "b", value="asthma")
            .when(condition=F.col("cohort") == "c", value="smoker")
            .when(condition=F.col("cohort") == "d", value="")
            .otherwise(value=None),
        )
        # Exclude exclusions (small percentage of patients
        # who do not fall nicely into a single cohort (E.g., pregnant and have asthma)
        .filter(F.col("cohort").isNotNull())
        .select("patient", "cohort", "cohort_name")
    ).cache()

    patients_per_cohort_df = cohorts_df.groupBy(
        F.col("cohort"), F.col("cohort_name")
    ).agg(F.countDistinct(F.col("patient")).alias("patients_in_cohort"))

    return [cohorts_df, patients_per_cohort_df]


def get_conditions_during_or_after_covid(
    covid_patients_df: DataFrame,
    covid_patients_all_conditions_df: DataFrame,
    covid_patients_by_cohort_df: DataFrame,
    patient_count_per_cohort_df: DataFrame,
) -> DataFrame:
    other_cond_df = (
        covid_patients_all_conditions_df.join(
            other=covid_patients_by_cohort_df,
            on=["patient"],
            how="inner",
        )
        .join(other=covid_patients_df, on=["patient"], how="inner")
        .join(
            other=patient_count_per_cohort_df.drop(F.col("cohort_name")),
            on=["cohort"],
            how="inner",
        )
        # Condition started on or after Covid
        .filter(F.col("condition_start_date") >= F.col("covid_start_date"))
        .groupBy(
            F.col("cohort"),
            F.col("cohort_name"),
            F.col("symptom_code"),
            F.col("symptom_desc"),
            F.col("patients_in_cohort"),
        )
        .agg(F.countDistinct(F.col("patient")).alias("patient_with_symptom"))
    )

    other_conditions_agg_df = (
        other_cond_df.withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy(F.col("cohort")).orderBy(
                    F.col("patients_with_symptom").desc(), F.col("symptom_desc")
                )
            ),
        )
        .withColumn(
            "percent_patients_with_symptom",
            F.round(
                col=F.col("patients_with_symptom") / F.col("patients_in_cohort"),
                scale=2,
            ),
        )
        .filter(F.col("row_num") <= 5)
        .sort(F.col("cohort"), F.col("row_num"))
        .select(
            "cohort",
            "cohort_name",
            "symptom_desc",
            "symptom_code",
            "patients_in_cohort",
            "patients_with_symptom",
            "percent_patients_with_symptom",
        )
    )

    return other_conditions_agg_df


def save_as_tsv(df: DataFrame, file_path: str) -> NoReturn:
    (
        df.write.option("header", True).csv(
            header=True,
            sep="\t",
            path=f"{OUTPUT_BASE_DIR}/{file_path}.tsv",
            mode="overwrite",
        )
    )


def main():
    spark = SparkSession.builder.appName("abc").getOrCreate()

    ###########################
    # Task 1.1: Data Ingestion
    ###########################
    conditions_df = read_csv(spark=spark, file_name="conditions.csv").cache()

    ###############################
    # Task 1.2: Data Preprocessing
    ###############################
    covid_patients_df = get_covid_patients(conditions_df=conditions_df).cache()
    # 88,166 patients have Covid-19
    # +--------------------+----------------+
    # |             patient|covid_start_date|
    # +--------------------+----------------+
    # |1b9abba6-fc17-4af...|      2020-03-08|
    # |ec7f9ffa-5d03-466...|      2020-03-02|
    # |53afada1-44d9-461...|      2020-03-04|
    # +--------------------+----------------+

    covid_patients_all_conditions_df = get_covid_patient_conditions(
        conditions_df=conditions_df, covid_patients_df=covid_patients_df
    ).cache()
    # +--------------------+---------+--------------------+--------------------+
    # |             patient|     code|         description|condition_start_date|
    # +--------------------+---------+--------------------+--------------------+
    # |1ff7f10f-a204-4bb...|386661006|     Fever (finding)|          2020-03-01|
    # |9bcf6ed5-d808-44a...| 44465007|     Sprain of ankle|          2020-02-12|
    # |9bcf6ed5-d808-44a...| 49727002|     Cough (finding)|          2020-03-13|
    # +--------------------+---------+--------------------+--------------------+

    covid_patients_by_cohort_df, patient_count_per_cohort_df = get_patients_by_cohort(
        df=covid_patients_all_conditions_df
    )
    # +--------------------+------+-------------+
    # |             patient|cohort|  cohort_name|
    # +--------------------+------+-------------+
    # |1b9abba6-fc17-4af...|     a|    pregnancy|
    # |bf138b41-d49b-40f...|     b|     asthma  |
    # |b8e071b6-3a2d-48d...|     c|       smoker|
    # |ec7f9ffa-5d03-466...|     d|             |
    # +--------------------+------+-------------+

    # +------+-------------+--------+
    # |cohort|  cohort_name|patients|
    # +------+-------------+--------+
    # |     a|    pregnancy|    6508|
    # |     b|       asthma|    1563|
    # |     c|       smoker|     551|
    # |     d|             |   79461|
    # +------+-------------+--------+

    ################################################
    # Task 2.1: ANOVA Calculation, compare symptoms
    ################################################
    other_conditions_df = get_conditions_during_or_after_covid(
        covid_patients_df=covid_patients_df,
        covid_patients_all_conditions_df=covid_patients_all_conditions_df,
        covid_patients_by_cohort_df=covid_patients_by_cohort_df,
        patient_count_per_cohort_df=patient_count_per_cohort_df,
    )
    # +------+-----------+------------+------------------------+------------------+-------------+
    # |cohort|cohort_name|symptom_code|symptom_desc            |patients_in_cohort|patient_count|
    # +------+-----------+------------+------------------------+------------------+-------------+
    # |a     |pregnancy  |386661006   |Fever (finding)         |6508              |5534         |
    # |a     |pregnancy  |49727002    |Cough (finding)         |6508              |4206         |
    # |a     |pregnancy  |36955009    |Loss of taste (finding) |6508              |3156         |
    # |a     |pregnancy  |84229001    |Fatigue (finding)       |6508              |2414         |
    # |a     |pregnancy  |248595008   |Sputum finding (finding)|6508              |2122         |
    # |b     |asthma     |386661006   |Fever (finding)         |1563              |1321         |
    # |b     |asthma     |49727002    |Cough (finding)         |1563              |1024         |
    # |b     |asthma     |36955009    |Loss of taste (finding) |1563              |731          |
    # |b     |asthma     |84229001    |Fatigue (finding)       |1563              |574          |
    # |b     |asthma     |248595008   |Sputum finding (finding)|1563              |502          |
    # |c     |smoker     |386661006   |Fever (finding)         |551               |477          |
    # |c     |smoker     |49727002    |Cough (finding)         |551               |375          |
    # |c     |smoker     |36955009    |Loss of taste (finding) |551               |265          |
    # |c     |smoker     |84229001    |Fatigue (finding)       |551               |212          |
    # |c     |smoker     |389087006   |Hypoxemia (disorder)    |551               |180          |
    # |d     |           |386661006   |Fever (finding)         |79461             |67712        |
    # |d     |           |49727002    |Cough (finding)         |79461             |51636        |
    # |d     |           |36955009    |Loss of taste (finding) |79461             |38611        |
    # |d     |           |84229001    |Fatigue (finding)       |79461             |29241        |
    # |d     |           |248595008   |Sputum finding (finding)|79461             |25627        |
    # +------+-----------+------------+------------------------+------------------+-------------+

    # Save data as TSV files
    output_table_locations = {
        "symptom_comparisons": other_conditions_df,
    }

    for table_name, df in output_table_locations:
        save_as_tsv(df=df, file_path=table_name)


if __name__ == "__main__":
    main()
