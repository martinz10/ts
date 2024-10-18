# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from bs4 import BeautifulSoup

# init spark session
spark = SparkSession.builder.appName("SparkExam").getOrCreate()

# %%
df = spark.read.json("/opt/spark-app/data/marketing_sample_for_careerbuilder_usa-careerbuilder_job_listing__20200401_20200630__30k_data.ldjson")
df.show(20, truncate=False)
df.printSchema()


# %%
# calculate number of jobs posted on daily basis, per each city
daily_jobs_per_city = df.groupBy("city", "post_date").count().orderBy("count", ascending=False)


# %%


# %%


# calculate average salary per job title and state

# function removes all letters except "hour", "year", "yearly", "mile", "week", "weekly", "month", "monthly", "day", "daily"
def clean_salary_column(salary_column):
    return F.regexp_replace(
        salary_column,
        r"[a-zA-Z$,!?/\\€£&](?!hour|year|yearly|mile|week|weekly|month|monthly|day|daily)",  # key words
        ""
    )

# clean the salary_offered column, remove unnecessary characters except keywords
df_cleaned = df.withColumn(
    "salary_offered_cleaned",
    F.when(
        F.col("salary_offered").rlike(r"^[a-zA-Z\s]*$"),
        None
    ).otherwise(
        clean_salary_column(F.col("salary_offered"))  # Čišćenje salary vrednosti
    )
)

# transform hourly, weekly, monthly, daily wages to yearly, mileage to yearly, and handle salary ranges
df_transformed = df_cleaned.withColumn(
    "salary_offered_cleaned",
    F.when(
        # handle hourly wages, convert to yearly (2080 working hours/year)
        F.col("salary_offered").rlike(r"(?i)(hour|an hour|per hour)"),
        ((F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float") +
          F.when(F.col("salary_offered_cleaned").contains("-"),
                 F.regexp_extract(F.col("salary_offered_cleaned"), r"- (\d+\.?\d*)", 1).cast("float"))
          .otherwise(F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float"))) / 2 * 2080)
    ).when(
        # handle weekly wages, convert to yearly (52 weeks/year)
        F.col("salary_offered").rlike(r"(?i)(week|weekly)"),
        ((F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float") +
          F.when(F.col("salary_offered_cleaned").contains("-"),
                 F.regexp_extract(F.col("salary_offered_cleaned"), r"- (\d+\.?\d*)", 1).cast("float"))
          .otherwise(F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float"))) / 2 * 52)
    ).when(
        # handle monthly wages, convert to yearly (12 months/year)
        F.col("salary_offered").rlike(r"(?i)(month|monthly)"),
        ((F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float") +
          F.when(F.col("salary_offered_cleaned").contains("-"),
                 F.regexp_extract(F.col("salary_offered_cleaned"), r"- (\d+\.?\d*)", 1).cast("float"))
          .otherwise(F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float"))) / 2 * 12)
    ).when(
        # handle daily wages, convert to yearly (260 working days/year)
        F.col("salary_offered").rlike(r"(?i)(day|daily)"),
        ((F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float") +
          F.when(F.col("salary_offered_cleaned").contains("-"),
                 F.regexp_extract(F.col("salary_offered_cleaned"), r"- (\d+\.?\d*)", 1).cast("float"))
          .otherwise(F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float"))) / 2 * 260)
    ).when(
        # handle mileage rates, 50,000 miles/year
        F.col("salary_offered").rlike(r"(?i)(mile)"),
        ((F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float") +
          F.when(F.col("salary_offered_cleaned").contains("-"),
                 F.regexp_extract(F.col("salary_offered_cleaned"), r"- (\d+\.?\d*)", 1).cast("float"))
          .otherwise(F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float"))) / 2 * 50000)
    ).when(
        # handle yearly salaries and salary ranges
        F.col("salary_offered").rlike(r"(?i)(year|yearly)"),
        F.when(F.col("salary_offered_cleaned").contains("-"),
               (F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float") +
                F.regexp_extract(F.col("salary_offered_cleaned"), r"- (\d+\.?\d*)", 1).cast("float")) / 2)
        .otherwise(F.regexp_extract(F.col("salary_offered_cleaned"), r"(\d+\.?\d*)", 0).cast("float"))
    ).otherwise(F.col("salary_offered_cleaned"))
)

# handle '0' values by converting them to None
df_final = df_transformed.withColumn(
    "salary_offered_cleaned",
    F.when(F.col("salary_offered_cleaned") == "0", None).otherwise(F.col("salary_offered_cleaned"))
)

# group by 'job_title' and 'state' to calculate average salary
df_avg_salary = df_final.groupBy("job_title", "state") \
    .agg(F.avg("salary_offered_cleaned").alias("average_salary"))
# display the results (including original and cleaned salary values)
df_final.select("job_title", "state", "salary_offered", "salary_offered_cleaned").show(truncate=False)

# group by 'job_title' and 'state' to calculate average salary
df_avg_salary = df_final.groupBy("job_title", "state") \
    .agg(F.avg("salary_offered_cleaned").alias("average_salary"))





# %%
# identify the top 10 most active companies by number of positions opened
top_companies = df.groupBy("company_name").count().orderBy("count", ascending=False).limit(10)

# %%
daily_jobs_per_city.toPandas().to_csv('/opt/spark-app/output/daily_jobs_per_city.csv', index=False)
df_avg_salary.toPandas().to_csv('/opt/spark-app/output/avg_salary_per_job_title.csv', index=False)
top_companies.toPandas().to_csv('/opt/spark-app/output/top_companies.csv', index=False)

# %%
# create a UDF function to clean job description from HTML code contained inside
def clean_html(raw_html):
    if raw_html is None:
        return None
    else:
        return BeautifulSoup(raw_html, "html.parser").get_text()

# regiter our function like udf
clean_html_udf = F.udf(clean_html, StringType())

# udf on job_description
df_cleaned = df.withColumn(
    "cleaned_job_description", 
    clean_html_udf(F.col("job_description"))
)

# results
df_cleaned.select("job_description", "cleaned_job_description").show(truncate=False)
df_to_export = df_cleaned.select("job_description", "cleaned_job_description")
# convert to pandas
df_pandas = df_to_export.toPandas()
# output to csv
df_pandas.to_csv('/opt/spark-app/output/job_descriptions_comparison.csv', index=False)
# done print
print("CSV saved 'job_descriptions_comparison.csv'")

# %%
spark.stop()


