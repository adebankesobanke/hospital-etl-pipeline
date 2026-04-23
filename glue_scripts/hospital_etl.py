from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder.appName("final-model").getOrCreate()

# -----------------------
# LOAD DATA
# -----------------------
appointments = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/appointments/",
    header=True,
    inferSchema=True
)

patients = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/patients/",
    header=True,
    inferSchema=True
)

doctors = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/doctors/",
    header=True,
    inferSchema=True
)

branches = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/branches/",
    header=True,
    inferSchema=True
)

billings = spark.read.csv(
    "s3://hospital-etl-data-adebanke/raw/billings/",
    header=True,
    inferSchema=True
)

# -----------------------
# CLEAN DIMENSIONS
# -----------------------
patients_clean = patients.select(
    "patient_id",
    col("first_name").alias("patient_first_name"),
    col("last_name").alias("patient_last_name")
)

doctors_clean = doctors.select(
    "doctor_id",
    col("first_name").alias("doctor_first_name"),
    col("last_name").alias("doctor_last_name"),
    "specialization"
)

branches_clean = branches.select(
    "branch_id",
    "branch_name"
)

appointments_clean = appointments.select(
    "appointment_id",
    "patient_id",
    "doctor_id",
    "branch_id",
    "appointment_date",
    "status"
)

# -----------------------
# BUILD CORE MODEL
# -----------------------
base_df = appointments_clean \
    .join(patients_clean, "patient_id", "left") \
    .join(doctors_clean, "doctor_id", "left") \
    .join(branches_clean, "branch_id", "left")

# -----------------------
# FACT AGGREGATION
# -----------------------
billing_agg = billings.groupBy("patient_id").agg(
    sum("amount").alias("total_amount"),
    count("amount").alias("billing_count")
)

# -----------------------
# FINAL MODEL
# -----------------------
final_df = base_df.join(billing_agg, "patient_id", "left")

# -----------------------
# DATA QUALITY
# -----------------------
df_clean = final_df.filter(
    col("patient_id").isNotNull() &
    col("doctor_id").isNotNull() &
    col("appointment_id").isNotNull()
)

# -----------------------
# FINAL SCHEMA
# -----------------------
df_clean = df_clean.select(
    "appointment_id",
    "patient_id",
    "doctor_id",
    "branch_id",
    "appointment_date",
    "status",
    "patient_first_name",
    "patient_last_name",
    "doctor_first_name",
    "doctor_last_name",
    "specialization",
    "branch_name",
    "total_amount",
    "billing_count"
)

# -----------------------
# WRITE OUTPUT
# -----------------------
df_clean.write.mode("overwrite").parquet(
  "s3://hospital-etl-data-adebanke/curated/patient_revenue_summary/v2/"
)
