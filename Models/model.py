from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, from_json, monotonically_increasing_id, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StandardScaler, StringIndexerModel
from confluent_kafka import Producer
import json
import traceback

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StudentDropoutModel") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_STATIC = "external_dataset"
TOPIC_DYNAMIC = "manual_dataset_with_target"
TOPIC_EVALUATION = "manual_dataset_without_target"
PREDICTED_TOPIC = "predicted_manual_dataset"

# Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})


def decode_data(df, with_identity=False):
    base_schema = StructType([
        StructField("Marital status", StringType()),
        StructField("Application mode", StringType()),
        StructField("Application order", StringType()),
        StructField("Course", StringType()),
        StructField("Daytime/evening attendance", StringType()),
        StructField("Previous qualification", StringType()),
        StructField("Nacionality", StringType()),
        StructField("Mother's qualification", StringType()),
        StructField("Father's qualification", StringType()),
        StructField("Mother's occupation", StringType()),
        StructField("Father's occupation", StringType()),
        StructField("Displaced", StringType()),
        StructField("Educational special needs", StringType()),
        StructField("Debtor", StringType()),
        StructField("Tuition fees up to date", StringType()),
        StructField("Gender", StringType()),
        StructField("Scholarship holder", StringType()),
        StructField("Age at enrollment", StringType()),
        StructField("International", StringType()),
        StructField("Curricular units 1st sem (credited)", StringType()),
        StructField("Curricular units 1st sem (enrolled)", StringType()),
        StructField("Curricular units 1st sem (evaluations)", StringType()),
        StructField("Curricular units 1st sem (approved)", StringType()),
        StructField("Curricular units 1st sem (grade)", StringType()),
        StructField("Curricular units 1st sem (without evaluations)", StringType()),
        StructField("Curricular units 2nd sem (credited)", StringType()),
        StructField("Curricular units 2nd sem (enrolled)", StringType()),
        StructField("Curricular units 2nd sem (evaluations)", StringType()),
        StructField("Curricular units 2nd sem (approved)", StringType()),
        StructField("Curricular units 2nd sem (grade)", StringType()),
        StructField("Curricular units 2nd sem (without evaluations)", StringType()),
        StructField("Unemployment rate", StringType()),
        StructField("Inflation rate", StringType()),
        StructField("GDP", StringType()),
        StructField("Target", StringType())
    ])

    if with_identity:
        identity_fields = [
            StructField("Student ID", StringType()),
            StructField("First Name", StringType()),
            StructField("Last Name", StringType())
        ]
        full_schema = StructType(identity_fields + base_schema.fields)
    else:
        full_schema = base_schema

    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
                  .select(from_json(col("json"), full_schema).alias("data")) \
                  .select("data.*")

    int_cols = [
        'Marital status', 'Application mode', 'Application order', 'Course', 'Daytime/evening attendance',
        'Previous qualification', 'Nacionality', 'Mother\'s qualification', 'Father\'s qualification',
        'Mother\'s occupation', 'Father\'s occupation', 'Displaced', 'Educational special needs',
        'Debtor', 'Tuition fees up to date', 'Gender', 'Scholarship holder', 'Age at enrollment', 'International'
    ]

    float_cols = [
        'Curricular units 1st sem (credited)', 'Curricular units 1st sem (enrolled)',
        'Curricular units 1st sem (evaluations)', 'Curricular units 1st sem (approved)',
        'Curricular units 1st sem (grade)', 'Curricular units 1st sem (without evaluations)',
        'Curricular units 2nd sem (credited)', 'Curricular units 2nd sem (enrolled)',
        'Curricular units 2nd sem (evaluations)', 'Curricular units 2nd sem (approved)',
        'Curricular units 2nd sem (grade)', 'Curricular units 2nd sem (without evaluations)',
        'Unemployment rate', 'Inflation rate', 'GDP'
    ]

    for col_name in int_cols:
        parsed_df = parsed_df.withColumn(col_name, col(col_name).cast("int"))

    for col_name in float_cols:
        parsed_df = parsed_df.withColumn(col_name, col(col_name).cast("double"))

    return parsed_df

def preprocess(df, indexer_model=None, scaler_model=None, encoder_model=None, assembler=None, is_training=True):
    # Rename columns for consistency
    df = df.withColumnRenamed("Nacionality", "Nationality") \
           .withColumnRenamed("Mother's qualification", "Mother_qualification") \
           .withColumnRenamed("Father's qualification", "Father_qualification") \
           .withColumnRenamed("Mother's occupation", "Mother_occupation") \
           .withColumnRenamed("Father's occupation", "Father_occupation") \
           .withColumnRenamed("Age at enrollment", "Age")

    # Clean column names (remove spaces and parentheses)
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").replace("(", "").replace(")", "")
        if new_col_name != col_name:
            df = df.withColumnRenamed(col_name, new_col_name)

    # Drop irrelevant columns
    for drop_col in ["Nationality", "International", "Educational_special_needs"]:
        if drop_col in df.columns:
            df = df.drop(drop_col)

    # Handle label encoding if Target column exists
    if "Target" in df.columns:
        labels = ["Dropout", "Enrolled", "Graduate"]
        indexer_model = StringIndexerModel.from_labels(labels, inputCol="Target", outputCol="Target_index").setHandleInvalid("keep")
        df = indexer_model.transform(df).drop("Target")
        df = df.filter(df["Target_index"] != 1.0)
        df = df.withColumn("label", when(df["Target_index"] == 2.0, 1.0).otherwise(0.0))
    elif is_training:
        raise ValueError("Training data must contain 'Target' column.")
    else:
        df = df.withColumn("label", lit(0.0))  # Placeholder label for prediction

    # Feature engineering (averaging semesters)
    df = df.withColumn("avg_credited", (col("Curricular_units_1st_sem_credited") + col("Curricular_units_2nd_sem_credited")) / 2) \
           .withColumn("avg_evaluations", (col("Curricular_units_1st_sem_evaluations") + col("Curricular_units_2nd_sem_evaluations")) / 2) \
           .withColumn("avg_approved", (col("Curricular_units_1st_sem_approved") + col("Curricular_units_2nd_sem_approved")) / 2) \
           .withColumn("avg_without_evaluations", (col("Curricular_units_1st_sem_without_evaluations") + col("Curricular_units_2nd_sem_without_evaluations")) / 2)

    # Drop original features
    df = df.drop(*[c for c in df.columns if "Curricular_units" in c or c in ["Unemployment_rate", "Inflation_rate", "GDP"]])

    # Categorical and numerical columns
    categorical = ['Marital_status', 'Application_mode', 'Course', 'Daytime/evening_attendance', 'Previous_qualification',
                   'Mother_qualification', 'Father_qualification', 'Mother_occupation', 'Father_occupation',
                   'Displaced', 'Debtor', 'Tuition_fees_up_to_date', 'Gender', 'Scholarship_holder']

    numeric = ["Age", "Application_order", "avg_credited", "avg_evaluations", "avg_approved", "avg_without_evaluations"]

    print("🚨 Null count check before assembling:")
    df.select(*[(col(c).isNull().cast("int").alias(c)) for c in categorical + numeric]).groupBy().sum().show()
    
    # Fit or use encoder
    if is_training:
        encoder_model = OneHotEncoder(
            inputCols=categorical,
            outputCols=[f"{col}_vec" for col in categorical],
            handleInvalid="keep"
        ).fit(df)
    df = encoder_model.transform(df)

    # Fit or use assembler
    if is_training:
        assembler = VectorAssembler(
            inputCols=[f"{c}_vec" for c in categorical] + numeric,
            outputCol="features"
        )
    df = assembler.transform(df)

    # Feature count check (optional)
    sample_features = df.select("features").first()["features"]
    print(f"✅ Number of features: {len(sample_features)}")

    # Fit or use scaler
    if is_training:
        scaler_model = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True).fit(df)
    df = scaler_model.transform(df)

    return df.select("scaledFeatures", "label"), indexer_model, scaler_model, encoder_model, assembler


def train_model(df):
    lr = LogisticRegression(
        featuresCol="scaledFeatures",
        labelCol="label",  # ✅ Use the correct column created in preprocess()
        maxIter=100
    )
    return lr.fit(df)

# === Train on static data ===
def train_initial_model():
    static_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_STATIC) \
        .option("startingOffsets", "earliest") \
        .load()

    decoded = decode_data(static_df, with_identity=False)
    train_df, indexer_model, scaler_model, encoder_model, assembler = preprocess(decoded, is_training=True)
    global model
    model = train_model(train_df)


# === Check for retraining ===
def check_for_new_labeled_data():
    # Read new labeled data from Kafka
    labeled_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_DYNAMIC) \
        .option("startingOffsets", "earliest") \
        .load()

    decoded_new = decode_data(labeled_df, with_identity=True)

    if decoded_new.filter((col("Target").isNotNull()) & (col("Target") != "")).count() > 0:
        print("🔁 New labeled data found, retraining model...")

        # Remove identity columns
        new_features_df = decoded_new.drop("Student ID", "First Name", "Last Name")

        # === Load static data again ===
        static_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC_STATIC) \
            .option("startingOffsets", "earliest") \
            .load()

        decoded_static = decode_data(static_df, with_identity=False)

        # === Combine static + new labeled ===
        combined_df = decoded_static.unionByName(new_features_df)

        # Preprocess combined data from scratch
        processed_df, new_indexer, new_scaler, new_encoder, new_assembler = preprocess(combined_df, is_training=True)

        # Retrain model
        global model, indexer_model, scaler_model, encoder_model, assembler
        model = train_model(processed_df)

        # Save updated transformers
        indexer_model = new_indexer
        scaler_model = new_scaler
        encoder_model = new_encoder
        assembler = new_assembler

        print("✅ Model retrained using static + new labeled data.")
    else:
        print("ℹ️ No new labeled data to retrain.")

# === Predict new unlabeled data ===
def predict_new_student_data():
    print("🔍 Checking for new student data for prediction...")

    try:
        eval_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC_EVALUATION) \
            .option("startingOffsets", "earliest") \
            .load()

        decoded_df = decode_data(eval_df, with_identity=True)

        if decoded_df.count() == 0:
            print("ℹ️ No new evaluation data found.")
            return

        # Split identity and features
        identity_df = decoded_df.select("Student ID", "First Name", "Last Name")
        features_df = decoded_df.drop("Student ID", "First Name", "Last Name")

        # Preprocess and predict
        processed_df, _, _, _, _ = preprocess(features_df,indexer_model=indexer_model,scaler_model=scaler_model,encoder_model=encoder_model,assembler=assembler,is_training=False)

        predictions = model.transform(processed_df)

        # Add dropout probability
        extract_dropout_prob = udf(lambda prob: float(prob[1]), FloatType())
        predictions = predictions.withColumn("dropout_probability", extract_dropout_prob(col("probability")))

        # Zip identity and predictions row-by-row
        combined_rows = identity_df.collect()
        prediction_rows = predictions.select("dropout_probability").collect()

        for identity, prediction in zip(combined_rows, prediction_rows):
            result = {
                "student_id": identity["Student ID"],
                "first_name": identity["First Name"],
                "last_name": identity["Last Name"],
                "dropout_probability": prediction["dropout_probability"]
            }
            producer.produce(topic=PREDICTED_TOPIC, value=json.dumps(result).encode('utf-8'))

        producer.flush()
        print(f"📤 Sent {len(prediction_rows)} predictions to Kafka.")

    except Exception as e:
        print("❌ Error during prediction:")
        traceback.print_exc()

