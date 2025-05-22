# Student Dropout Prediction System

A Big Data-based system for early identification of students at risk of dropping out. 
The project includes a user friendly interface, real time processing, and machine learning based prediction.

## 📌 Project Overview

Student retention is a critical challenge in higher education. Dropping out affects both students (emotionally, financially, and professionally) and institutions (reputation, funding, and academic success).

This project aims to detect students at risk of dropping out using advanced data-driven technologies. The system enables institutions to intervene early, personalize support, and enhance student success.

## 🛠 Technologies & Tools

- **Apache Spark + Spark ML** – for parallel data processing and training a Logistic Regression model.
- **Apache Kafka** – for real-time communication between system components.
- **Docker** – for deploying Kafka and Zookeeper services.
- **PyQt6** – for the user interface.
- **Python** – for scripting, data processing, and ML pipeline.
- **Pandas, Scikit-learn, PySpark** – for data analysis and preprocessing.
- **VSCode** – development environment.

## 📁 Dataset Overview

The dataset includes information about:
- Demographics (gender, age, nationality)
- Academic performance (grades, course enrollments, scholarship)
- Socioeconomic background (parents' occupation, financial status)
- Economic indicators (unemployment, inflation, GDP)

> Source: [Kaggle – Undergraduate student dataset with 4,425 records and 35 features](https://www.kaggle.com/datasets/thedevastator/higher-education-predictors-of-student-retention)


## 📈 Model Building Process

1. Data cleaning and feature encoding
2. Chi-square tests and correlation analysis
3. Binary classification setup: `Dropout = 0.0`, `Graduate = 1.0`
4. Logistic Regression training with One-Hot Encoding and normalization
5. Evaluation metrics:
   - **Accuracy**: 90.27%
   - **Recall**: 95.20%
   - **Precision**: 89.82%
   - **F1 Score**: 92.43%
   - **AUC**: 0.93

## 💻 User Interface (UI)

The graphical interface contains two main screens:
- **Student Insights** – Predict dropout risk by student ID
- **Add Student Data** – Manually input or upload a CSV file

All data transfer between components is handled via Kafka topics.

## 📦 Key Project Files

- `Main.py` – Entry point of the system
- `Models/model.py` – Data processing, training, and prediction logic
- `Models/producer1.py` – Sends initial static dataset to Kafka
- `Views/dashboard.py` – PyQt6-based user dashboard
- `StudentsDataset.csv` – Original labeled dataset
- `fake_data_upd.csv` – Test input data (with or without target labels)
- `docker-compose.yml` – Launches Kafka and Zookeeper containers
- `final_project_bigdata.ipynb` – Early-stage analysis and model prototyping

## ▶️ How to Run the Project

### Prerequisites

- Docker installed (to run Kafka and Zookeeper)
- Python 3.10+
- Java 8+
- Installing required libraries via pip: pyspark, kafka-python, PyQt6, pandas

Once all prerequisites are in place, follow these steps:

1. Run Kafka and Zookeeper via Docker:
    ```bash
    docker-compose up
2. Run the main application:
   ```bash
    python3 Main.py
⚠️ Before running, make sure the file paths in the following scripts are properly adjusted to your local environment (i.e., the actual directory structure on your personal machine). Otherwise, the system may not function correctly:

- Main.py
- Models/producer1.py
- Views/dashboard.py

## 📓 Jupyter Notebook
A Jupyter notebook (final_project_bigdata.ipynb) is included in this project. It contains initial data exploration, feature engineering, and model prototyping for student dropout prediction using PySpark and logistic regression.

