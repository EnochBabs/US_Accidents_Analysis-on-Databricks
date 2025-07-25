# US_Accidents_Analysis-on-Databricks

**Got it! Here’s your `README.md` rewritten in a **clean, professional GitHub format** (with proper headings, emojis, and spacing).**
You can copy-paste this directly into your repo.

---

# **US Accidents ETL Pipeline (Databricks Lakehouse)**

![Databricks](https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png)

---

## **Project Overview**

This project demonstrates how to build a **production-ready ETL pipeline** using **Databricks (PySpark + Delta Lake)**, following the **Lakehouse architecture**.

The pipeline processes the **US Accidents dataset (\~3M rows)** and transforms it through three layers:

* **Bronze Layer** → Raw ingestion (CSV files as-is)
* **Silver Layer** → Cleaned, curated Delta table
* **Gold Layer** → Business-ready aggregated insights for dashboards

---

## **Architecture**

```
Raw CSV (DBFS) → Bronze (Raw Delta) → Silver (Clean Delta) → Gold (Aggregations)
```

**Technologies Used**

* **PySpark** → Distributed data processing
* **Delta Lake** → ACID transactions, schema evolution, time travel
* **Databricks Lakehouse** → Unified data & analytics platform

---

## **Dataset**

* **Source**: [US Accidents (March 2023)](https://www.kaggle.com/sobhanmoosavi/us-accidents)
* **Size**: \~5.7M rows, 50+ columns
* **Key Columns**:

  * `ID` → Unique accident identifier
  * `Start_Time`, `End_Time` → Timestamps of accidents
  * `Severity` → Accident severity (1–4)
  * `State`, `City` → Location
  * `Weather_Condition`, `Temperature(F)` → Weather impact

---

## **Pipeline Steps**

### **1. Bronze Layer – Raw Ingestion**

✔ Ingested chunked CSV files (\~3GB) into **DBFS** (`/FileStore/tables`).
✔ Stored as raw Delta for traceability.

```python
bronze_df = spark.read.csv("/FileStore/tables/US_Accidents_March23_part*.csv", header=True, inferSchema=True)
```

---

### **2. Silver Layer – Cleaning & Curation**

✔ **Data Quality Rules**:

* Removed duplicates (`ID`)
* Dropped nulls in critical fields (`Start_Time`, `Severity`, `City`)
* Casted timestamps & numeric columns
* Filtered invalid GPS values

✔ Saved as Delta:

```python
silver_df.write.format("delta").mode("overwrite").save("/FileStore/tables/silver/us_accidents")
```

---

### **3. Gold Layer – Business Aggregations**

Generated **3 key Gold tables**:

1. **Accidents per State**

   ```python
   silver_df.groupBy("State").count()
   ```
2. **Average Severity by Weather**
3. **Hourly Accident Trend (rush-hour analysis)**

Sample hourly trend:

| Accident\_Hour | Total\_Accidents    |
| -------------- | ------------------- |
| 7              | High (Morning rush) |
| 17             | Peak (Evening rush) |
| 2              | Low (Late night)    |

**Business Use Cases**:

* Traffic management → Deploy patrols during peak hours
* Insurance analytics → Identify high-risk weather conditions
* City planning → Focus on high-accident states

---

## **How to Run**

1. Upload dataset parts to **DBFS** (`/FileStore/tables`).
2. Open `bronze_silver_gold.ipynb` in Databricks.
3. Run sequentially: **Bronze → Silver → Gold**.
4. (Optional) Visualize Gold tables using Databricks’ built-in charts or connect to **Power BI/Tableau**.
