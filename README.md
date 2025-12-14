# 🍺 ABInBev | BEES - Data Engineering Challenge

[![Databricks](https://img.shields.io/badge/Databricks-Medallion_Architecture-FF3621?style=flat&logo=databricks)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-Data_Processing-E25A1C?style=flat&logo=apache-spark)](https://spark.apache.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?style=flat&logo=powerbi)](https://powerbi.microsoft.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Storage-00ADD8?style=flat)](https://delta.io/)

> **Data Engineering Challenge**: End-to-end sales analytics pipeline implementing Medallion Architecture (Bronze → Silver → Gold) with Databricks and Power BI visualization.

**Delivery Date**: December 2025

---

## 📋 Executive Summary

This solution implements a complete **end-to-end data pipeline** for ABInBev | BEES sales analysis, using:

- **Databricks** (Medallion Architecture: Bronze → Silver → Gold)
- **PySpark** (data transformation and modeling)
- **Delta Lake** (ACID-compliant storage format)
- **Power BI** (interactive dashboard)

### ✅ Project Deliverables

1. **4 Databricks notebooks** implementing the data pipeline
2. **Star Schema** with 1 fact table and 2 dimension tables
3. **Power BI dashboard** with sales performance vs targets
4. **Complete documentation** (this file)

---

## 🎯 Business Problem

The solution analyzes ABInBev | BEES sales data to:
- Track revenue performance against customer targets
- Identify top-performing customers and products
- Visualize trends by city, customer category, and product type
- Support data-driven decision making through interactive dashboards

---

## 🏗️ Technical Solution

### Architecture: Medallion (Bronze → Silver → Gold)

```
📊 Excel Files (orders, users, items, targets)
    ↓ [Upload to Databricks]
🥉 BRONZE Layer - Raw ingestion
    ↓ [Data cleaning + validation]
🥈 SILVER Layer - Cleaned data
    ↓ [Star Schema modeling]
🥇 GOLD Layer - Dimensional model
    ↓ [Databricks Connector]
📊 Power BI Dashboard
```

**Gold Layer Output:**
- ✅ `fact_orders` - Transactional sales data (order_id, order_date, user_id, product_id, revenue)
- ✅ `dim_users` - Customers with categories, cities, and monthly revenue targets
- ✅ `dim_items` - Products categorized as beer, nab, or soda
- ✅ `dim_calendar` - Date dimension (created in Power BI using Power Query M)

---

## 📂 Project Structure

```
Challenge/
├── README.md                         # This documentation
├── .gitignore                        # Files to ignore
├── case_abinbev.pbix                 # Power BI Dashboard
└── notebooks/                        # Databricks notebooks (executed in Databricks)
    ├── 01_bronze_ingestion.py        # Creates abinbev.bronze.* tables
    ├── 02_silver_transformation.py   # Creates abinbev.silver.* tables
    ├── 03_gold_modeling.py           # Creates abinbev.gold.fact_orders, dim_users, dim_items
    └── 04_data_quality_validation.py # Validates Gold layer integrity
```

---

## 🔧 Implementation Details

### Data Pipeline Notebooks

Four PySpark notebooks were developed and executed in Databricks:

1. **01_bronze_ingestion.py** - Ingests raw Excel files into Delta tables with audit columns
2. **02_silver_transformation.py** - Cleans data, validates business rules, and merges users with targets
3. **03_gold_modeling.py** - Creates star schema with fact and dimension tables
4. **04_data_quality_validation.py** - Validates referential integrity and data quality

### Pipeline Results

| Layer | Tables Created | Records |
|-------|----------------|----------|
| 🥉 **Bronze** | `orders`, `users`, `items`, `targets` | Raw data as-is |
| 🥈 **Silver** | `orders`, `users`, `items` | Cleaned & validated |
| 🥇 **Gold** | `fact_orders`, `dim_users`, `dim_items` | Star schema ready |

---

## 📊 Power BI Dashboard

### Dashboard Structure

The dashboard is organized into the following sections:

#### 🎯 Key Metrics (Top Cards)
- **Total Revenue** - Aggregated sales from all transactions
- **Revenue Achievement** - Performance percentage against targets
- **Average Ticket** - Average revenue per order

#### 📈 Revenue Over Time (Central Area)
- **Area Chart** with dual series (YTD Revenue vs. Monthly Revenue)
- **Time Axis** showing monthly progression
- **Toggle Buttons** to switch between Monthly Revenue and YTD Revenue views
- **Data Labels** displaying values at key points

#### 🏪 User Category Analysis (Right Panel - Top)
- **Horizontal Bar Chart** comparing Total Revenue vs. Total Target
- **Categories**: Bar, Restaurant, Shop
- **Dual-color bars** (blue for revenue, green for target)

#### 🌆 User City Analysis (Right Panel - Bottom)
- **Horizontal Bar Chart** showing performance by geographic location
- **Cities**: Campinas, Rio de Janeiro, São Paulo
- **Target vs. Actual comparison** with overlaid bars

### Dashboard Features
- ✅ **Interactive visuals** with cross-filtering capabilities
- ✅ **Target vs. Actual comparison** across all dimensions
- ✅ **Time-based navigation** for trend analysis
- ✅ **Geographic and categorical segmentation**

---

## 📊 Data Model (Star Schema)

The solution implements a star schema optimized for analytical queries:

### Fact Table: `fact_orders`

| Column | Type | Description |
|--------|------|-----------|
| order_id | Long | Unique order ID |
| order_date | Date | Order date |
| user_id | Long | FK → dim_users |
| product_id | Long | FK → dim_items |
| revenue | Double | Order revenue |
| year | Integer | Derived year |
| month | Integer | Derived month |

### Dimension: `dim_users`

| Column | Type | Description |
|--------|------|-----------|
| user_id | Long | PK - Customer ID |
| category | String | bar / restaurant / shop |
| city | String | City (São Paulo, Rio de Janeiro, Campinas) |
| monthly_revenue_target | Double | Monthly revenue target |

### Dimension: `dim_items`

| Column | Type | Description |
|--------|------|-----------|
| item_id | Long | PK - Product ID |
| category | String | beer / nab / soda |

### Dimension: `dim_calendar` (Power BI)

**Created in Power BI** using Power Query M (`dim_calendar.pq`):

| Column | Type | Description |
|--------|------|-----------|
| date | Date | PK - Date |
| year | Integer | Year |
| month | Integer | Month (1-12) |
| quarter | Integer | Quarter (1-4) |
| week_of_year | Integer | Week of year |
| day_of_week | Integer | Day of week |
| month_name | String | Month name |
| day_name | String | Day name |
| year_month | String | YYYY/MM |
| year_week | String | YYYY/WW |

### Relationships Implemented

```
fact_orders[user_id]     → dim_users[user_id]     (Many-to-One)
fact_orders[product_id]  → dim_items[item_id]     (Many-to-One)
fact_orders[order_date]  → dim_calendar[date]     (Many-to-One)
```

---

## 🛠️ Technologies Used

| Component | Technology |
|-----------|------------|
| **Cloud Platform** | Databricks Free Edition |
| **Processing** | PySpark |
| **Storage** | Delta Lake |
| **Modeling** | Star Schema |
| **Visualization** | Power BI Desktop |
| **Version Control** | Git & GitHub |

---

## ✅ Solution Highlights

| Aspect | Implementation |
|--------|----------------|
| **Architecture** | Medallion pattern (Bronze → Silver → Gold) |
| **Data Quality** | Validation at each transformation layer |
| **Scalability** | Delta Lake with partitioning capability |
| **Performance** | Star schema optimized for BI queries |
| **Maintainability** | Modular notebooks with clear documentation |
| **Best Practices** | ACID transactions, schema evolution, audit columns |

---

## 📊 Key Business Metrics

| Metric | Implementation |
|--------|---------|
| **Total Revenue** | Aggregation of all order revenues |
| **Revenue Achievement** | Performance % against customer targets |
| **Average Ticket** | Average revenue per transaction |
| **Customer Segmentation** | Analysis by category (bar, restaurant, shop) |
| **Geographic Analysis** | Performance by city |
| **Trend Analysis** | Monthly revenue patterns and YTD comparison |

---

**Thank you for reviewing this solution!** 🚀