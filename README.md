# 🍺 ABInBev | BEES - Senior Data Analyst Challenge

[![Databricks](https://img.shields.io/badge/Databricks-Medallion_Architecture-FF3621?style=flat&logo=databricks)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-Data_Processing-E25A1C?style=flat&logo=apache-spark)](https://spark.apache.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?style=flat&logo=powerbi)](https://powerbi.microsoft.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Storage-00ADD8?style=flat)](https://delta.io/)

> **Senior Data Analyst Challenge**: End-to-end sales analytics pipeline implementing Medallion Architecture (Bronze → Silver → Gold) with Databricks and Power BI visualization.

**Delivery Date**: December 2025

---

## 📋 Executive Summary

End-to-end sales analytics pipeline implementing:
- **Medallion Architecture** (Bronze → Silver → Gold) on Databricks
- **PySpark** for data transformation
- **Delta Lake** for ACID-compliant storage
- **Star Schema** modeling (1 fact + 3 dimension tables)
- **Power BI** interactive dashboard

---

## 🎯 Business Problem

Analyze sales data to track revenue performance against targets, identify top customers/products, and visualize trends by city and category.

---

## 🏗️ Architecture

**Medallion Pattern**: Bronze (raw data) → Silver (cleaned data) → Gold (star schema)

**Gold Layer Tables:**
- `fact_orders` - Sales transactions
- `dim_users` - Customers with targets
- `dim_items` - Products (beer, nab, soda)
- `dim_calendar` - Date dimension (Power BI)

---

## 📂 Project Structure

```
├── case_abinbev.pbix                 # Power BI Dashboard
└── notebooks/                        # PySpark notebooks (Databricks)
    ├── 01_bronze_ingestion.py        # Raw data ingestion
    ├── 02_silver_transformation.py   # Data cleaning & validation
    ├── 03_gold_modeling.py           # Star schema creation
    └── 04_data_quality_validation.py # Data quality checks
```

---

## 🔧 Pipeline Implementation

| Layer | Tables | Description |
|-------|--------|-------------|
| 🥉 **Bronze** | `orders`, `users`, `items`, `targets` | Raw Excel data with audit columns |
| 🥈 **Silver** | `orders`, `users`, `items` | Cleaned, validated, merged data |
| 🥇 **Gold** | `fact_orders`, `dim_users`, `dim_items` | Star schema for analytics |

---

## 📊 Power BI Dashboard

![ABInBev BEES Sales Performance Dashboard](https://github.com/user-attachments/assets/f8c5a8b0-3c5e-4d6e-9c8a-7f5b6d8e9f0a)

**Key Features:**
- 🎯 **KPIs**: Total Revenue, Target Revenue, Achievement(%), Average Ticket
- 📈 **Trend Analysis**: Monthly/YTD revenue comparison with toggle view
- 🏪 **Category Breakdown**: Revenue by item category (bar, restaurant, shop)
- 🌆 **Geographic Analysis**: Performance by city with target comparison
- ✅ **Interactive filtering** and cross-highlighting

---

## 📊 Data Model

**Star Schema:**
- **Fact**: `fact_orders` (order_id, order_date, user_id, product_id, revenue)
- **Dimensions**: 
  - `dim_users` (user_id, category, city, monthly_target)
  - `dim_items` (item_id, category)
  - `dim_calendar` (date, year, month, quarter, week)

**Relationships**: fact_orders → dim_users/dim_items/dim_calendar (Many-to-One)

---

## 🛠️ Tech Stack

**Data Engineering**: Databricks Community Edition | PySpark | Delta Lake  
**Analytics**: Star Schema | Power BI Desktop  
**Version Control**: Git & GitHub

---

## ✅ Key Highlights

- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ Data quality validation at each layer
- ✅ ACID-compliant Delta Lake storage
- ✅ Star schema optimized for BI queries
- ✅ Interactive dashboard with target tracking
- ✅ Modular, well-documented code

---

**ABInBev | BEES - Senior Data Analyst Challenge** | December 2025