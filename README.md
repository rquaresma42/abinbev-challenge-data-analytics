# 🍺 ABInBev | BEES - Data Engineering Challenge

**Candidate**: Rafael  
**Delivery Date**: December 2025  
**Challenge**: Sales Analytics Pipeline + Dashboard

---

## 📋 Executive Summary

This solution implements a complete **end-to-end data pipeline** for ABInBev sales analysis, using:

- **Databricks** (Medallion Architecture: Bronze → Silver → Gold)
- **PySpark** (data transformation and modeling)
- **Delta Lake** (storage format)
- **Power BI** (interactive dashboard)

### ✅ Deliverables

1. **4 Databricks notebooks** implementing the data pipeline
2. **Star Schema** with 1 fact table and 2 dimension tables
3. **Power BI dashboard** with sales performance vs targets
4. **Complete documentation** (this file)

---

## 🎯 Business Problem Solved

Analyze ABInBev sales data to:
- Track revenue performance against customer targets
- Identify top-performing customers and products
- Visualize trends by city, customer category, and product type
- Support data-driven decision making

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

### Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Databricks Community Edition** | Cost-effective cloud platform for processing |
| **Delta Lake format** | ACID transactions, time travel, schema evolution |
| **Star Schema** | Optimized for analytics queries and BI tools |
| **Merged users + targets** | Single source of truth for customer data |
| **Calendar in Power BI** | Flexibility for date hierarchies without SQL Warehouse |

### Data Model (Star Schema)

```
       dim_users (8 customers)
           |
           | (user_id)
           |
     fact_orders (100+ transactions)
           |
           | (product_id)
           |
       dim_items (10 products)
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
├── .env                              # Credentials (do not commit)
├── .env.example                      # Configuration template
├── .gitignore                        # Files to ignore
└── notebooks/                        # Databricks notebooks (executed in Databricks)
    ├── 01_bronze_ingestion.py        # Creates bronze_raw_* tables
    ├── 02_silver_transformation.py   # Creates silver_* tables
    ├── 03_gold_modeling.py           # Creates fact_orders, dim_users, dim_items
    └── 04_data_quality_validation.py # Validates Gold layer integrity
```

**Note**: The `notebooks/` folder contains PySpark code that must be executed in **Databricks workspace**. These notebooks generate the dimensional model (fact and dimension tables) consumed by Power BI.

---

## 🔍 For Reviewers: How to Validate This Solution

### Option 1: Review Code & Documentation (Recommended)

**What to check:**
1. **Notebooks** (`/notebooks/`) - Well-documented PySpark code with clear transformations
2. **This README** - Complete architecture and decision documentation
3. **Data model** - Star Schema design rationale
4. **Power BI** - Dashboard file (.pbix) with insights

**No setup needed** - Just review the code quality and approach.

---

### Option 2: Reproduce the Pipeline (Optional)

If you want to test the solution:

#### Prerequisites

- Databricks Community Edition account (free)
- Power BI Desktop

#### Steps

1. **Setup Databricks:**
   - Upload source Excel files (orders, users, items, targets)
   - Import the 4 notebooks from `/notebooks/` folder
   - Execute notebooks in order (01 → 02 → 03 → 04)

2. **Connect Power BI:**
   - Copy `.env.example` to `.env`
   - Add your Databricks credentials:
     ```env
     DATABRICKS_HOST=your-workspace.cloud.databricks.com
     DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
     DATABRICKS_TOKEN=your-token-here
     ```
   - Open Power BI Desktop
   - Get Data → Databricks connector
   - Import tables: `fact_orders`, `dim_users`, `dim_items`
   - Load the calendar dimension using `dim_calendar.pq`

3. **Explore Dashboard:**
   - Review sales metrics vs targets
   - Analyze customer and product performance
   - Check data quality validations

### 3️⃣ Run Databricks Pipeline

**Important**: These notebooks must be executed **inside Databricks workspace** to create the Gold layer tables.

#### Upload Notebooks to Databricks

1. Go to **Workspace** in Databricks
2. Navigate to `abinbev` folder
3. Import the 4 notebooks from `Challenge/notebooks/`
4. Execute in order:

```
1. 01_bronze_ingestion.py        → Creates bronze_raw_* tables
2. 02_silver_transformation.py   → Creates silver_* tables  
3. 03_gold_modeling.py           → Creates fact_orders, dim_users, dim_items
4. 04_data_quality_validation.py → Validates data quality (optional)
```

#### Expected Output

After running all notebooks, you'll have these tables in `abinbev.default` catalog:

**Gold Layer (ready for Power BI)**:
- ✅ `fact_orders` - Transactional sales data
- ✅ `dim_users` - Customer dimension with targets
- ✅ `dim_items` - Product dimension

### 4️⃣ Consume in Power BI

#### Using Databricks Connector (Import Mode) ✅

```
Power BI Desktop
→ Get Data → More → Search "Databricks"
→ Select "Databricks" (not Azure Databricks)
→ Server hostname: community.cloud.databricks.com
→ HTTP path: /sql/1.0/warehouses/[your-warehouse-id]
→ Authentication: Personal Access Token
→ Paste your token from .env file
→ Select tables:
   - abinbev.default.fact_orders
   - abinbev.default.dim_users
   - abinbev.default.dim_items
→ Data Connectivity Mode: Import
→ Load
```

**Note**: This method connects directly to Databricks tables using your Personal Access Token for authentication.

### 5️⃣ Create Calendar Dimension & Configure Relationships

#### Create Calendar Table in Power BI

Use the provided `dim_calendar.pq` file or create using DAX/Power Query with the following columns:
- date, year, month, quarter, week_of_year, day_of_week, month_name, day_name, year_month, year_week

#### Configure Relationships in Model View

```
fact_orders[user_id]     → dim_users[user_id]      (N:1, Single)
fact_orders[product_id]  → dim_items[item_id]      (N:1, Single)
fact_orders[order_date]  → dim_calendar[date]      (N:1, Single)
```

---

## 📊 Data Model

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

---

## 💡 Key Insights & Results

### Business Metrics Implemented

| Metric | Description | Power BI Implementation |
|--------|-------------|------------------------|
| **Total Revenue** | Sum of all orders | `SUM(fact_orders[revenue])` |
| **Achievement %** | Revenue vs annual targets | `DIVIDE([Revenue], [Target] * 12)` |
| **Avg Ticket** | Revenue per order | `DIVIDE([Revenue], COUNT(orders))` |
| **Customer Ranking** | Top performers | Table with revenue + target |
| **Product Mix** | Revenue by category | Bar chart (beer/nab/soda) |
| **Geographic Analysis** | Revenue by city | Map visual |

### Expected Insights (Based on Data)

- **Customer Performance**: 8 customers across 3 categories (bar, restaurant, shop) in 3 cities
- **Target Tracking**: Monthly targets aggregated to annual for comparison
- **Product Analysis**: Revenue distribution across beer, NAB (non-alcoholic), and soda
- **Trend Analysis**: Monthly sales patterns for 2024

---

## 🎨 Dashboard Design

The Power BI dashboard includes:

### Main Visuals
- 📊 **KPI Cards**: Revenue, Target, Achievement %, Avg Ticket
- 📈 **Trend Chart**: Monthly revenue with target line
- 🗺️ **Map**: Geographic revenue distribution
- 📦 **Category Breakdown**: Product and customer segmentation
- 📋 **Top Customers Table**: Ranking with targets and achievement

### Interactive Features
- Filters by date, city, customer category, product type
- Drill-through for customer details
- Cross-highlighting between visuals

---

## 🛠️ Technical Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Cloud Platform** | Databricks Community Edition | Data processing and storage |
| **Processing Engine** | PySpark | Distributed data transformation |
| **Storage Format** | Delta Lake | ACID-compliant data lake |
| **Data Modeling** | Star Schema | Optimized for analytics |
| **Visualization** | Power BI Desktop | Interactive dashboard |
| **Version Control** | Git | Code versioning (recommended) |

---

## ✅ Solution Highlights

### What Makes This Solution Production-Ready

1. **Scalable Architecture**: Medallion pattern allows incremental improvements
2. **Data Quality**: Built-in validations at each layer (Bronze → Silver → Gold)
3. **Maintainability**: Modular notebooks, clear documentation, standard naming
4. **Performance**: Delta Lake optimization, partitioning strategy ready
5. **Best Practices**: 
   - Separate raw/cleaned/modeled layers
   - Single source of truth (merged targets)
   - Star schema for fast queries
   - Proper error handling

---

## 📞 Contact & Questions

For any questions about this solution:
- **Candidate**: Rafael
- **Delivery**: December 2025
- **Repository**: [If using GitHub, add link here]

---

## 📚 References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Kimball Star Schema Design](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Power BI Best Practices](https://learn.microsoft.com/en-us/power-bi/)

---

**Thank you for reviewing this solution!** 🚀
