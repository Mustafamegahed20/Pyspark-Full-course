# Pyspark-Full-course

# Complete PySpark Master Class Notes

## Table of Contents
1. [Introduction to Apache Spark](#introduction)
2. [Spark Architecture](#architecture)
3. [Setup & Environment](#setup)
4. [Data Reading](#data-reading)
5. [Basic Transformations](#basic-transformations)
6. [Intermediate Transformations](#intermediate-transformations)
7. [Advanced Functions](#advanced-functions)
8. [Data Writing](#data-writing)
9. [SparkSQL](#sparksql)

---

## <a name="introduction"></a>1. Introduction to Apache Spark

### What is Apache Spark?

**Apache Spark** is a distributed computing engine that distributes data across multiple machines (cluster) to process it efficiently.

**Simple Definition:** Spark takes your data and splits it across multiple computers to process faster.

### Why Use Spark Over Single Machine?

**Option 1: Single Machine**
- Increase RAM, CPU, Storage
- Has limits (saturation point)

**Option 2: Cluster of Machines** ✅
- Multiple connected computers acting as one
- Scalable to infinite numbers
- No saturation point

```
Single Machine:     [1 Computer] → Limited Resources
Cluster:           [Computer1 + Computer2 + Computer3 + ...] → Unlimited Scaling
```

---

## <a name="architecture"></a>2. Spark Architecture

### Core Components

```
┌─────────────────────────────────────────────┐
│           Cluster Manager                    │
│  (Manages Resources & Creates Nodes)        │
└─────────────────────────────────────────────┘
                    ↓
        ┌───────────────────────┐
        │    Driver Node        │
        │  (Orchestrates Tasks) │
        └───────────────────────┘
                    ↓
        ┌───────────────────────┐
        │   Worker Nodes        │
        │  (Execute Tasks)      │
        │  [Worker1] [Worker2]  │
        └───────────────────────┘
```

### How It Works:

1. **You submit code** → Goes to Driver Node
2. **Driver Node**:
   - Breaks code into transformations, stages, jobs, tasks
   - Sends breakdown to Cluster Manager
3. **Cluster Manager**:
   - Creates Worker Nodes based on requirements
4. **Worker Nodes**:
   - Actually execute transformations
   - Process the data

**Architecture Type:** Master-Slave
- Master = Driver Program
- Slaves = Worker Nodes

---

## <a name="setup"></a>3. Databricks Setup & Overview

### Creating Free Databricks Account

1. **Google Search:** "databricks community sign up"
2. **Select:** "Get started with Community Edition"
3. **Fill Information** → Click Continue
4. **Check Email** → Verify account

### Databricks Interface Overview

```
Databricks Workspace
├── Workspace (Store notebooks)
├── Recent (Recently opened items)
├── Search (Find resources)
├── Catalog (Upload/manage data)
├── Workflows (Orchestrate notebooks)
└── Compute (Create clusters)
```

### Creating Your First Notebook

**Step 1: Create Workspace Folder**
```
1. Click "Workspace"
2. Click "Create" → "Folder"
3. Name: "databricks_tutorial"
4. Click "Create"
```

**Step 2: Create Notebook**
```
1. Navigate to your folder
2. Click "Create" → "Notebook"
3. Name: "first_pyspark_tutorial"
4. Language: Python
5. Click "Create"
```

### Creating Spark Cluster

**Why Need Cluster?** 
Spark uses cluster of machines to process data.

**Steps:**
```
1. Click "Compute" (left sidebar)
2. Click "Create Cluster"
3. Cluster Name: "my_cluster"
4. Keep default runtime
5. Click "Create"
6. Wait for green light (cluster ready)
```

**Status Indicators:**
- 🟢 Green = Cluster Running
- 🟡 Yellow = Starting
- ⚫ Gray = Stopped

---

## <a name="data-reading"></a>4. Data Reading with PySpark

### Uploading Data to Databricks

**Step 1: Upload CSV File**
```
1. Click "Catalog" tab
2. Click "Create Table"
3. Drop your CSV file OR click to browse
4. Note the file path: /FileStore/tables/your_file.csv
```

**Important:** File path format = `/FileStore/tables/filename.csv`

### Reading CSV Files

#### Basic CSV Read with Schema Inference

```python
# Read CSV with automatic schema detection
df = spark.read \
    .format("csv") \
    .option("inferSchema", True) \  # Auto-detect data types
    .option("header", True) \        # First row is header
    .load("/FileStore/tables/bigmart_sales.csv")

# Display the data
df.display()

# Alternative display method (less pretty)
df.show()
```

**Code Breakdown:**
- `spark.read` = Spark's Data Reader API
- `.format("csv")` = Tell Spark it's a CSV file
- `.option("inferSchema", True)` = Auto-detect column types (int, string, double)
- `.option("header", True)` = Use first row as column names
- `.load(path)` = File location
- `.display()` = Show data in nice table format ✅
- `.show()` = Show data in plain text format

#### Finding File Paths with DBUtils

If you forget the file path:

```python
# List files in FileStore
dbutils.fs.ls("/FileStore")

# List files in tables folder
dbutils.fs.ls("/FileStore/tables/")
```

**Output Example:**
```
FileStoreList([
    FileInfo(path='dbfs:/FileStore/tables/', name='tables/', size=0)
])
```

### Reading JSON Files

#### Upload JSON File
```
1. Catalog → Create Table
2. Upload drivers.json
3. Note path: /FileStore/tables/drivers.json
```

#### JSON Read Code

```python
# Read JSON with schema inference
df_json = spark.read \
    .format("json") \
    .option("inferSchema", True) \
    .option("header", True) \
    .option("multiLine", False) \  # Single-line JSON
    .load("/FileStore/tables/drivers.json")

df_json.display()
```

**JSON Format Types:**

**Single-Line JSON:** (multiLine=False)
```json
{"id": 1, "name": "John"}
{"id": 2, "name": "Jane"}
```

**Multi-Line JSON:** (multiLine=True)
```json
{
  "id": 1,
  "name": "John"
}
{
  "id": 2,
  "name": "Jane"
}
```

### Print Schema

```python
# View data types of all columns
df.printSchema()
```

**Output Example:**
```
root
 |-- item_identifier: string
 |-- item_weight: double
 |-- item_fat_content: string
 |-- item_visibility: double
 |-- item_type: string
 |-- item_MRP: double
```

---

## <a name="basic-transformations"></a>5. Schema Definition (DDL & StructType)

### Why Define Schema Manually?

Sometimes you want to control data types instead of letting Spark infer them.

**Example Use Case:**
- Spark infers `item_weight` as `double`
- You want it as `string`

### Method 1: DDL Schema (Recommended ✅)

**DDL = Data Definition Language** (Same as SQL)

```python
# Define schema using DDL (easy way)
my_ddl_schema = """
    item_identifier STRING,
    item_weight STRING,
    item_fat_content STRING,
    item_visibility DOUBLE,
    item_type STRING,
    item_MRP DOUBLE,
    outlet_identifier STRING,
    outlet_establishment_year INT,
    outlet_size STRING,
    outlet_location_type STRING,
    outlet_type STRING,
    item_outlet_sales DOUBLE
"""

# Read CSV with custom schema
df = spark.read \
    .format("csv") \
    .schema(my_ddl_schema) \  # Use custom schema
    .option("header", True) \
    .load("/FileStore/tables/bigmart_sales.csv")

df.printSchema()
```

**Benefits of DDL Schema:**
- Simple and readable
- Same syntax as SQL
- Quick to write

### Method 2: StructType Schema (Detailed)

```python
# Import required types
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define schema using StructType
my_struct_schema = StructType([
    StructField("item_identifier", StringType(), True),
    StructField("item_weight", StringType(), True),
    StructField("item_fat_content", StringType(), True),
    StructField("item_visibility", DoubleType(), True),
    StructField("item_type", StringType(), True),
    StructField("item_MRP", DoubleType(), True)
])

# Read with StructType schema
df = spark.read \
    .format("csv") \
    .schema(my_struct_schema) \
    .option("header", True) \
    .load("/FileStore/tables/bigmart_sales.csv")
```

**StructField Parameters:**
```python
StructField("column_name", DataType(), nullable)
#           ↓              ↓           ↓
#         Column Name    Data Type   Allow Nulls?
```

**Common Data Types:**
- `StringType()`
- `IntegerType()`
- `DoubleType()`
- `FloatType()`
- `BooleanType()`
- `DateType()`
- `TimestampType()`

**Why Use StructType?**
- More control
- Can specify nullability
- Good for complex schemas

**Recommendation:** Use DDL Schema for simplicity!

---

### Select Transformation

**Purpose:** Choose specific columns from DataFrame

#### Method 1: Direct Column Names (Simple)

```python
# Select 3 columns
df_select = df.select(
    "item_identifier",
    "item_weight", 
    "item_fat_content"
)

df_select.display()
```

#### Method 2: Using Column Objects (Professional ✅)

```python
from pyspark.sql.functions import col

# Select using col() function
df_select = df.select(
    col("item_identifier"),
    col("item_weight"),
    col("item_fat_content")
)

df_select.display()
```

**Why use `col()`?**
- Required for applying functions
- Needed for aggregations
- Professional standard
- More flexible

**Tip:** Always use `col()` for consistency!

---

### Alias Transformation

**Purpose:** Rename columns in your output

```python
from pyspark.sql.functions import col

# Rename column using alias
df.select(
    col("item_identifier").alias("item_id"),
    col("item_weight"),
    col("item_fat_content")
).display()
```

**Output:**
```
| item_id | item_weight | item_fat_content |
|---------|-------------|------------------|
| FDA15   | 9.30        | Low Fat          |
```

**Difference: Alias vs withColumnRenamed**

```python
# Alias - temporary rename in query
df.select(col("item_identifier").alias("item_id"))

# withColumnRenamed - permanent rename in DataFrame
df = df.withColumnRenamed("item_identifier", "item_id")
```

---

### Filter/Where Transformation

**Purpose:** Filter rows based on conditions

#### Scenario 1: Simple Filter

```python
# Filter rows where item_fat_content = 'Regular'
df.filter(
    col("item_fat_content") == "Regular"
).display()
```

**Operators:**
- `==` Equal to
- `!=` Not equal to
- `>` Greater than
- `<` Less than
- `>=` Greater than or equal
- `<=` Less than or equal

#### Scenario 2: Multiple Conditions (AND)

```python
# Filter: item_type = 'Soft Drinks' AND item_weight < 10
df.filter(
    (col("item_type") == "Soft Drinks") & 
    (col("item_weight") < 10)
).display()
```

**Important:**
- Use `&` for AND (not `and`)
- Use `|` for OR (not `or`)
- Wrap each condition in parentheses `()`

#### Scenario 3: Complex Filter (OR + NULL check)

```python
# Filter: (location_type IN ['Tier 1', 'Tier 2']) AND (outlet_size IS NULL)
df.filter(
    (col("outlet_size").isNull()) &
    (col("outlet_location_type").isin("Tier 1", "Tier 2"))
).display()
```

**Special Filter Functions:**
- `.isNull()` - Check if NULL
- `.isNotNull()` - Check if NOT NULL
- `.isin(value1, value2, ...)` - Check if value in list

---

### WithColumnRenamed Transformation

**Purpose:** Rename columns at DataFrame level

```python
# Rename column permanently
df = df.withColumnRenamed("item_weight", "item_wt")

df.display()
```

**Result:**
- Column `item_weight` → `item_wt`
- Change persists in DataFrame

---

### WithColumn Transformation

**Purpose:** Create new columns OR modify existing ones

#### Scenario 1: Create Column with Constant Value

```python
from pyspark.sql.functions import lit

# Add new column with constant value
df = df.withColumn("flag", lit("new"))

df.display()
```

**What is `lit()`?**
- `lit()` = Literal value
- Converts constant value into column expression
- Required for constant values in withColumn

**Output:**
```
| item_identifier | item_weight | flag |
|-----------------|-------------|------|
| FDA15           | 9.30        | new  |
| DRC01           | 5.92        | new  |
```

#### Scenario 2: Create Column with Calculation

```python
# Create column by multiplying two columns
df = df.withColumn(
    "multiply",
    col("item_weight") * col("item_MRP")
)

df.display()
```

**Output:**
```
| item_weight | item_MRP | multiply  |
|-------------|----------|-----------|
| 9.30        | 249.8092 | 2323.23   |
| 5.92        | 48.2692  | 285.75    |
```

#### Scenario 3: Modify Existing Column

```python
from pyspark.sql.functions import regexp_replace

# Modify existing column (replace values)
df = df.withColumn(
    "item_fat_content",
    regexp_replace(col("item_fat_content"), "Regular", "R")
)

df.display()
```

**Key Point:** 
- Same column name = Modify existing
- New column name = Create new

#### Scenario 4: Multiple Transformations (Chaining)

```python
# Chain multiple withColumn operations
df = df.withColumn(
    "item_fat_content",
    regexp_replace(col("item_fat_content"), "Regular", "R")
).withColumn(
    "item_fat_content",
    regexp_replace(col("item_fat_content"), "Low Fat", "LF")
)

df.display()
```

**Result:**
- "Regular" → "R"
- "Low Fat" → "LF"

