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
# Complete PySpark Master Class Notes (Continued)

## 6. Type Casting

**Purpose:** Convert column data types

```python
from pyspark.sql.types import StringType

# Cast column to different type
df = df.withColumn(
    "item_weight",
    col("item_weight").cast(StringType())
)

# Verify change
df.printSchema()
```

**Common Type Casts:**
```python
.cast(StringType())    # To String
.cast(IntegerType())   # To Integer
.cast(DoubleType())    # To Double
.cast(FloatType())     # To Float
.cast(DateType())      # To Date
```

**When to Use:**
- Before joins (matching column types)
- Before aggregations (ensure numeric types)
- Data type corrections

---

## 7. Sort/OrderBy Transformation

**Purpose:** Arrange DataFrame rows by column values

### Scenario 1: Sort Descending

```python
from pyspark.sql.functions import desc

# Sort by item_weight (descending)
df.sort(
    col("item_weight").desc()
).display()
```

### Scenario 2: Sort Ascending

```python
from pyspark.sql.functions import asc

# Sort by item_visibility (ascending)
df.sort(
    col("item_visibility").asc()
).display()

# Default is ascending (can omit .asc())
df.sort(col("item_visibility")).display()
```

### Scenario 3: Sort Multiple Columns

```python
# Sort by multiple columns
df.sort(
    col("item_weight").desc(),
    col("item_visibility").desc()
).display()
```

### Scenario 4: Sort with Boolean Array

```python
# Sort multiple columns with different orders
df.sort(
    ["item_weight", "item_visibility"],
    ascending=[False, False]  # Both descending
).display()

# Mixed ordering
df.sort(
    ["item_weight", "item_visibility"],
    ascending=[False, True]  # weight DESC, visibility ASC
).display()
```

**How Multi-Column Sort Works:**
1. Sort by first column
2. For duplicate values in first column, sort by second column
3. Continue pattern for additional columns

---

## 8. Limit Transformation

**Purpose:** Return only N rows (like SQL LIMIT or Pandas head())

```python
# Get first 10 rows
df.limit(10).display()
```

**Use Cases:**
- Preview data quickly
- Testing transformations
- Sampling data

---

## 9. Drop Transformation

**Purpose:** Remove columns from DataFrame

### Drop Single Column

```python
# Drop one column
df.drop("item_visibility").display()
```

### Drop Multiple Columns

```python
# Drop multiple columns
df.drop(
    "item_visibility",
    "item_type"
).display()
```

**Note:** Original DataFrame unchanged unless reassigned:
```python
# Permanent change
df = df.drop("item_visibility")
```

---

## 10. Drop Duplicates (Deduplication)

**Purpose:** Remove duplicate rows

### Method 1: Drop All Duplicate Rows

```python
# Remove rows where ALL columns are duplicates
df.dropDuplicates().display()

# Alternative syntax (Pandas style)
df.drop_duplicates().display()
```

### Method 2: Drop Based on Specific Columns

```python
# Remove duplicates based on subset of columns
df.dropDuplicates(subset=["item_type"]).display()

# Alternative
df.drop_duplicates(subset=["item_type"]).display()
```

**Result:** Returns only unique values in specified columns

### Method 3: Distinct (Simplified)

```python
# Drop all duplicate rows (simpler syntax)
df.distinct().display()
```

**Comparison:**
- `distinct()` = Always removes complete row duplicates
- `dropDuplicates()` = Can specify subset of columns
- `drop_duplicates()` = Pandas-style syntax

---

## 11. Union Transformation

**Purpose:** Combine two DataFrames vertically (stack rows)

### Setup: Create Sample DataFrames

```python
# Data for DataFrame 1
data1 = [
    (1, "Alice"),
    (2, "Bob")
]

schema1 = "id INT, name STRING"

df1 = spark.createDataFrame(data1, schema1)

# Data for DataFrame 2
data2 = [
    (3, "Charlie"),
    (4, "David")
]

schema2 = "id INT, name STRING"

df2 = spark.createDataFrame(data2, schema2)
```

### Basic Union

```python
# Combine DataFrames
df_union = df1.union(df2)
df_union.display()
```

**Output:**
```
| id | name    |
|----|---------|
| 1  | Alice   |
| 2  | Bob     |
| 3  | Charlie |
| 4  | David   |
```

**How Union Works:**
- Stacks DataFrames one after another
- Does NOT check column names
- Matches by position only

### Problem: Column Order Mismatch

```python
# Create df1 with different column order
data1 = [(1, "Alice")]
schema1 = "name STRING, id INT"  # name first
df1 = spark.createDataFrame(data1, schema1)

# df2 with normal order
data2 = [(3, "Charlie")]
schema2 = "id INT, name STRING"  # id first
df2 = spark.createDataFrame(data2, schema2)

# Union will mismatch!
df1.union(df2).display()
```

**Result (WRONG):**
```
| name    | id      |
|---------|---------|
| Alice   | 1       |
| 3       | Charlie | ← Mismatched!
```

### Solution: UnionByName ✅

```python
# Union matching by column names
df1.unionByName(df2).display()
```

**Result (CORRECT):**
```
| name    | id |
|---------|----|
| Alice   | 1  |
| Charlie | 3  |
```

**Key Differences:**
- `union()` = Matches by position (order matters)
- `unionByName()` = Matches by column name (order doesn't matter) ✅

---

## 12. String Functions

**Purpose:** Manipulate text in columns

### initCap (Proper Case)

```python
from pyspark.sql.functions import initcap

# Capitalize first letter of each word
df.select(
    initcap(col("item_type"))
).display()
```

**Example:**
- "soft drinks" → "Soft Drinks"
- "dairy" → "Dairy"

### Lower (Lowercase)

```python
from pyspark.sql.functions import lower

# Convert to lowercase
df.select(
    lower(col("item_type"))
).display()
```

**Example:**
- "Soft Drinks" → "soft drinks"
- "DAIRY" → "dairy"

### Upper (Uppercase)

```python
from pyspark.sql.functions import upper

# Convert to uppercase
df.select(
    upper(col("item_type")).alias("upper_item_type")
).display()
```

**Example:**
- "soft drinks" → "SOFT DRINKS"
- "dairy" → "DAIRY"

**Use Cases:**
- Standardizing text for joins
- Data cleaning
- Formatting output

---

## 13. Date Functions

**Purpose:** Work with dates and timestamps

### Current Date

```python
from pyspark.sql.functions import current_date

# Add column with today's date
df = df.withColumn("current_date", current_date())

df.display()
```

### Date Add

```python
from pyspark.sql.functions import date_add

# Add 7 days to current_date
df = df.withColumn(
    "week_after",
    date_add(col("current_date"), 7)
)

df.display()
```

### Date Subtract (Two Methods)

**Method 1: date_sub function**
```python
from pyspark.sql.functions import date_sub

# Subtract 7 days
df.withColumn(
    "week_before",
    date_sub(col("current_date"), 7)
).display()
```

**Method 2: date_add with negative (Recommended ✅)**
```python
# Add negative days (cleaner approach)
df.withColumn(
    "week_before",
    date_add(col("current_date"), -7)
).display()
```

### Date Difference

```python
from pyspark.sql.functions import datediff

# Calculate days between two dates
df = df.withColumn(
    "date_diff",
    datediff(col("week_after"), col("current_date"))
)

df.display()
```

**Output:** Returns number of days between dates

**Parameters:**
```python
datediff(end_date, start_date)
#        ↓          ↓
#     Later Date  Earlier Date
```

### Date Format

```python
from pyspark.sql.functions import date_format

# Change date format
df = df.withColumn(
    "week_before",
    date_format(col("week_before"), "dd-MM-yyyy")
)

df.display()
```

**Format Options:**
- `yyyy-MM-dd` → 2025-11-25
- `dd-MM-yyyy` → 25-11-2025
- `MM/dd/yyyy` → 11/25/2025
- `dd MMM yyyy` → 25 Nov 2025

---

## 14. Handling Nulls

**Purpose:** Deal with missing values

### Dropping Nulls

#### Drop ALL Rows with ANY Null

```python
# Drop rows with null in ANY column
df.dropna(how="any").display()
```

#### Drop Rows Where ALL Columns are Null

```python
# Drop rows where ALL columns are null
df.dropna(how="all").display()
```

#### Drop Based on Specific Column (Recommended ✅)

```python
# Drop nulls only in outlet_size column
df.dropna(subset=["outlet_size"]).display()
```

**Comparison:**
- `how="any"` = Drop if ANY column has null (aggressive)
- `how="all"` = Drop if ALL columns are null (lenient)
- `subset=["col"]` = Drop based on specific columns (precise) ✅

### Filling Nulls

#### Fill ALL Nulls with Same Value

```python
# Replace all nulls with "Not Available"
df.fillna("Not Available").display()
```

#### Fill Nulls in Specific Column (Recommended ✅)

```python
# Fill nulls only in outlet_size column
df.fillna("Not Available", subset=["outlet_size"]).display()
```

**Best Practice:** Always use `subset` to target specific columns!

---

## 15. Split and Indexing

**Purpose:** Split string columns into arrays and extract elements

### Split Function

```python
from pyspark.sql.functions import split

# Split outlet_type by space
df = df.withColumn(
    "outlet_type",
    split(col("outlet_type"), " ")
)

df.display()
```

**Example:**
- "Supermarket Type1" → ["Supermarket", "Type1"]
- "Grocery Store" → ["Grocery", "Store"]

**Result:** Column becomes array type

### Array Indexing

```python
# Get element at index 1 (second element)
df = df.withColumn(
    "outlet_type",
    split(col("outlet_type"), " ")[1]
)

df.display()
```

**Array Indexing:**
- Index starts at 0
- [0] = First element
- [1] = Second element
- [2] = Third element, etc.

**Example:**
- ["Supermarket", "Type1"][1] → "Type1"
- ["Grocery", "Store"][1] → "Store"

---

## 16. Explode Function

**Purpose:** Convert array into separate rows

### Setup: Create Array Column

```python
# Create array column first
df_exp = df.withColumn(
    "outlet_type",
    split(col("outlet_type"), " ")
)

df_exp.display()
```

**Current State:**
```
| outlet_type              |
|--------------------------|
| [Supermarket, Type1]     |
| [Grocery, Store]         |
```

### Explode Array

```python
from pyspark.sql.functions import explode

# Explode array into separate rows
df_exp = df_exp.withColumn(
    "outlet_type",
    explode(col("outlet_type"))
)

df_exp.display()
```

**Result:**
```
| outlet_type   |
|---------------|
| Supermarket   |
| Type1         |
| Grocery       |
| Store         |
```

**Use Cases:**
- Flattening nested data
- Normalizing array columns
- Creating one row per array element

---

## 17. Array Contains

**Purpose:** Check if value exists in array column

```python
from pyspark.sql.functions import array_contains

# Check if array contains "Type1"
df_exp = df_exp.withColumn(
    "type1_flag",
    array_contains(col("outlet_type"), "Type1")
)

df_exp.display()
```

**Output:**
```
| outlet_type              | type1_flag |
|--------------------------|------------|
| [Supermarket, Type1]     | true       |
| [Grocery, Store]         | false      |
| [Supermarket, Type2]     | false      |
```

**Use Cases:**
- Creating flags/indicators
- Filtering arrays
- Conditional logic on arrays

---

## 18. GroupBy and Aggregations

**Purpose:** Group data and calculate summary statistics

### Basic GroupBy with Sum

```python
# Sum of item_MRP by item_type
df.groupBy("item_type") \
  .agg(sum("item_MRP")) \
  .display()
```

### GroupBy with Average

```python
from pyspark.sql.functions import avg

# Average MRP by item_type
df.groupBy("item_type") \
  .agg(avg("item_MRP")) \
  .display()
```

### GroupBy Multiple Columns

```python
# Group by two columns
df.groupBy("item_type", "outlet_size") \
  .agg(sum("item_MRP").alias("total_MRP")) \
  .display()
```

### Multiple Aggregations

```python
from pyspark.sql.functions import sum, avg

# Multiple aggregations at once
df.groupBy("item_type", "outlet_size") \
  .agg(
      sum("item_MRP").alias("total_MRP"),
      avg("item_MRP").alias("avg_MRP")
  ) \
  .display()
```

**Common Aggregation Functions:**
- `sum()` - Sum of values
- `avg()` - Average
- `count()` - Count of rows
- `min()` - Minimum value
- `max()` - Maximum value
- `countDistinct()` - Count unique values

---

## 19. Collect List

**Purpose:** Aggregate values into an array

```python
from pyspark.sql.functions import collect_list

# Example data
data_books = [
    ("user1", "book1"),
    ("user1", "book2"),
    ("user2", "book3"),
    ("user2", "book4")
]

df_books = spark.createDataFrame(data_books, ["user", "book"])

# Collect all books per user into array
df_books.groupBy("user") \
    .agg(collect_list("book")) \
    .display()
```

**Output:**
```
| user  | collect_list(book)  |
|-------|---------------------|
| user1 | [book1, book2]      |
| user2 | [book3, book4]      |
```

**Use Cases:**
- Creating lists of items per group
- Aggregating categorical values
- Alternative to group_concat (MySQL)

---

## 20. Pivot

**Purpose:** Transform rows into columns (like Excel pivot tables)

```python
# Pivot data
df.groupBy("item_type") \
  .pivot("outlet_size") \
  .agg(avg("item_MRP")) \
  .display()
```

**Before Pivot:**
```
| item_type | outlet_size | item_MRP |
|-----------|-------------|----------|
| Dairy     | Medium      | 100      |
| Dairy     | High        | 120      |
| Drinks    | Medium      | 80       |
```

**After Pivot:**
```
| item_type | Medium | High | Small |
|-----------|--------|------|-------|
| Dairy     | 100    | 120  | null  |
| Drinks    | 80     | null | null  |
```

**How Pivot Works:**
1. Group by specified column(s)
2. Convert unique values from pivot column into separate columns
3. Aggregate values at intersections

---

## 21. When-Otherwise (Conditional Logic)

**Purpose:** Apply if-then-else logic (like SQL CASE WHEN)

### Simple Condition

```python
from pyspark.sql.functions import when

# Create veg flag
df = df.withColumn(
    "veg_flag",
    when(col("item_type") == "Meat", "non-veg")
    .otherwise("veg")
)

df.display()
```

### Multiple Conditions

```python
# Complex conditional logic
df = df.withColumn(
    "veg_exp_flag",
    when((col("veg_flag") == "veg") & (col("item_MRP") < 100), "veg inexpensive")
    .when((col("veg_flag") == "veg") & (col("item_MRP") >= 100), "veg expensive")
    .otherwise("non-veg")
)

df.display()
```

**Syntax:**
```python
when(condition, value_if_true)
.when(another_condition, value_if_true)
.otherwise(default_value)
```

**Comparison to SQL:**
```sql
CASE 
    WHEN condition1 THEN value1
    WHEN condition2 THEN value2
    ELSE default_value
END
```

---

# Complete PySpark Master Class Notes (Continued)

## 22. Joins in PySpark

**Purpose:** Combine two DataFrames based on common columns

### Understanding Joins - Setup

```python
# Sample Data for Joins
data1 = [
    (1, "Alice", "D01"),
    (2, "Bob", "D02"),
    (3, "Charlie", "D03"),
    (4, "David", "D03"),
    (5, "Eve", "D05")
]

schema1 = "emp_id INT, emp_name STRING, dept_id STRING"
df1 = spark.createDataFrame(data1, schema1)

data2 = [
    ("D01", "HR"),
    ("D02", "IT"),
    ("D03", "Finance"),
    ("D04", "Marketing"),
    ("D05", "Sales")
]

schema2 = "dept_id STRING, dept_name STRING"
df2 = spark.createDataFrame(data2, schema2)
```

**DataFrames:**
```
df1 (Employees):          df2 (Departments):
| emp_id | emp_name | dept_id |    | dept_id | dept_name  |
|--------|----------|---------|    |---------|------------|
| 1      | Alice    | D01     |    | D01     | HR         |
| 2      | Bob      | D02     |    | D02     | IT         |
| 3      | Charlie  | D03     |    | D03     | Finance    |
| 4      | David    | D03     |    | D04     | Marketing  |
| 5      | Eve      | D05     |    | D05     | Sales      |
```

---

### Inner Join

**Definition:** Returns only matching records from both DataFrames

**Visual:**
```
df1: D01, D02, D03, D03, D05
df2: D01, D02, D03, D04, D05
                    ↓
Inner Join: D01, D02, D03, D03, D05 (only matching)
```

**Code:**
```python
# Inner Join
df_inner = df1.join(
    df2,
    df1["dept_id"] == df2["dept_id"],
    "inner"
)

df_inner.display()
```

**Result:**
```
| emp_id | emp_name | dept_id | dept_id | dept_name |
|--------|----------|---------|---------|-----------|
| 1      | Alice    | D01     | D01     | HR        |
| 2      | Bob      | D02     | D02     | IT        |
| 3      | Charlie  | D03     | D03     | Finance   |
| 4      | David    | D03     | D03     | Finance   |
| 5      | Eve      | D05     | D05     | Sales     |
```

**Key Points:**
- D04 from df2 NOT included (no match in df1)
- D03 appears twice (two employees in same dept)
- Only common records returned

---

### Left Join (Left Outer Join)

**Definition:** Returns ALL records from left DataFrame + matching records from right

**Visual:**
```
df1 (Left): D01, D02, D03, D03, D05, D06
df2 (Right): D01, D02, D03, D04, D05
                    ↓
Left Join: All from df1 + matching from df2
Result: D01, D02, D03, D03, D05, D06 (D06 has null for df2 columns)
```

**Code:**
```python
# Add record with no match in df2
data1_extra = [
    (1, "Alice", "D01"),
    (2, "Bob", "D02"),
    (3, "Charlie", "D03"),
    (4, "David", "D03"),
    (5, "Eve", "D05"),
    (6, "Frank", "D06")  # No D06 in df2
]

df1 = spark.createDataFrame(data1_extra, schema1)

# Left Join
df_left = df1.join(
    df2,
    df1["dept_id"] == df2["dept_id"],
    "left"
)

df_left.display()
```

**Result:**
```
| emp_id | emp_name | dept_id | dept_id | dept_name |
|--------|----------|---------|---------|-----------|
| 1      | Alice    | D01     | D01     | HR        |
| 2      | Bob      | D02     | D02     | IT        |
| 3      | Charlie  | D03     | D03     | Finance   |
| 4      | David    | D03     | D03     | Finance   |
| 5      | Eve      | D05     | D05     | Sales     |
| 6      | Frank    | D06     | null    | null      |
```

**Key Points:**
- ALL records from df1 (left) included
- D06 has no match → nulls for df2 columns
- Most commonly used join ✅
- Prevents data loss from left table

---

### Right Join (Right Outer Join)

**Definition:** Returns matching records + ALL records from right DataFrame

**Visual:**
```
df1 (Left): D01, D02, D03, D03, D05
df2 (Right): D01, D02, D03, D04, D05
                    ↓
Right Join: All from df2 + matching from df1
Result: D01, D02, D03, D03, D04, D05 (D04 has null for df1 columns)
```

**Code:**
```python
# Right Join
df_right = df1.join(
    df2,
    df1["dept_id"] == df2["dept_id"],
    "right"
)

df_right.display()
```

**Result:**
```
| emp_id | emp_name | dept_id | dept_id | dept_name  |
|--------|----------|---------|---------|------------|
| 1      | Alice    | D01     | D01     | HR         |
| 2      | Bob      | D02     | D02     | IT         |
| 3      | Charlie  | D03     | D03     | Finance    |
| 4      | David    | D03     | D03     | Finance    |
| null   | null     | null    | D04     | Marketing  |
| 5      | Eve      | D05     | D05     | Sales      |
```

**Key Points:**
- ALL records from df2 (right) included
- D04 has no match → nulls for df1 columns
- Opposite of left join

---

### Anti Join

**Definition:** Returns records from left DataFrame that DON'T match right DataFrame

**Visual:**
```
df1: D01, D02, D03, D03, D05, D06
df2: D01, D02, D03, D04, D05
                    ↓
Anti Join: D06 (only in df1, NOT in df2)
```

**Code:**
```python
# Anti Join
df_anti = df1.join(
    df2,
    df1["dept_id"] == df2["dept_id"],
    "anti"
)

df_anti.display()
```

**Result:**
```
| emp_id | emp_name | dept_id |
|--------|----------|---------|
| 6      | Frank    | D06     |
```

**Key Points:**
- Returns ONLY non-matching records from left
- No columns from right DataFrame
- NOT available in standard SQL (need subqueries)
- Very useful for finding missing/orphaned records

**Use Cases:**
- Find employees without departments
- Find unprocessed records
- Data validation
- Identify orphaned records

---

### Join Syntax Summary

```python
# General Join Syntax
df1.join(
    df2,                              # Right DataFrame
    df1["key"] == df2["key"],         # Join condition
    "join_type"                       # Type: inner, left, right, anti
)

# Common join types:
"inner"    # Only matching records
"left"     # All from left + matching from right ✅ Most Used
"right"    # All from right + matching from left
"anti"     # Only non-matching from left
"full"     # All records from both (with nulls)
```

**Best Practices:**
1. Always specify DataFrame in condition: `df1["col"] == df2["col"]`
2. Use left join when you can't lose left table data
3. Use anti join to find missing records
4. Always check for duplicate column names after join

---

## 23. Window Functions

**Purpose:** Perform calculations across rows related to current row

### Understanding Window Functions

**Traditional Aggregation:**
```python
# Groups collapse to single row per group
df.groupBy("item_type").agg(sum("item_MRP"))
```

**Window Function:**
```python
# Keeps all rows, adds calculation column
df.withColumn("row_num", row_number().over(window_spec))
```

**Key Difference:**
- GroupBy → Reduces rows
- Window Functions → Keeps all rows, adds calculated column

---

### Row Number

**Purpose:** Assign unique sequential number to each row

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define window specification
window_spec = Window.orderBy("item_identifier")

# Add row number column
df = df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

df.display()
```

**Result:**
```
| item_identifier | item_weight | row_num |
|-----------------|-------------|---------|
| DRA12          | 10.5        | 1       |
| DRA24          | 9.3         | 2       |
| DRA59          | 5.92        | 3       |
| FDB23          | 17.5        | 4       |
```

**Use Cases:**
- Generate unique IDs
- Remove duplicates (keep first/last occurrence)
- Pagination
- Top N per group

### Removing Duplicates with Row Number

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Window partitioned by duplicate columns
window_spec = Window.partitionBy("item_identifier").orderBy("item_weight")

# Add row number
df_dedup = df.withColumn("row_num", row_number().over(window_spec))

# Keep only first occurrence (row_num = 1)
df_dedup = df_dedup.filter(col("row_num") == 1).drop("row_num")

df_dedup.display()
```

---

### Rank Function

**Purpose:** Assign rank with gaps for ties

```python
from pyspark.sql.functions import rank

# Window specification
window_spec = Window.orderBy(col("item_weight").desc())

# Add rank column
df = df.withColumn(
    "rank",
    rank().over(window_spec)
)

df.display()
```

**Example:**
```
| item_weight | rank |
|-------------|------|
| 10.5        | 1    |
| 10.5        | 1    |
| 10.5        | 1    |
| 9.3         | 4    | ← Skips 2 and 3!
| 8.1         | 5    |
```

**How Rank Works:**
- Same values get same rank
- Next rank skips numbers (gaps)
- If 3 values tied at rank 1, next rank is 4

---

### Dense Rank Function

**Purpose:** Assign rank WITHOUT gaps for ties

```python
from pyspark.sql.functions import dense_rank

# Window specification
window_spec = Window.orderBy(col("item_weight").desc())

# Add dense rank column
df = df.withColumn(
    "dense_rank",
    dense_rank().over(window_spec)
)

df.display()
```

**Example:**
```
| item_weight | dense_rank |
|-------------|------------|
| 10.5        | 1          |
| 10.5        | 1          |
| 10.5        | 1          |
| 9.3         | 2          | ← No gaps!
| 8.1         | 3          |
```

**Comparison:**
```
| Value | Rank | Dense Rank |
|-------|------|------------|
| 100   | 1    | 1          |
| 100   | 1    | 1          |
| 90    | 3    | 2          | ← Key Difference
| 80    | 4    | 3          |
```

---

### Window Specification Options

```python
from pyspark.sql.window import Window

# Order by column
Window.orderBy("column_name")
Window.orderBy(col("column_name").desc())

# Partition by (group within window)
Window.partitionBy("category").orderBy("sales")

# Multiple columns
Window.partitionBy("category", "region").orderBy(col("sales").desc())
```

**partitionBy vs groupBy:**
- `groupBy()` → Collapses rows to one per group
- `partitionBy()` → Keeps all rows, calculates within groups

---

### Cumulative Sum (Advanced Window Function)

**Purpose:** Running total (sum of all previous + current row)

```python
from pyspark.sql.functions import sum
from pyspark.sql.window import Window

# Window with frame specification
window_spec = Window \
    .orderBy("item_type") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Cumulative sum
df = df.withColumn(
    "cumsum",
    sum("item_MRP").over(window_spec)
)

df.display()
```

**Result:**
```
| item_type | item_MRP | cumsum  |
|-----------|----------|---------|
| Dairy     | 100      | 100     | (100)
| Dairy     | 150      | 250     | (100+150)
| Drinks    | 80       | 330     | (100+150+80)
| Drinks    | 90       | 420     | (100+150+80+90)
```

**Frame Specification:**
```python
.rowsBetween(Window.unboundedPreceding, Window.currentRow)
#            ↓                          ↓
#         Start of partition       Current row
```

**Frame Options:**
- `unboundedPreceding` → All rows before current
- `currentRow` → Current row
- `unboundedFollowing` → All rows after current

**Use Cases:**
- Running totals
- Moving averages
- Cumulative percentages
- Year-to-date calculations

### Total Sum (All Rows)

```python
# Sum of ALL rows (no frame restriction)
window_spec = Window \
    .orderBy("item_type") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df = df.withColumn(
    "total_sum",
    sum("item_MRP").over(window_spec)
)
```

**Result:** Same total in every row

---

## 24. User Defined Functions (UDF)

**Purpose:** Create custom functions when built-in functions aren't enough

### ⚠️ Important Warning

**UDFs have performance issues!**
- Adds Python interpreter to executors
- Slower than built-in functions
- Use ONLY when necessary

**Recommendation:** Always try built-in functions first!

### Creating UDF - 3 Steps

**Step 1: Create Python Function**
```python
# Regular Python function
def my_function(x):
    return x * x  # Square the value
```

**Step 2: Register as PySpark UDF**
```python
from pyspark.sql.functions import udf

# Convert to PySpark UDF
my_udf = udf(my_function)
```

**Step 3: Use in DataFrame**
```python
# Apply UDF to column
df = df.withColumn(
    "my_new_column",
    my_udf(col("item_MRP"))
)

df.display()
```

**Complete Example:**
```python
# Step 1: Define function
def square_value(x):
    return x * x

# Step 2: Register UDF
from pyspark.sql.functions import udf
square_udf = udf(square_value)

# Step 3: Use UDF
df = df.withColumn(
    "mrp_squared",
    square_udf(col("item_MRP"))
)

df.display()
```

### UDF with Return Type (Better Practice)

```python
from pyspark.sql.types import DoubleType

# Define function with return type
my_udf = udf(my_function, DoubleType())

# Use in DataFrame
df = df.withColumn("squared", my_udf(col("item_MRP")))
```

**Common Return Types:**
- `StringType()`
- `IntegerType()`
- `DoubleType()`
- `BooleanType()`
- `ArrayType(StringType())`

### Complex UDF Example

```python
def categorize_price(price):
    if price < 50:
        return "Cheap"
    elif price < 150:
        return "Medium"
    else:
        return "Expensive"

# Register UDF
from pyspark.sql.types import StringType
categorize_udf = udf(categorize_price, StringType())

# Apply UDF
df = df.withColumn(
    "price_category",
    categorize_udf(col("item_MRP"))
)
```

**When to Use UDFs:**
- Complex business logic not available in built-in functions
- Custom calculations
- External library integration
- No other option available

**When NOT to Use:**
- Built-in function exists ✅
- Can achieve with when-otherwise
- Performance is critical

---

## 25. Data Writing

**Purpose:** Save transformed DataFrames to storage

### Understanding Data Writing Architecture

```
Your Transformation → DataFrame → Write to Storage

Storage Options:
- Azure: ADLS Gen2 (Data Lake)
- AWS: S3
- Databricks: Default Storage (for practice)
```

### Writing CSV Files

```python
# Basic CSV write
df.write \
    .format("csv") \
    .save("/FileStore/tables/csv/data.csv")
```

**Problem:** What if file already exists?

---

### Writing Modes (Critical!)

#### Mode 1: Append

**Purpose:** Add data to existing files (no data loss)

```python
df.write \
    .format("csv") \
    .mode("append") \
    .save("/FileStore/tables/csv/data.csv")
```

**How it works:**
```
Existing Folder:
├── file1.csv (existing)

After Append:
├── file1.csv (existing)
├── file2.csv (new)
```

**Use Cases:**
- Incremental loads
- Logging data
- Historical data
- When you can't lose existing data

---

#### Mode 2: Overwrite ⚠️

**Purpose:** Replace ALL existing data

```python
df.write \
    .format("csv") \
    .mode("overwrite") \
    .save("/FileStore/tables/csv/data.csv")
```

**How it works:**
```
Existing Folder:
├── file1.csv (existing)
├── file2.csv (existing)

After Overwrite:
├── file3.csv (new) ← Old files deleted!
```

**Use Cases:**
- Staging tables
- Temporary data
- Full refresh loads
- When you want to replace everything

**⚠️ Warning:** Can cause data loss! Use carefully!

---

#### Mode 3: Error (Default)

**Purpose:** Throw error if file exists

```python
df.write \
    .format("csv") \
    .mode("error") \
    .save("/FileStore/tables/csv/data.csv")
```

**How it works:**
```
If file exists → Throws error
If file doesn't exist → Writes data
```

**Error Message:**
```
Path already exists: /FileStore/tables/csv/data.csv
```

**Use Cases:**
- Prevent accidental overwrites
- Safety check
- First-time writes

---

#### Mode 4: Ignore

**Purpose:** Skip write if file exists (silent)

```python
df.write \
    .format("csv") \
    .mode("ignore") \
    .save("/FileStore/tables/csv/data.csv")
```

**How it works:**
```
If file exists → Do nothing (no error)
If file doesn't exist → Write data
```

**Use Cases:**
- Idempotent pipelines
- Safe retries
- "Write once" scenarios

---

### Writing Modes Summary

```python
# Comparison Table
Mode         | File Exists? | Action
-------------|--------------|------------------
append       | Yes/No       | Add new file ✅
overwrite    | Yes/No       | Delete & replace ⚠️
error        | Yes          | Throw error ❌
error        | No           | Write data ✅
ignore       | Yes          | Do nothing 🤷
ignore       | No           | Write data ✅
```

---

### Alternative Write Syntax (Cleaner)

```python
# Using .option() for path
df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "/FileStore/tables/csv/data.csv") \
    .save()
```

---

### Writing Parquet Files

**Why Parquet?**
- Columnar storage format
- Better compression
- Faster queries
- Industry standard for big data ✅

```python
# Write as Parquet
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("/FileStore/tables/parquet/data.parquet")
```

### Parquet vs CSV

**Row-Based (CSV):**
```
Row 1: [1, "Alice", 100]
Row 2: [2, "Bob", 200]
Row 3: [3, "Charlie", 150]
```

**Column-Based (Parquet):**
```
Column ID: [1, 2, 3]
Column Name: ["Alice", "Bob", "Charlie"]
Column Sales: [100, 200, 150]
```

**Benefits of Parquet:**
- Select only needed columns (faster)
- Better compression (smaller files)
- Optimized for analytics
- Metadata in footer

---

### Writing Delta Format

**What is Delta Lake?**
- Built on top of Parquet
- ACID transactions
- Time travel
- Schema evolution
- Industry standard for lakehouse ✅

```python
# Write as Delta
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/FileStore/tables/delta/data")
```

**Delta vs Parquet:**

**Parquet:**
```
data.parquet
└── metadata in footer
```

**Delta:**
```
data/
├── part-001.parquet
├── part-002.parquet
└── _delta_log/
    └── 00000.json (transaction log)
```

**Benefits of Delta:**
- ACID transactions
- Time travel (query old versions)
- Schema enforcement
- Upserts/Merges
- Better for production ✅

---

### Creating Tables (Instead of Files)

```python
# Create managed table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("my_table")
```

**Result:** Creates table you can query with SQL

---

### Managed vs External Tables

#### Managed Tables

```python
# Managed table (no location specified)
df.write.saveAsTable("my_table")
```

**Characteristics:**
- Data stored in Databricks default location
- Databricks manages the data
- `DROP TABLE` → Deletes data + schema
- Easy but less control

**Diagram:**
```
DataFrame → Databricks Managed Location → Table
                (Hidden from you)
```

#### External Tables

```python
# External table (your location)
df.write \
    .option("path", "/my/custom/location") \
    .saveAsTable("my_table")
```

**Characteristics:**
- Data stored in YOUR location
- YOU manage the data
- `DROP TABLE` → Deletes schema only (data remains)
- More control ✅
- Production standard

**Diagram:**
```
DataFrame → Your ADLS/S3 Location → Table
            (You control this)
```

**Comparison:**
```
Aspect          | Managed       | External
----------------|---------------|------------------
Data Location   | Databricks    | Your storage ✅
DROP TABLE      | Deletes data  | Keeps data ✅
Control         | Less          | More ✅
Production Use  | No            | Yes ✅
```

**Recommendation:** Always use External Tables in production!

---

## 26. SparkSQL

**Purpose:** Use SQL syntax on DataFrames

### Benefits of SparkSQL

✅ Same performance as PySpark API
✅ Familiar SQL syntax
✅ Can mix SQL and Python
✅ Great for complex queries

### Creating Temporary View

**Before using SQL, create a view:**

```python
# Create temporary view from DataFrame
df.createTempView("my_view")
```

**What is Temporary View?**
- SQL-queryable object
- Lives only during session
- Automatically deleted when session ends
- No permanent storage

---

### Using SparkSQL - Method 1: Magic Command

```python
# Change cell to SQL
%sql

SELECT * 
FROM my_view
WHERE item_fat_content = 'LF'
```

**Steps:**
1. Click language dropdown (Python)
2. Select SQL
3. Write SQL query
4. Run cell

---

### Using SparkSQL - Method 2: spark.sql() ✅

**Recommended approach:**

```python
# Execute SQL and get DataFrame
df_sql = spark.sql("""
    SELECT 
        item_identifier,
        item_weight,
        item_MRP
    FROM my_view
    WHERE item_fat_content = 'LF'
""")

df_sql.display()
```

**Benefits:**
- Returns DataFrame (can continue transformations)
- Can assign to variable
- Can chain operations
- More flexible

### Complex SQL Example

```python
# Complex query with joins and aggregations
df_result = spark.sql("""
    SELECT 
        item_type,
        outlet_size,
        COUNT(*) as count,
        AVG(item_MRP) as avg_price,
        SUM(item_outlet_sales) as total_sales
    FROM my_view
    WHERE outlet_size IS NOT NULL
    GROUP BY item_type, outlet_size
    ORDER BY total_sales DESC
""")

df_result.display()
```

### Window Functions in SQL

```python
df_window = spark.sql("""
    SELECT 
        item_identifier,
        item_MRP,
        ROW_NUMBER() OVER (ORDER BY item_MRP DESC) as rank,
        SUM(item_MRP) OVER (
            ORDER BY item_type 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumsum
    FROM my_view
""")

df_window.display()
```

### Mixing SQL and PySpark

```python
# Use SQL for complex logic
df_sql = spark.sql("SELECT * FROM my_view WHERE item_MRP > 100")

# Continue with PySpark transformations
df_final = df_sql \
    .withColumn("price_category", 
                when(col("item_MRP") < 150, "Medium")
                .otherwise("Expensive")) \
    .filter(col("item_type") == "Dairy")

df_final.display()
```

---

## 27. Complete End-to-End Example

### Scenario: Sales Analysis Pipeline

```python
# ============================================
# STEP 1: READ DATA
# ============================================
df_sales = spark.read \
    .format("csv") \
    .option("inferSchema", True) \
    .option("header", True) \
    .load("/FileStore/tables/bigmart_sales.csv")

# ============================================
# STEP 2: DATA CLEANING
# ============================================
from pyspark.sql.functions import *

# Remove duplicates
df_clean = df_sales.dropDuplicates()

# Handle nulls
df_clean = df_clean.fillna("Unknown", subset=["outlet_size"])

# Standardize text
df_clean = df_clean.withColumn(
    "item_fat_content",
    when(col("item_fat_content") == "Regular", "R")
    .when(col("item_fat_content") == "Low Fat", "LF")
    .otherwise(col("item_fat_content"))
)

# ============================================
# STEP 3: FEATURE ENGINEERING
# ============================================

# Add price category
df_clean = df_clean.withColumn(
    "price_category",
    when(col("item_MRP") < 100, "Low")
    .when(col("item_MRP") < 200, "Medium")
    .otherwise("High")
)

# Add sales per unit weight
df_clean = df_clean.withColumn(
    "sales_per_weight",
    col("item_outlet_sales") / col("item_weight")
)

# ============================================
# STEP 4: AGGREGATIONS
# ============================================

# Sales by category and outlet
df_agg = df_clean.groupBy("item_type", "outlet_type") \
    .agg(
        sum("item_outlet_sales").alias("total_sales"),
        avg("item_MRP").alias("avg_price"),
        count("*").alias("item_count")
    )

# ============================================
# STEP 5: WINDOW FUNCTIONS
# ============================================

from pyspark.sql.window import Window

# Rank items by sales within each category
window_spec = Window.partitionBy("item_type").orderBy(col("total_sales").desc())

df_ranked = df_agg.withColumn(
    "sales_rank",
    rank().over(window_spec)
)

# ============================================
# STEP 6: WRITE RESULTS
# ============================================

# Write to Delta (production format)
df_ranked.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/FileStore/tables/delta/sales_analysis") \
    .saveAsTable("sales_analysis")

# ============================================
# STEP 7: CREATE SQL VIEW FOR REPORTING
# ============================================

df_ranked.createTempView("sales_summary")

# Query with SQL
df_top_performers = spark.sql("""
    SELECT 
        item_type,
        outlet_type,
        total_sales,
        avg_price,
        sales_rank
    FROM sales_summary
    WHERE sales_rank <= 3
    ORDER BY item_type, sales_rank
""")

df_top_performers.display()
```

---

## 28. Best Practices & Tips

### Performance Optimization

```python
# ❌ BAD: Multiple passes over data
df.filter(col("A") > 10).count()
df.filter(col("B") < 5).count()

# ✅ GOOD: Single pass with cache
df_filtered = df.filter((col("A") > 10) | (col("B") < 5)).cache()
df_filtered.count()
```

### Avoid UDFs When Possible

```python
# ❌ BAD: UDF
def add_ten(x):
    return x + 10
    
add_ten_udf = udf(add_ten)
df.withColumn("result", add_ten_udf(col("value")))

# ✅ GOOD: Built-in function
df.withColumn("result", col("value") + 10)
```

### Use Appropriate Joins

```python
# ✅ Use left join when you can't lose left table data
df1.join(df2, "key", "left")

# ✅ Use anti join to find missing records
df1.join(df2, "key", "anti")
```

### Partition Data for Large Datasets

```python
# Write partitioned data
df.write \
    .format("parquet") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save("/path/to/data")
```

### Use Delta Lake in Production

```python
# ✅ Production standard
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "/your/storage/path") \
    .saveAsTable("production_table")
```

---

## 29. Common Interview Questions

### Q1: Explain Spark Architecture
**Answer:** Spark uses master-slave architecture with Driver Node orchestrating tasks and Worker Nodes executing them.

### Q2: What is lazy evaluation?
**Answer:** Transformations aren't executed until an action is called. Spark builds an execution plan and optimizes it before running.

### Q3: Difference between transformation and action?
**Answer:** 
- Transformations (lazy): select, filter, groupBy
- Actions (trigger execution): show, display, collect, count

### Q4: Difference between rank and dense_rank?
**Answer:**
- rank: Skips numbers after ties (1,1,3)
- dense_rank: No gaps (1,1,2)

### Q5: When to use UDF?
**Answer:** Only when built-in functions can't solve the problem. UDFs have performance overhead.

### Q6: Managed vs External tables?
**Answer:**
- Managed: Databricks controls data, DROP deletes data
- External: You control data, DROP keeps data ✅

### Q7: How to remove duplicates?
**Answer:**
```python
# Method 1
df.dropDuplicates()

# Method 2: Based on columns
df.dropDuplicates(subset=["col1", "col2"])

# Method 3: Using window function
window_spec = Window.partitionBy("col1").orderBy("col2")
df.withColumn("rn", row_number().over(window_spec)) \
  .filter(col("rn") == 1) \
  .drop("rn")
```

---

## 30. Cheat Sheet

### Essential Imports
```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

### Quick Reference

```python
# READ
df = spark.read.format("csv").option("header", True).load("path")

# SELECT
df.select("col1", "col2")
df.select(col("col1").alias("new_name"))

# FILTER
df.filter(col("col1") > 10)
df.filter((col("col1") > 10) & (col("col2") == "value"))

# ADD/MODIFY COLUMN
df.withColumn("new_col", lit("value"))
df.withColumn("new_col", col("col1") * 2)

# RENAME
df.withColumnRenamed("old", "new")

# DROP
df.drop("col1", "col2")

# AGGREGATION
df.groupBy("col1").agg(sum("col2"), avg("col3"))

# JOIN
df1.join(df2, df1["key"] == df2["key"], "left")

# WINDOW
window_spec = Window.orderBy("col1")
df.withColumn("rn", row_number().over(window_spec))

# WRITE
df.write.format("delta").mode("overwrite").save("path")

# SQL
df.createTempView("view_name")
spark.sql("SELECT * FROM view_name")
```

---

