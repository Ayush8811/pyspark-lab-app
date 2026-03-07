"""
Seed script: Populate the practice_list_problems table with 18 curated
Window Functions problems (both PySpark and SQL variants).

Safe to run multiple times — skips problems that already exist (by title).

Usage:
  python seed_practice_list.py
"""
from database import engine, SessionLocal
from models import Base, PracticeListProblem

PROBLEMS = [
    # =====================================================================
    # 1. ROW_NUMBER — Easy — HR
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Rank Employees by Salary Within Department",
        "description": (
            "You are given an `employees` table with employee details. "
            "For each department, assign a **row number** to employees ordered by salary (highest first). "
            "Return the columns: `name`, `department`, `salary`, `salary_rank`.\n\n"
            "Order the final output by `department` ascending, then `salary_rank` ascending."
        ),
        "difficulty": "Easy",
        "window_function_type": "ROW_NUMBER",
        "use_case_category": "HR",
        "order_index": 1,
        "datasets": {
            "employees": [
                {"employee_id": 1, "name": "Alice", "department": "Engineering", "salary": 95000},
                {"employee_id": 2, "name": "Bob", "department": "Engineering", "salary": 105000},
                {"employee_id": 3, "name": "Charlie", "department": "Engineering", "salary": 90000},
                {"employee_id": 4, "name": "Diana", "department": "Marketing", "salary": 80000},
                {"employee_id": 5, "name": "Eve", "department": "Marketing", "salary": 85000},
                {"employee_id": 6, "name": "Frank", "department": "Marketing", "salary": 78000},
                {"employee_id": 7, "name": "Grace", "department": "Sales", "salary": 70000},
                {"employee_id": 8, "name": "Hank", "department": "Sales", "salary": 75000},
            ]
        },
        "expected_output": [
            {"name": "Bob", "department": "Engineering", "salary": 105000, "salary_rank": 1},
            {"name": "Alice", "department": "Engineering", "salary": 95000, "salary_rank": 2},
            {"name": "Charlie", "department": "Engineering", "salary": 90000, "salary_rank": 3},
            {"name": "Eve", "department": "Marketing", "salary": 85000, "salary_rank": 1},
            {"name": "Diana", "department": "Marketing", "salary": 80000, "salary_rank": 2},
            {"name": "Frank", "department": "Marketing", "salary": 78000, "salary_rank": 3},
            {"name": "Hank", "department": "Sales", "salary": 75000, "salary_rank": 1},
            {"name": "Grace", "department": "Sales", "salary": 70000, "salary_rank": 2},
        ],
        "initial_code_pyspark": "# Assign a row number to employees ranked by salary (desc) within each department\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import row_number, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Assign a row number to employees ranked by salary (desc) within each department\n-- Columns: name, department, salary, salary_rank\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import row_number, col\n\n"
            "window_spec = Window.partitionBy('department').orderBy(col('salary').desc())\n"
            "final_df = spark.table('employees').withColumn('salary_rank', row_number().over(window_spec))"
            ".select('name', 'department', 'salary', 'salary_rank')"
            ".orderBy('department', 'salary_rank')"
        ),
        "solution_code_sql": (
            "SELECT name, department, salary,\n"
            "       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank\n"
            "FROM employees\n"
            "ORDER BY department, salary_rank"
        ),
        "explanation": (
            "**ROW_NUMBER()** assigns a unique sequential integer to each row within a partition.\n\n"
            "- **PARTITION BY department**: Restarts numbering for each department.\n"
            "- **ORDER BY salary DESC**: Highest salary gets rank 1.\n\n"
            "Unlike RANK or DENSE_RANK, ROW_NUMBER always produces unique values — no ties."
        ),
    },
    # =====================================================================
    # 2. ROW_NUMBER — Medium — E-Commerce (Deduplication)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Deduplicate Customer Orders by Latest Date",
        "description": (
            "An `orders` table has duplicate entries per customer (multiple orders). "
            "For each customer, keep only the **most recent order** (latest `order_date`). "
            "Use `ROW_NUMBER()` to identify the latest order per customer, then filter to keep only row number 1.\n\n"
            "Return: `customer_id`, `customer_name`, `order_date`, `amount`.\n"
            "Order by `customer_id` ascending."
        ),
        "difficulty": "Medium",
        "window_function_type": "ROW_NUMBER",
        "use_case_category": "E-Commerce",
        "order_index": 2,
        "datasets": {
            "orders": [
                {"order_id": 101, "customer_id": 1, "customer_name": "Alice", "order_date": "2024-01-15", "amount": 250},
                {"order_id": 102, "customer_id": 1, "customer_name": "Alice", "order_date": "2024-03-20", "amount": 180},
                {"order_id": 103, "customer_id": 2, "customer_name": "Bob", "order_date": "2024-02-10", "amount": 320},
                {"order_id": 104, "customer_id": 2, "customer_name": "Bob", "order_date": "2024-04-05", "amount": 150},
                {"order_id": 105, "customer_id": 2, "customer_name": "Bob", "order_date": "2024-01-25", "amount": 400},
                {"order_id": 106, "customer_id": 3, "customer_name": "Charlie", "order_date": "2024-05-01", "amount": 500},
                {"order_id": 107, "customer_id": 4, "customer_name": "Diana", "order_date": "2024-02-28", "amount": 120},
                {"order_id": 108, "customer_id": 4, "customer_name": "Diana", "order_date": "2024-06-15", "amount": 275},
            ]
        },
        "expected_output": [
            {"customer_id": 1, "customer_name": "Alice", "order_date": "2024-03-20", "amount": 180},
            {"customer_id": 2, "customer_name": "Bob", "order_date": "2024-04-05", "amount": 150},
            {"customer_id": 3, "customer_name": "Charlie", "order_date": "2024-05-01", "amount": 500},
            {"customer_id": 4, "customer_name": "Diana", "order_date": "2024-06-15", "amount": 275},
        ],
        "initial_code_pyspark": "# Keep only the most recent order per customer using ROW_NUMBER\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import row_number, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Keep only the most recent order per customer using ROW_NUMBER\n-- Columns: customer_id, customer_name, order_date, amount\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import row_number, col\n\n"
            "window_spec = Window.partitionBy('customer_id').orderBy(col('order_date').desc())\n"
            "ranked = spark.table('orders').withColumn('rn', row_number().over(window_spec))\n"
            "final_df = ranked.filter(col('rn') == 1).select('customer_id', 'customer_name', 'order_date', 'amount').orderBy('customer_id')"
        ),
        "solution_code_sql": (
            "SELECT customer_id, customer_name, order_date, amount\n"
            "FROM (\n"
            "    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn\n"
            "    FROM orders\n"
            ") ranked\n"
            "WHERE rn = 1\n"
            "ORDER BY customer_id"
        ),
        "explanation": (
            "This is the classic **deduplication pattern** using ROW_NUMBER.\n\n"
            "1. Assign ROW_NUMBER partitioned by `customer_id`, ordered by `order_date DESC`.\n"
            "2. The most recent order gets `rn = 1`.\n"
            "3. Filter to keep only `rn = 1` rows.\n\n"
            "This pattern is widely used in ETL pipelines to keep the latest record per entity."
        ),
    },
    # =====================================================================
    # 3. RANK — Easy — Sports
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Rank Athletes by Score with Ties",
        "description": (
            "A `competition` table records athletes and their scores. "
            "Rank athletes by score (highest first) using `RANK()`. "
            "Athletes with the same score should get the same rank, and the next rank should skip accordingly.\n\n"
            "Return: `athlete_name`, `sport`, `score`, `rank`.\n"
            "Order by `rank` ascending, then `athlete_name` ascending."
        ),
        "difficulty": "Easy",
        "window_function_type": "RANK",
        "use_case_category": "Sports",
        "order_index": 3,
        "datasets": {
            "competition": [
                {"athlete_id": 1, "athlete_name": "Alex", "sport": "Swimming", "score": 95},
                {"athlete_id": 2, "athlete_name": "Beth", "sport": "Swimming", "score": 92},
                {"athlete_id": 3, "athlete_name": "Carl", "sport": "Swimming", "score": 95},
                {"athlete_id": 4, "athlete_name": "Dana", "sport": "Swimming", "score": 88},
                {"athlete_id": 5, "athlete_name": "Eric", "sport": "Swimming", "score": 92},
                {"athlete_id": 6, "athlete_name": "Faye", "sport": "Swimming", "score": 85},
            ]
        },
        "expected_output": [
            {"athlete_name": "Alex", "sport": "Swimming", "score": 95, "rank": 1},
            {"athlete_name": "Carl", "sport": "Swimming", "score": 95, "rank": 1},
            {"athlete_name": "Beth", "sport": "Swimming", "score": 92, "rank": 3},
            {"athlete_name": "Eric", "sport": "Swimming", "score": 92, "rank": 3},
            {"athlete_name": "Dana", "sport": "Swimming", "score": 88, "rank": 5},
            {"athlete_name": "Faye", "sport": "Swimming", "score": 85, "rank": 6},
        ],
        "initial_code_pyspark": "# Rank athletes by score (desc) using RANK — ties get the same rank\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import rank, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Rank athletes by score (desc) using RANK\n-- Columns: athlete_name, sport, score, rank\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import rank, col\n\n"
            "window_spec = Window.orderBy(col('score').desc())\n"
            "final_df = spark.table('competition').withColumn('rank', rank().over(window_spec))"
            ".select('athlete_name', 'sport', 'score', 'rank')"
            ".orderBy('rank', 'athlete_name')"
        ),
        "solution_code_sql": (
            "SELECT athlete_name, sport, score,\n"
            "       RANK() OVER (ORDER BY score DESC) AS rank\n"
            "FROM competition\n"
            "ORDER BY rank, athlete_name"
        ),
        "explanation": (
            "**RANK()** assigns the same rank to tied values, then skips the next rank(s).\n\n"
            "For example, if two athletes tie at rank 1, the next athlete gets rank 3 (rank 2 is skipped).\n\n"
            "Compare with:\n"
            "- **ROW_NUMBER**: No ties — always unique sequential numbers.\n"
            "- **DENSE_RANK**: Same rank for ties, but no gaps (next rank is always +1)."
        ),
    },
    # =====================================================================
    # 4. DENSE_RANK — Easy — Education
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Dense Rank Students by GPA",
        "description": (
            "A `students` table contains student GPAs. "
            "Assign a **dense rank** to students by GPA (highest first) within each `major`. "
            "Unlike RANK, DENSE_RANK does not skip ranks after ties.\n\n"
            "Return: `student_name`, `major`, `gpa`, `gpa_rank`.\n"
            "Order by `major` ascending, then `gpa_rank` ascending."
        ),
        "difficulty": "Easy",
        "window_function_type": "DENSE_RANK",
        "use_case_category": "Education",
        "order_index": 4,
        "datasets": {
            "students": [
                {"student_id": 1, "student_name": "Alice", "major": "CS", "gpa": 3.9},
                {"student_id": 2, "student_name": "Bob", "major": "CS", "gpa": 3.7},
                {"student_id": 3, "student_name": "Charlie", "major": "CS", "gpa": 3.9},
                {"student_id": 4, "student_name": "Diana", "major": "CS", "gpa": 3.5},
                {"student_id": 5, "student_name": "Eve", "major": "Math", "gpa": 3.8},
                {"student_id": 6, "student_name": "Frank", "major": "Math", "gpa": 3.8},
                {"student_id": 7, "student_name": "Grace", "major": "Math", "gpa": 3.6},
            ]
        },
        "expected_output": [
            {"student_name": "Alice", "major": "CS", "gpa": 3.9, "gpa_rank": 1},
            {"student_name": "Charlie", "major": "CS", "gpa": 3.9, "gpa_rank": 1},
            {"student_name": "Bob", "major": "CS", "gpa": 3.7, "gpa_rank": 2},
            {"student_name": "Diana", "major": "CS", "gpa": 3.5, "gpa_rank": 3},
            {"student_name": "Eve", "major": "Math", "gpa": 3.8, "gpa_rank": 1},
            {"student_name": "Frank", "major": "Math", "gpa": 3.8, "gpa_rank": 1},
            {"student_name": "Grace", "major": "Math", "gpa": 3.6, "gpa_rank": 2},
        ],
        "initial_code_pyspark": "# Dense rank students by GPA within each major\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import dense_rank, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Dense rank students by GPA (desc) within each major\n-- Columns: student_name, major, gpa, gpa_rank\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import dense_rank, col\n\n"
            "window_spec = Window.partitionBy('major').orderBy(col('gpa').desc())\n"
            "final_df = spark.table('students').withColumn('gpa_rank', dense_rank().over(window_spec))"
            ".select('student_name', 'major', 'gpa', 'gpa_rank')"
            ".orderBy('major', 'gpa_rank', 'student_name')"
        ),
        "solution_code_sql": (
            "SELECT student_name, major, gpa,\n"
            "       DENSE_RANK() OVER (PARTITION BY major ORDER BY gpa DESC) AS gpa_rank\n"
            "FROM students\n"
            "ORDER BY major, gpa_rank, student_name"
        ),
        "explanation": (
            "**DENSE_RANK()** works like RANK but without gaps.\n\n"
            "If two students tie at rank 1, the next student gets rank 2 (not 3).\n\n"
            "Use DENSE_RANK when you need consecutive ranking (e.g., top-N without gaps)."
        ),
    },
    # =====================================================================
    # 5. DENSE_RANK — Medium — E-Commerce (Top-N per group)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Top 2 Best-Selling Products per Category",
        "description": (
            "A `product_sales` table tracks total units sold per product. "
            "Find the **top 2 best-selling products** in each category using DENSE_RANK. "
            "If products are tied, include all tied products.\n\n"
            "Return: `product_name`, `category`, `units_sold`, `sales_rank`.\n"
            "Only include rows where `sales_rank <= 2`.\n"
            "Order by `category` ascending, then `sales_rank` ascending."
        ),
        "difficulty": "Medium",
        "window_function_type": "DENSE_RANK",
        "use_case_category": "E-Commerce",
        "order_index": 5,
        "datasets": {
            "product_sales": [
                {"product_id": 1, "product_name": "Laptop", "category": "Electronics", "units_sold": 500},
                {"product_id": 2, "product_name": "Phone", "category": "Electronics", "units_sold": 800},
                {"product_id": 3, "product_name": "Tablet", "category": "Electronics", "units_sold": 300},
                {"product_id": 4, "product_name": "Headphones", "category": "Electronics", "units_sold": 800},
                {"product_id": 5, "product_name": "T-Shirt", "category": "Clothing", "units_sold": 1200},
                {"product_id": 6, "product_name": "Jeans", "category": "Clothing", "units_sold": 900},
                {"product_id": 7, "product_name": "Jacket", "category": "Clothing", "units_sold": 600},
            ]
        },
        "expected_output": [
            {"product_name": "T-Shirt", "category": "Clothing", "units_sold": 1200, "sales_rank": 1},
            {"product_name": "Jeans", "category": "Clothing", "units_sold": 900, "sales_rank": 2},
            {"product_name": "Headphones", "category": "Electronics", "units_sold": 800, "sales_rank": 1},
            {"product_name": "Phone", "category": "Electronics", "units_sold": 800, "sales_rank": 1},
            {"product_name": "Laptop", "category": "Electronics", "units_sold": 500, "sales_rank": 2},
        ],
        "initial_code_pyspark": "# Find top 2 best-selling products per category using DENSE_RANK\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import dense_rank, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Find top 2 best-selling products per category using DENSE_RANK\n-- Columns: product_name, category, units_sold, sales_rank\n-- Only include sales_rank <= 2\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import dense_rank, col\n\n"
            "window_spec = Window.partitionBy('category').orderBy(col('units_sold').desc())\n"
            "ranked = spark.table('product_sales').withColumn('sales_rank', dense_rank().over(window_spec))\n"
            "final_df = ranked.filter(col('sales_rank') <= 2)"
            ".select('product_name', 'category', 'units_sold', 'sales_rank')"
            ".orderBy('category', 'sales_rank', 'product_name')"
        ),
        "solution_code_sql": (
            "SELECT product_name, category, units_sold, sales_rank\n"
            "FROM (\n"
            "    SELECT product_name, category, units_sold,\n"
            "           DENSE_RANK() OVER (PARTITION BY category ORDER BY units_sold DESC) AS sales_rank\n"
            "    FROM product_sales\n"
            ") ranked\n"
            "WHERE sales_rank <= 2\n"
            "ORDER BY category, sales_rank, product_name"
        ),
        "explanation": (
            "The **Top-N per group** pattern uses DENSE_RANK to find the best items within each category.\n\n"
            "1. Partition by `category` and order by `units_sold DESC`.\n"
            "2. DENSE_RANK assigns the same rank to tied values without gaps.\n"
            "3. Filter to keep only `sales_rank <= 2`.\n\n"
            "DENSE_RANK is preferred over RANK here because we want exactly top-2 tiers, not top-2 positions."
        ),
    },
    # =====================================================================
    # 6. NTILE — Medium — Finance
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Assign Customers to Spending Quartiles",
        "description": (
            "A `customer_spending` table records total annual spending. "
            "Divide customers into **4 quartiles** based on their spending (highest spending = quartile 1). "
            "Use `NTILE(4)` to bucket customers.\n\n"
            "Return: `customer_name`, `annual_spending`, `spending_quartile`.\n"
            "Order by `spending_quartile` ascending, then `annual_spending` descending."
        ),
        "difficulty": "Medium",
        "window_function_type": "NTILE",
        "use_case_category": "Finance",
        "order_index": 6,
        "datasets": {
            "customer_spending": [
                {"customer_id": 1, "customer_name": "Alice", "annual_spending": 15000},
                {"customer_id": 2, "customer_name": "Bob", "annual_spending": 8000},
                {"customer_id": 3, "customer_name": "Charlie", "annual_spending": 22000},
                {"customer_id": 4, "customer_name": "Diana", "annual_spending": 5000},
                {"customer_id": 5, "customer_name": "Eve", "annual_spending": 30000},
                {"customer_id": 6, "customer_name": "Frank", "annual_spending": 12000},
                {"customer_id": 7, "customer_name": "Grace", "annual_spending": 3000},
                {"customer_id": 8, "customer_name": "Hank", "annual_spending": 18000},
            ]
        },
        "expected_output": [
            {"customer_name": "Eve", "annual_spending": 30000, "spending_quartile": 1},
            {"customer_name": "Charlie", "annual_spending": 22000, "spending_quartile": 1},
            {"customer_name": "Hank", "annual_spending": 18000, "spending_quartile": 2},
            {"customer_name": "Alice", "annual_spending": 15000, "spending_quartile": 2},
            {"customer_name": "Frank", "annual_spending": 12000, "spending_quartile": 3},
            {"customer_name": "Bob", "annual_spending": 8000, "spending_quartile": 3},
            {"customer_name": "Diana", "annual_spending": 5000, "spending_quartile": 4},
            {"customer_name": "Grace", "annual_spending": 3000, "spending_quartile": 4},
        ],
        "initial_code_pyspark": "# Divide customers into 4 spending quartiles using NTILE\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import ntile, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Divide customers into 4 spending quartiles using NTILE\n-- Columns: customer_name, annual_spending, spending_quartile\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import ntile, col\n\n"
            "window_spec = Window.orderBy(col('annual_spending').desc())\n"
            "final_df = spark.table('customer_spending').withColumn('spending_quartile', ntile(4).over(window_spec))"
            ".select('customer_name', 'annual_spending', 'spending_quartile')"
            ".orderBy('spending_quartile', col('annual_spending').desc())"
        ),
        "solution_code_sql": (
            "SELECT customer_name, annual_spending,\n"
            "       NTILE(4) OVER (ORDER BY annual_spending DESC) AS spending_quartile\n"
            "FROM customer_spending\n"
            "ORDER BY spending_quartile, annual_spending DESC"
        ),
        "explanation": (
            "**NTILE(n)** divides rows into `n` approximately equal groups (buckets).\n\n"
            "With 8 rows and NTILE(4), each quartile gets 2 rows.\n"
            "If rows don't divide evenly, earlier buckets get one extra row.\n\n"
            "Common uses: percentile analysis, customer segmentation, creating balanced groups."
        ),
    },
    # =====================================================================
    # 7. LAG — Medium — Finance (Month-over-Month)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Month-over-Month Revenue Change",
        "description": (
            "A `monthly_revenue` table tracks revenue per month. "
            "Calculate the **previous month's revenue** using `LAG` and compute the "
            "month-over-month change (`current - previous`).\n\n"
            "Return: `month`, `revenue`, `prev_revenue`, `revenue_change`.\n"
            "The first month should have `NULL` for `prev_revenue` and `revenue_change`.\n"
            "Order by `month` ascending."
        ),
        "difficulty": "Medium",
        "window_function_type": "LAG",
        "use_case_category": "Finance",
        "order_index": 7,
        "datasets": {
            "monthly_revenue": [
                {"month": "2024-01", "revenue": 50000},
                {"month": "2024-02", "revenue": 55000},
                {"month": "2024-03", "revenue": 48000},
                {"month": "2024-04", "revenue": 62000},
                {"month": "2024-05", "revenue": 58000},
                {"month": "2024-06", "revenue": 70000},
            ]
        },
        "expected_output": [
            {"month": "2024-01", "revenue": 50000, "prev_revenue": None, "revenue_change": None},
            {"month": "2024-02", "revenue": 55000, "prev_revenue": 50000, "revenue_change": 5000},
            {"month": "2024-03", "revenue": 48000, "prev_revenue": 55000, "revenue_change": -7000},
            {"month": "2024-04", "revenue": 62000, "prev_revenue": 48000, "revenue_change": 14000},
            {"month": "2024-05", "revenue": 58000, "prev_revenue": 62000, "revenue_change": -4000},
            {"month": "2024-06", "revenue": 70000, "prev_revenue": 58000, "revenue_change": 12000},
        ],
        "initial_code_pyspark": "# Calculate month-over-month revenue change using LAG\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import lag, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate month-over-month revenue change using LAG\n-- Columns: month, revenue, prev_revenue, revenue_change\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import lag, col\n\n"
            "window_spec = Window.orderBy('month')\n"
            "df = spark.table('monthly_revenue')\n"
            "df = df.withColumn('prev_revenue', lag('revenue', 1).over(window_spec))\n"
            "df = df.withColumn('revenue_change', col('revenue') - col('prev_revenue'))\n"
            "final_df = df.select('month', 'revenue', 'prev_revenue', 'revenue_change').orderBy('month')"
        ),
        "solution_code_sql": (
            "SELECT month, revenue,\n"
            "       LAG(revenue, 1) OVER (ORDER BY month) AS prev_revenue,\n"
            "       revenue - LAG(revenue, 1) OVER (ORDER BY month) AS revenue_change\n"
            "FROM monthly_revenue\n"
            "ORDER BY month"
        ),
        "explanation": (
            "**LAG(column, offset)** accesses the value from a previous row.\n\n"
            "- `LAG(revenue, 1)` gets the revenue from 1 row before (the previous month).\n"
            "- The first row has no previous row, so LAG returns NULL.\n"
            "- `revenue_change` is simply `current - previous`.\n\n"
            "LAG is essential for time-series analysis: comparing periods, detecting changes, calculating growth rates."
        ),
    },
    # =====================================================================
    # 8. LAG — Hard — IoT (Session Gap Detection)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Detect Session Gaps in IoT Sensor Data",
        "description": (
            "An `sensor_readings` table logs timestamps of IoT sensor events for different devices. "
            "Use `LAG` to find the **time difference** (in minutes) between consecutive readings per device. "
            "Flag readings where the gap exceeds 30 minutes as a `new_session` (1 = new session, 0 = same session).\n\n"
            "Return: `device_id`, `reading_time`, `prev_reading_time`, `gap_minutes`, `new_session`.\n"
            "The first reading per device should have `NULL` gap and `new_session = 1`.\n"
            "Order by `device_id`, `reading_time`."
        ),
        "difficulty": "Hard",
        "window_function_type": "LAG",
        "use_case_category": "IoT",
        "order_index": 8,
        "datasets": {
            "sensor_readings": [
                {"reading_id": 1, "device_id": "D1", "reading_time": "2024-01-01 08:00"},
                {"reading_id": 2, "device_id": "D1", "reading_time": "2024-01-01 08:10"},
                {"reading_id": 3, "device_id": "D1", "reading_time": "2024-01-01 08:20"},
                {"reading_id": 4, "device_id": "D1", "reading_time": "2024-01-01 09:15"},
                {"reading_id": 5, "device_id": "D1", "reading_time": "2024-01-01 09:25"},
                {"reading_id": 6, "device_id": "D2", "reading_time": "2024-01-01 10:00"},
                {"reading_id": 7, "device_id": "D2", "reading_time": "2024-01-01 10:05"},
                {"reading_id": 8, "device_id": "D2", "reading_time": "2024-01-01 11:00"},
            ]
        },
        "expected_output": [
            {"device_id": "D1", "reading_time": "2024-01-01 08:00", "prev_reading_time": None, "gap_minutes": None, "new_session": 1},
            {"device_id": "D1", "reading_time": "2024-01-01 08:10", "prev_reading_time": "2024-01-01 08:00", "gap_minutes": 10, "new_session": 0},
            {"device_id": "D1", "reading_time": "2024-01-01 08:20", "prev_reading_time": "2024-01-01 08:10", "gap_minutes": 10, "new_session": 0},
            {"device_id": "D1", "reading_time": "2024-01-01 09:15", "prev_reading_time": "2024-01-01 08:20", "gap_minutes": 55, "new_session": 1},
            {"device_id": "D1", "reading_time": "2024-01-01 09:25", "prev_reading_time": "2024-01-01 09:15", "gap_minutes": 10, "new_session": 0},
            {"device_id": "D2", "reading_time": "2024-01-01 10:00", "prev_reading_time": None, "gap_minutes": None, "new_session": 1},
            {"device_id": "D2", "reading_time": "2024-01-01 10:05", "prev_reading_time": "2024-01-01 10:00", "gap_minutes": 5, "new_session": 0},
            {"device_id": "D2", "reading_time": "2024-01-01 11:00", "prev_reading_time": "2024-01-01 10:05", "gap_minutes": 55, "new_session": 1},
        ],
        "initial_code_pyspark": "# Detect session gaps using LAG — flag gaps > 30 minutes as new sessions\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import lag, col, unix_timestamp, when\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Detect session gaps using LAG — flag gaps > 30 minutes as new sessions\n-- Columns: device_id, reading_time, prev_reading_time, gap_minutes, new_session\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import lag, col, unix_timestamp, when\n\n"
            "window_spec = Window.partitionBy('device_id').orderBy('reading_time')\n"
            "df = spark.table('sensor_readings')\n"
            "df = df.withColumn('prev_reading_time', lag('reading_time', 1).over(window_spec))\n"
            "df = df.withColumn('gap_minutes',\n"
            "    ((unix_timestamp('reading_time', 'yyyy-MM-dd HH:mm') - unix_timestamp('prev_reading_time', 'yyyy-MM-dd HH:mm')) / 60).cast('int')\n"
            ")\n"
            "df = df.withColumn('new_session', when(col('prev_reading_time').isNull() | (col('gap_minutes') > 30), 1).otherwise(0))\n"
            "final_df = df.select('device_id', 'reading_time', 'prev_reading_time', 'gap_minutes', 'new_session').orderBy('device_id', 'reading_time')"
        ),
        "solution_code_sql": (
            "SELECT device_id, reading_time,\n"
            "       LAG(reading_time, 1) OVER (PARTITION BY device_id ORDER BY reading_time) AS prev_reading_time,\n"
            "       CAST((CAST(strftime('%s', reading_time) AS INT) - CAST(strftime('%s', LAG(reading_time, 1) OVER (PARTITION BY device_id ORDER BY reading_time)) AS INT)) / 60 AS INT) AS gap_minutes,\n"
            "       CASE\n"
            "           WHEN LAG(reading_time, 1) OVER (PARTITION BY device_id ORDER BY reading_time) IS NULL THEN 1\n"
            "           WHEN CAST((CAST(strftime('%s', reading_time) AS INT) - CAST(strftime('%s', LAG(reading_time, 1) OVER (PARTITION BY device_id ORDER BY reading_time)) AS INT)) / 60 AS INT) > 30 THEN 1\n"
            "           ELSE 0\n"
            "       END AS new_session\n"
            "FROM sensor_readings\n"
            "ORDER BY device_id, reading_time"
        ),
        "explanation": (
            "This problem combines **LAG** with **time calculations** for session detection.\n\n"
            "1. Use LAG partitioned by `device_id` to get the previous reading time.\n"
            "2. Calculate the gap in minutes between consecutive readings.\n"
            "3. Flag a new session when the gap exceeds 30 minutes or there is no previous reading.\n\n"
            "This is a common pattern in clickstream analysis, IoT monitoring, and user session tracking."
        ),
    },
    # =====================================================================
    # 9. LEAD — Medium — Logistics
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Predict Next Delivery Date per Customer",
        "description": (
            "A `deliveries` table tracks delivery dates per customer. "
            "Use `LEAD` to find the **next delivery date** for each customer.\n\n"
            "Return: `customer_name`, `delivery_date`, `next_delivery_date`.\n"
            "The last delivery per customer should have `NULL` for `next_delivery_date`.\n"
            "Order by `customer_name`, `delivery_date`."
        ),
        "difficulty": "Medium",
        "window_function_type": "LEAD",
        "use_case_category": "Logistics",
        "order_index": 9,
        "datasets": {
            "deliveries": [
                {"delivery_id": 1, "customer_name": "Alice", "delivery_date": "2024-01-10"},
                {"delivery_id": 2, "customer_name": "Alice", "delivery_date": "2024-02-15"},
                {"delivery_id": 3, "customer_name": "Alice", "delivery_date": "2024-04-20"},
                {"delivery_id": 4, "customer_name": "Bob", "delivery_date": "2024-01-05"},
                {"delivery_id": 5, "customer_name": "Bob", "delivery_date": "2024-03-12"},
                {"delivery_id": 6, "customer_name": "Charlie", "delivery_date": "2024-02-01"},
                {"delivery_id": 7, "customer_name": "Charlie", "delivery_date": "2024-02-28"},
                {"delivery_id": 8, "customer_name": "Charlie", "delivery_date": "2024-05-15"},
            ]
        },
        "expected_output": [
            {"customer_name": "Alice", "delivery_date": "2024-01-10", "next_delivery_date": "2024-02-15"},
            {"customer_name": "Alice", "delivery_date": "2024-02-15", "next_delivery_date": "2024-04-20"},
            {"customer_name": "Alice", "delivery_date": "2024-04-20", "next_delivery_date": None},
            {"customer_name": "Bob", "delivery_date": "2024-01-05", "next_delivery_date": "2024-03-12"},
            {"customer_name": "Bob", "delivery_date": "2024-03-12", "next_delivery_date": None},
            {"customer_name": "Charlie", "delivery_date": "2024-02-01", "next_delivery_date": "2024-02-28"},
            {"customer_name": "Charlie", "delivery_date": "2024-02-28", "next_delivery_date": "2024-05-15"},
            {"customer_name": "Charlie", "delivery_date": "2024-05-15", "next_delivery_date": None},
        ],
        "initial_code_pyspark": "# Find the next delivery date per customer using LEAD\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import lead, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Find the next delivery date per customer using LEAD\n-- Columns: customer_name, delivery_date, next_delivery_date\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import lead, col\n\n"
            "window_spec = Window.partitionBy('customer_name').orderBy('delivery_date')\n"
            "final_df = spark.table('deliveries')"
            ".withColumn('next_delivery_date', lead('delivery_date', 1).over(window_spec))"
            ".select('customer_name', 'delivery_date', 'next_delivery_date')"
            ".orderBy('customer_name', 'delivery_date')"
        ),
        "solution_code_sql": (
            "SELECT customer_name, delivery_date,\n"
            "       LEAD(delivery_date, 1) OVER (PARTITION BY customer_name ORDER BY delivery_date) AS next_delivery_date\n"
            "FROM deliveries\n"
            "ORDER BY customer_name, delivery_date"
        ),
        "explanation": (
            "**LEAD(column, offset)** accesses the value from a subsequent row.\n\n"
            "- `LEAD(delivery_date, 1)` gets the delivery date from the next row.\n"
            "- The last row per partition has no next row, so LEAD returns NULL.\n\n"
            "LEAD is the opposite of LAG:\n"
            "- **LAG** looks backward (previous rows).\n"
            "- **LEAD** looks forward (next rows)."
        ),
    },
    # =====================================================================
    # 10. SUM (window) — Easy — Finance (Running Total)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Running Total of Daily Sales",
        "description": (
            "A `daily_sales` table records sales amounts per day. "
            "Calculate a **running total** (cumulative sum) of sales ordered by date.\n\n"
            "Return: `sale_date`, `amount`, `running_total`.\n"
            "Order by `sale_date` ascending."
        ),
        "difficulty": "Easy",
        "window_function_type": "SUM",
        "use_case_category": "Finance",
        "order_index": 10,
        "datasets": {
            "daily_sales": [
                {"sale_id": 1, "sale_date": "2024-01-01", "amount": 200},
                {"sale_id": 2, "sale_date": "2024-01-02", "amount": 350},
                {"sale_id": 3, "sale_date": "2024-01-03", "amount": 150},
                {"sale_id": 4, "sale_date": "2024-01-04", "amount": 400},
                {"sale_id": 5, "sale_date": "2024-01-05", "amount": 275},
                {"sale_id": 6, "sale_date": "2024-01-06", "amount": 500},
            ]
        },
        "expected_output": [
            {"sale_date": "2024-01-01", "amount": 200, "running_total": 200},
            {"sale_date": "2024-01-02", "amount": 350, "running_total": 550},
            {"sale_date": "2024-01-03", "amount": 150, "running_total": 700},
            {"sale_date": "2024-01-04", "amount": 400, "running_total": 1100},
            {"sale_date": "2024-01-05", "amount": 275, "running_total": 1375},
            {"sale_date": "2024-01-06", "amount": 500, "running_total": 1875},
        ],
        "initial_code_pyspark": "# Calculate running total of daily sales\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import sum as spark_sum, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate running total of daily sales\n-- Columns: sale_date, amount, running_total\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import sum as spark_sum, col\n\n"
            "window_spec = Window.orderBy('sale_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)\n"
            "final_df = spark.table('daily_sales')"
            ".withColumn('running_total', spark_sum('amount').over(window_spec))"
            ".select('sale_date', 'amount', 'running_total')"
            ".orderBy('sale_date')"
        ),
        "solution_code_sql": (
            "SELECT sale_date, amount,\n"
            "       SUM(amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total\n"
            "FROM daily_sales\n"
            "ORDER BY sale_date"
        ),
        "explanation": (
            "**SUM() OVER (ORDER BY ...)** with a frame clause creates a running total.\n\n"
            "- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` means: sum from the first row up to the current row.\n"
            "- Each row's `running_total` includes all previous amounts plus the current amount.\n\n"
            "Running totals are fundamental in financial reporting, inventory tracking, and cumulative metrics."
        ),
    },
    # =====================================================================
    # 11. SUM (window) — Medium — E-Commerce (Running total per group)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Cumulative Order Value per Customer",
        "description": (
            "A `customer_orders` table records individual orders. "
            "Calculate the **cumulative order value** for each customer, ordered by order date.\n\n"
            "Return: `customer_name`, `order_date`, `order_amount`, `cumulative_total`.\n"
            "Order by `customer_name`, `order_date`."
        ),
        "difficulty": "Medium",
        "window_function_type": "SUM",
        "use_case_category": "E-Commerce",
        "order_index": 11,
        "datasets": {
            "customer_orders": [
                {"order_id": 1, "customer_name": "Alice", "order_date": "2024-01-05", "order_amount": 120},
                {"order_id": 2, "customer_name": "Alice", "order_date": "2024-02-10", "order_amount": 250},
                {"order_id": 3, "customer_name": "Alice", "order_date": "2024-03-15", "order_amount": 80},
                {"order_id": 4, "customer_name": "Bob", "order_date": "2024-01-20", "order_amount": 300},
                {"order_id": 5, "customer_name": "Bob", "order_date": "2024-02-25", "order_amount": 150},
                {"order_id": 6, "customer_name": "Charlie", "order_date": "2024-01-10", "order_amount": 500},
                {"order_id": 7, "customer_name": "Charlie", "order_date": "2024-04-01", "order_amount": 200},
            ]
        },
        "expected_output": [
            {"customer_name": "Alice", "order_date": "2024-01-05", "order_amount": 120, "cumulative_total": 120},
            {"customer_name": "Alice", "order_date": "2024-02-10", "order_amount": 250, "cumulative_total": 370},
            {"customer_name": "Alice", "order_date": "2024-03-15", "order_amount": 80, "cumulative_total": 450},
            {"customer_name": "Bob", "order_date": "2024-01-20", "order_amount": 300, "cumulative_total": 300},
            {"customer_name": "Bob", "order_date": "2024-02-25", "order_amount": 150, "cumulative_total": 450},
            {"customer_name": "Charlie", "order_date": "2024-01-10", "order_amount": 500, "cumulative_total": 500},
            {"customer_name": "Charlie", "order_date": "2024-04-01", "order_amount": 200, "cumulative_total": 700},
        ],
        "initial_code_pyspark": "# Calculate cumulative order value per customer\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import sum as spark_sum, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate cumulative order value per customer\n-- Columns: customer_name, order_date, order_amount, cumulative_total\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import sum as spark_sum, col\n\n"
            "window_spec = Window.partitionBy('customer_name').orderBy('order_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)\n"
            "final_df = spark.table('customer_orders')"
            ".withColumn('cumulative_total', spark_sum('order_amount').over(window_spec))"
            ".select('customer_name', 'order_date', 'order_amount', 'cumulative_total')"
            ".orderBy('customer_name', 'order_date')"
        ),
        "solution_code_sql": (
            "SELECT customer_name, order_date, order_amount,\n"
            "       SUM(order_amount) OVER (PARTITION BY customer_name ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total\n"
            "FROM customer_orders\n"
            "ORDER BY customer_name, order_date"
        ),
        "explanation": (
            "This extends the running total pattern by adding **PARTITION BY**.\n\n"
            "- `PARTITION BY customer_name`: The running total resets for each customer.\n"
            "- `ORDER BY order_date`: Accumulates in chronological order.\n\n"
            "Each customer's `cumulative_total` grows independently."
        ),
    },
    # =====================================================================
    # 12. AVG (window) — Medium — Finance (Moving Average)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "3-Day Moving Average of Stock Prices",
        "description": (
            "A `stock_prices` table records daily closing prices. "
            "Calculate a **3-day moving average** (current day + 2 preceding days) of the closing price.\n\n"
            "Return: `trade_date`, `close_price`, `moving_avg_3d`.\n"
            "Round `moving_avg_3d` to 2 decimal places.\n"
            "Order by `trade_date` ascending."
        ),
        "difficulty": "Medium",
        "window_function_type": "AVG",
        "use_case_category": "Finance",
        "order_index": 12,
        "datasets": {
            "stock_prices": [
                {"trade_date": "2024-01-01", "ticker": "ACME", "close_price": 150.00},
                {"trade_date": "2024-01-02", "ticker": "ACME", "close_price": 152.50},
                {"trade_date": "2024-01-03", "ticker": "ACME", "close_price": 148.00},
                {"trade_date": "2024-01-04", "ticker": "ACME", "close_price": 155.00},
                {"trade_date": "2024-01-05", "ticker": "ACME", "close_price": 160.00},
                {"trade_date": "2024-01-06", "ticker": "ACME", "close_price": 158.50},
                {"trade_date": "2024-01-07", "ticker": "ACME", "close_price": 162.00},
            ]
        },
        "expected_output": [
            {"trade_date": "2024-01-01", "close_price": 150.00, "moving_avg_3d": 150.00},
            {"trade_date": "2024-01-02", "close_price": 152.50, "moving_avg_3d": 151.25},
            {"trade_date": "2024-01-03", "close_price": 148.00, "moving_avg_3d": 150.17},
            {"trade_date": "2024-01-04", "close_price": 155.00, "moving_avg_3d": 151.83},
            {"trade_date": "2024-01-05", "close_price": 160.00, "moving_avg_3d": 154.33},
            {"trade_date": "2024-01-06", "close_price": 158.50, "moving_avg_3d": 157.83},
            {"trade_date": "2024-01-07", "close_price": 162.00, "moving_avg_3d": 160.17},
        ],
        "initial_code_pyspark": "# Calculate 3-day moving average of stock prices\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import avg, col, round as spark_round\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate 3-day moving average of stock prices\n-- Columns: trade_date, close_price, moving_avg_3d\n-- Round moving_avg_3d to 2 decimal places\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import avg, col, round as spark_round\n\n"
            "window_spec = Window.orderBy('trade_date').rowsBetween(-2, Window.currentRow)\n"
            "final_df = spark.table('stock_prices')"
            ".withColumn('moving_avg_3d', spark_round(avg('close_price').over(window_spec), 2))"
            ".select('trade_date', 'close_price', 'moving_avg_3d')"
            ".orderBy('trade_date')"
        ),
        "solution_code_sql": (
            "SELECT trade_date, close_price,\n"
            "       ROUND(AVG(close_price) OVER (ORDER BY trade_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3d\n"
            "FROM stock_prices\n"
            "ORDER BY trade_date"
        ),
        "explanation": (
            "A **moving average** uses a sliding window frame.\n\n"
            "- `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`: averages the current row and the 2 rows before it.\n"
            "- For the first row, only 1 value is available, so the average equals that value.\n"
            "- For the second row, 2 values are averaged.\n"
            "- From the third row onward, all 3 values are used.\n\n"
            "Moving averages smooth out short-term fluctuations to reveal trends."
        ),
    },
    # =====================================================================
    # 13. AVG (window) — Hard — HR (Departmental Comparison)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Compare Employee Salary to Department Average",
        "description": (
            "An `emp_salaries` table has employee salaries. "
            "For each employee, calculate:\n"
            "- The **department average salary** using a window function.\n"
            "- The **difference** between their salary and the department average.\n\n"
            "Return: `employee_name`, `department`, `salary`, `dept_avg_salary`, `diff_from_avg`.\n"
            "Round `dept_avg_salary` and `diff_from_avg` to 2 decimal places.\n"
            "Order by `department`, `salary` descending."
        ),
        "difficulty": "Hard",
        "window_function_type": "AVG",
        "use_case_category": "HR",
        "order_index": 13,
        "datasets": {
            "emp_salaries": [
                {"emp_id": 1, "employee_name": "Alice", "department": "Engineering", "salary": 120000},
                {"emp_id": 2, "employee_name": "Bob", "department": "Engineering", "salary": 95000},
                {"emp_id": 3, "employee_name": "Charlie", "department": "Engineering", "salary": 110000},
                {"emp_id": 4, "employee_name": "Diana", "department": "Marketing", "salary": 85000},
                {"emp_id": 5, "employee_name": "Eve", "department": "Marketing", "salary": 78000},
                {"emp_id": 6, "employee_name": "Frank", "department": "Marketing", "salary": 92000},
                {"emp_id": 7, "employee_name": "Grace", "department": "Sales", "salary": 70000},
                {"emp_id": 8, "employee_name": "Hank", "department": "Sales", "salary": 65000},
            ]
        },
        "expected_output": [
            {"employee_name": "Alice", "department": "Engineering", "salary": 120000, "dept_avg_salary": 108333.33, "diff_from_avg": 11666.67},
            {"employee_name": "Charlie", "department": "Engineering", "salary": 110000, "dept_avg_salary": 108333.33, "diff_from_avg": 1666.67},
            {"employee_name": "Bob", "department": "Engineering", "salary": 95000, "dept_avg_salary": 108333.33, "diff_from_avg": -13333.33},
            {"employee_name": "Frank", "department": "Marketing", "salary": 92000, "dept_avg_salary": 85000.00, "diff_from_avg": 7000.00},
            {"employee_name": "Diana", "department": "Marketing", "salary": 85000, "dept_avg_salary": 85000.00, "diff_from_avg": 0.00},
            {"employee_name": "Eve", "department": "Marketing", "salary": 78000, "dept_avg_salary": 85000.00, "diff_from_avg": -7000.00},
            {"employee_name": "Grace", "department": "Sales", "salary": 70000, "dept_avg_salary": 67500.00, "diff_from_avg": 2500.00},
            {"employee_name": "Hank", "department": "Sales", "salary": 65000, "dept_avg_salary": 67500.00, "diff_from_avg": -2500.00},
        ],
        "initial_code_pyspark": "# Compare each employee's salary to their department average\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import avg, col, round as spark_round\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Compare each employee's salary to their department average\n-- Columns: employee_name, department, salary, dept_avg_salary, diff_from_avg\n-- Round to 2 decimal places\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import avg, col, round as spark_round\n\n"
            "window_spec = Window.partitionBy('department')\n"
            "df = spark.table('emp_salaries')\n"
            "df = df.withColumn('dept_avg_salary', spark_round(avg('salary').over(window_spec), 2))\n"
            "df = df.withColumn('diff_from_avg', spark_round(col('salary') - col('dept_avg_salary'), 2))\n"
            "final_df = df.select('employee_name', 'department', 'salary', 'dept_avg_salary', 'diff_from_avg')"
            ".orderBy('department', col('salary').desc())"
        ),
        "solution_code_sql": (
            "SELECT employee_name, department, salary,\n"
            "       ROUND(AVG(salary) OVER (PARTITION BY department), 2) AS dept_avg_salary,\n"
            "       ROUND(salary - AVG(salary) OVER (PARTITION BY department), 2) AS diff_from_avg\n"
            "FROM emp_salaries\n"
            "ORDER BY department, salary DESC"
        ),
        "explanation": (
            "Using **AVG() OVER (PARTITION BY department)** without an ORDER BY clause computes the average across the entire partition (all rows in the department).\n\n"
            "Key insight: Without ORDER BY in the window spec, the frame defaults to the entire partition, giving us the full department average on every row.\n\n"
            "The difference `salary - dept_avg_salary` shows whether an employee is above or below their department's average."
        ),
    },
    # =====================================================================
    # 14. COUNT (window) — Easy — Social Media
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Cumulative Post Count per User",
        "description": (
            "A `posts` table records social media posts. "
            "For each user, calculate a **cumulative count** of posts over time.\n\n"
            "Return: `username`, `post_date`, `post_title`, `cumulative_posts`.\n"
            "Order by `username`, `post_date`."
        ),
        "difficulty": "Easy",
        "window_function_type": "COUNT",
        "use_case_category": "Social Media",
        "order_index": 14,
        "datasets": {
            "posts": [
                {"post_id": 1, "username": "alice", "post_date": "2024-01-05", "post_title": "Hello World"},
                {"post_id": 2, "username": "alice", "post_date": "2024-01-12", "post_title": "My Second Post"},
                {"post_id": 3, "username": "alice", "post_date": "2024-02-01", "post_title": "February Update"},
                {"post_id": 4, "username": "bob", "post_date": "2024-01-08", "post_title": "First Post"},
                {"post_id": 5, "username": "bob", "post_date": "2024-03-15", "post_title": "Spring Thoughts"},
                {"post_id": 6, "username": "charlie", "post_date": "2024-01-20", "post_title": "Intro"},
                {"post_id": 7, "username": "charlie", "post_date": "2024-01-25", "post_title": "Follow Up"},
                {"post_id": 8, "username": "charlie", "post_date": "2024-02-10", "post_title": "Deep Dive"},
                {"post_id": 9, "username": "charlie", "post_date": "2024-03-01", "post_title": "March News"},
            ]
        },
        "expected_output": [
            {"username": "alice", "post_date": "2024-01-05", "post_title": "Hello World", "cumulative_posts": 1},
            {"username": "alice", "post_date": "2024-01-12", "post_title": "My Second Post", "cumulative_posts": 2},
            {"username": "alice", "post_date": "2024-02-01", "post_title": "February Update", "cumulative_posts": 3},
            {"username": "bob", "post_date": "2024-01-08", "post_title": "First Post", "cumulative_posts": 1},
            {"username": "bob", "post_date": "2024-03-15", "post_title": "Spring Thoughts", "cumulative_posts": 2},
            {"username": "charlie", "post_date": "2024-01-20", "post_title": "Intro", "cumulative_posts": 1},
            {"username": "charlie", "post_date": "2024-01-25", "post_title": "Follow Up", "cumulative_posts": 2},
            {"username": "charlie", "post_date": "2024-02-10", "post_title": "Deep Dive", "cumulative_posts": 3},
            {"username": "charlie", "post_date": "2024-03-01", "post_title": "March News", "cumulative_posts": 4},
        ],
        "initial_code_pyspark": "# Calculate cumulative post count per user\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import count, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate cumulative post count per user\n-- Columns: username, post_date, post_title, cumulative_posts\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import count, col\n\n"
            "window_spec = Window.partitionBy('username').orderBy('post_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)\n"
            "final_df = spark.table('posts')"
            ".withColumn('cumulative_posts', count('post_id').over(window_spec))"
            ".select('username', 'post_date', 'post_title', 'cumulative_posts')"
            ".orderBy('username', 'post_date')"
        ),
        "solution_code_sql": (
            "SELECT username, post_date, post_title,\n"
            "       COUNT(post_id) OVER (PARTITION BY username ORDER BY post_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_posts\n"
            "FROM posts\n"
            "ORDER BY username, post_date"
        ),
        "explanation": (
            "**COUNT() OVER (...)** with a running frame gives a cumulative count.\n\n"
            "- Partitioned by `username` so each user's count is independent.\n"
            "- Ordered by `post_date` with `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.\n"
            "- The count increments by 1 for each post in chronological order.\n\n"
            "This pattern tracks milestones (e.g., 100th post) or engagement growth over time."
        ),
    },
    # =====================================================================
    # 15. FIRST_VALUE — Medium — E-Commerce (First Purchase)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Find Each Customer's First Purchased Product",
        "description": (
            "A `purchases` table records customer purchases. "
            "Use `FIRST_VALUE` to find the **first product** each customer ever purchased.\n\n"
            "Return: `customer_name`, `purchase_date`, `product_name`, `first_product`.\n"
            "Order by `customer_name`, `purchase_date`."
        ),
        "difficulty": "Medium",
        "window_function_type": "FIRST_VALUE",
        "use_case_category": "E-Commerce",
        "order_index": 15,
        "datasets": {
            "purchases": [
                {"purchase_id": 1, "customer_name": "Alice", "purchase_date": "2024-01-10", "product_name": "Laptop"},
                {"purchase_id": 2, "customer_name": "Alice", "purchase_date": "2024-02-15", "product_name": "Mouse"},
                {"purchase_id": 3, "customer_name": "Alice", "purchase_date": "2024-03-20", "product_name": "Keyboard"},
                {"purchase_id": 4, "customer_name": "Bob", "purchase_date": "2024-01-05", "product_name": "Phone"},
                {"purchase_id": 5, "customer_name": "Bob", "purchase_date": "2024-04-10", "product_name": "Case"},
                {"purchase_id": 6, "customer_name": "Charlie", "purchase_date": "2024-02-01", "product_name": "Monitor"},
                {"purchase_id": 7, "customer_name": "Charlie", "purchase_date": "2024-02-28", "product_name": "Cable"},
            ]
        },
        "expected_output": [
            {"customer_name": "Alice", "purchase_date": "2024-01-10", "product_name": "Laptop", "first_product": "Laptop"},
            {"customer_name": "Alice", "purchase_date": "2024-02-15", "product_name": "Mouse", "first_product": "Laptop"},
            {"customer_name": "Alice", "purchase_date": "2024-03-20", "product_name": "Keyboard", "first_product": "Laptop"},
            {"customer_name": "Bob", "purchase_date": "2024-01-05", "product_name": "Phone", "first_product": "Phone"},
            {"customer_name": "Bob", "purchase_date": "2024-04-10", "product_name": "Case", "first_product": "Phone"},
            {"customer_name": "Charlie", "purchase_date": "2024-02-01", "product_name": "Monitor", "first_product": "Monitor"},
            {"customer_name": "Charlie", "purchase_date": "2024-02-28", "product_name": "Cable", "first_product": "Monitor"},
        ],
        "initial_code_pyspark": "# Find each customer's first purchased product using FIRST_VALUE\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import first, col\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Find each customer's first purchased product using FIRST_VALUE\n-- Columns: customer_name, purchase_date, product_name, first_product\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import first, col\n\n"
            "window_spec = Window.partitionBy('customer_name').orderBy('purchase_date')\n"
            "final_df = spark.table('purchases')"
            ".withColumn('first_product', first('product_name').over(window_spec))"
            ".select('customer_name', 'purchase_date', 'product_name', 'first_product')"
            ".orderBy('customer_name', 'purchase_date')"
        ),
        "solution_code_sql": (
            "SELECT customer_name, purchase_date, product_name,\n"
            "       FIRST_VALUE(product_name) OVER (PARTITION BY customer_name ORDER BY purchase_date) AS first_product\n"
            "FROM purchases\n"
            "ORDER BY customer_name, purchase_date"
        ),
        "explanation": (
            "**FIRST_VALUE(column)** returns the first value in the window frame.\n\n"
            "- Partitioned by `customer_name` and ordered by `purchase_date`.\n"
            "- The first value is always the product from the earliest purchase.\n"
            "- Every row in the same partition gets the same `first_product` value.\n\n"
            "Note: In PySpark, `first()` over a window works similarly to `FIRST_VALUE` in SQL."
        ),
    },
    # =====================================================================
    # 16. LAST_VALUE — Hard — Support (Last Interaction)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Find Latest Support Ticket Status per Customer",
        "description": (
            "A `support_tickets` table tracks customer support interactions. "
            "Use `LAST_VALUE` to find the **most recent ticket status** for each customer across all their tickets.\n\n"
            "Return: `customer_name`, `ticket_date`, `ticket_subject`, `ticket_status`, `latest_status`.\n"
            "Order by `customer_name`, `ticket_date`.\n\n"
            "**Hint**: You need to specify the correct frame clause for LAST_VALUE to work properly."
        ),
        "difficulty": "Hard",
        "window_function_type": "LAST_VALUE",
        "use_case_category": "Support",
        "order_index": 16,
        "datasets": {
            "support_tickets": [
                {"ticket_id": 1, "customer_name": "Alice", "ticket_date": "2024-01-05", "ticket_subject": "Login Issue", "ticket_status": "Open"},
                {"ticket_id": 2, "customer_name": "Alice", "ticket_date": "2024-01-10", "ticket_subject": "Login Issue", "ticket_status": "In Progress"},
                {"ticket_id": 3, "customer_name": "Alice", "ticket_date": "2024-01-15", "ticket_subject": "Login Issue", "ticket_status": "Resolved"},
                {"ticket_id": 4, "customer_name": "Bob", "ticket_date": "2024-02-01", "ticket_subject": "Billing Error", "ticket_status": "Open"},
                {"ticket_id": 5, "customer_name": "Bob", "ticket_date": "2024-02-05", "ticket_subject": "Billing Error", "ticket_status": "Escalated"},
                {"ticket_id": 6, "customer_name": "Charlie", "ticket_date": "2024-03-01", "ticket_subject": "Feature Request", "ticket_status": "Open"},
            ]
        },
        "expected_output": [
            {"customer_name": "Alice", "ticket_date": "2024-01-05", "ticket_subject": "Login Issue", "ticket_status": "Open", "latest_status": "Resolved"},
            {"customer_name": "Alice", "ticket_date": "2024-01-10", "ticket_subject": "Login Issue", "ticket_status": "In Progress", "latest_status": "Resolved"},
            {"customer_name": "Alice", "ticket_date": "2024-01-15", "ticket_subject": "Login Issue", "ticket_status": "Resolved", "latest_status": "Resolved"},
            {"customer_name": "Bob", "ticket_date": "2024-02-01", "ticket_subject": "Billing Error", "ticket_status": "Open", "latest_status": "Escalated"},
            {"customer_name": "Bob", "ticket_date": "2024-02-05", "ticket_subject": "Billing Error", "ticket_status": "Escalated", "latest_status": "Escalated"},
            {"customer_name": "Charlie", "ticket_date": "2024-03-01", "ticket_subject": "Feature Request", "ticket_status": "Open", "latest_status": "Open"},
        ],
        "initial_code_pyspark": "# Find the latest support ticket status per customer using LAST_VALUE\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import last, col\n\n# Your code here — remember to set the correct frame!\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Find the latest support ticket status per customer using LAST_VALUE\n-- Columns: customer_name, ticket_date, ticket_subject, ticket_status, latest_status\n-- Hint: You need ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import last, col\n\n"
            "window_spec = Window.partitionBy('customer_name').orderBy('ticket_date').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)\n"
            "final_df = spark.table('support_tickets')"
            ".withColumn('latest_status', last('ticket_status').over(window_spec))"
            ".select('customer_name', 'ticket_date', 'ticket_subject', 'ticket_status', 'latest_status')"
            ".orderBy('customer_name', 'ticket_date')"
        ),
        "solution_code_sql": (
            "SELECT customer_name, ticket_date, ticket_subject, ticket_status,\n"
            "       LAST_VALUE(ticket_status) OVER (\n"
            "           PARTITION BY customer_name ORDER BY ticket_date\n"
            "           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\n"
            "       ) AS latest_status\n"
            "FROM support_tickets\n"
            "ORDER BY customer_name, ticket_date"
        ),
        "explanation": (
            "**LAST_VALUE** has a common gotcha: the default frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.\n\n"
            "With the default frame, LAST_VALUE only sees up to the current row, making it behave like the current value!\n\n"
            "To get the actual last value in the entire partition, you must extend the frame:\n"
            "`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`\n\n"
            "This is the #1 mistake with LAST_VALUE in interviews and production code."
        ),
    },
    # =====================================================================
    # 17. PERCENT_RANK — Medium — HR (Salary Percentile)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Calculate Salary Percentile Rank",
        "description": (
            "An `employee_salaries` table lists salaries. "
            "Calculate the **percent rank** of each employee's salary (0 = lowest, 1 = highest).\n\n"
            "Return: `employee_name`, `salary`, `pct_rank`.\n"
            "Round `pct_rank` to 2 decimal places.\n"
            "Order by `salary` ascending."
        ),
        "difficulty": "Medium",
        "window_function_type": "PERCENT_RANK",
        "use_case_category": "HR",
        "order_index": 17,
        "datasets": {
            "employee_salaries": [
                {"emp_id": 1, "employee_name": "Alice", "salary": 60000},
                {"emp_id": 2, "employee_name": "Bob", "salary": 75000},
                {"emp_id": 3, "employee_name": "Charlie", "salary": 55000},
                {"emp_id": 4, "employee_name": "Diana", "salary": 90000},
                {"emp_id": 5, "employee_name": "Eve", "salary": 80000},
                {"emp_id": 6, "employee_name": "Frank", "salary": 55000},
            ]
        },
        "expected_output": [
            {"employee_name": "Charlie", "salary": 55000, "pct_rank": 0.00},
            {"employee_name": "Frank", "salary": 55000, "pct_rank": 0.00},
            {"employee_name": "Alice", "salary": 60000, "pct_rank": 0.40},
            {"employee_name": "Bob", "salary": 75000, "pct_rank": 0.60},
            {"employee_name": "Eve", "salary": 80000, "pct_rank": 0.80},
            {"employee_name": "Diana", "salary": 90000, "pct_rank": 1.00},
        ],
        "initial_code_pyspark": "# Calculate percent rank of salaries\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import percent_rank, col, round as spark_round\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate percent rank of salaries\n-- Columns: employee_name, salary, pct_rank\n-- Round to 2 decimal places\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import percent_rank, col, round as spark_round\n\n"
            "window_spec = Window.orderBy('salary')\n"
            "final_df = spark.table('employee_salaries')"
            ".withColumn('pct_rank', spark_round(percent_rank().over(window_spec), 2))"
            ".select('employee_name', 'salary', 'pct_rank')"
            ".orderBy('salary', 'employee_name')"
        ),
        "solution_code_sql": (
            "SELECT employee_name, salary,\n"
            "       ROUND(PERCENT_RANK() OVER (ORDER BY salary), 2) AS pct_rank\n"
            "FROM employee_salaries\n"
            "ORDER BY salary, employee_name"
        ),
        "explanation": (
            "**PERCENT_RANK()** = `(rank - 1) / (total_rows - 1)`\n\n"
            "- The lowest value always gets 0.0.\n"
            "- The highest value always gets 1.0.\n"
            "- Tied values get the same percent rank.\n\n"
            "With 6 rows: ranks are distributed as 0/5, 0/5, 2/5, 3/5, 4/5, 5/5 = 0.00, 0.00, 0.40, 0.60, 0.80, 1.00.\n\n"
            "PERCENT_RANK is useful for normalization and percentile-based analysis."
        ),
    },
    # =====================================================================
    # 18. CUME_DIST — Hard — Finance (Cumulative Distribution)
    # =====================================================================
    {
        "track": "window_functions",
        "title": "Cumulative Distribution of Transaction Amounts",
        "description": (
            "A `transactions` table records transaction amounts. "
            "Calculate the **cumulative distribution** (`CUME_DIST`) of each transaction amount.\n"
            "CUME_DIST represents the proportion of rows with values less than or equal to the current row's value.\n\n"
            "Return: `transaction_id`, `amount`, `cume_dist_val`.\n"
            "Round `cume_dist_val` to 2 decimal places.\n"
            "Order by `amount` ascending."
        ),
        "difficulty": "Hard",
        "window_function_type": "CUME_DIST",
        "use_case_category": "Finance",
        "order_index": 18,
        "datasets": {
            "transactions": [
                {"transaction_id": 1, "amount": 100},
                {"transaction_id": 2, "amount": 200},
                {"transaction_id": 3, "amount": 200},
                {"transaction_id": 4, "amount": 300},
                {"transaction_id": 5, "amount": 400},
                {"transaction_id": 6, "amount": 500},
                {"transaction_id": 7, "amount": 500},
                {"transaction_id": 8, "amount": 800},
            ]
        },
        "expected_output": [
            {"transaction_id": 1, "amount": 100, "cume_dist_val": 0.13},
            {"transaction_id": 2, "amount": 200, "cume_dist_val": 0.38},
            {"transaction_id": 3, "amount": 200, "cume_dist_val": 0.38},
            {"transaction_id": 4, "amount": 300, "cume_dist_val": 0.50},
            {"transaction_id": 5, "amount": 400, "cume_dist_val": 0.63},
            {"transaction_id": 6, "amount": 500, "cume_dist_val": 0.88},
            {"transaction_id": 7, "amount": 500, "cume_dist_val": 0.88},
            {"transaction_id": 8, "amount": 800, "cume_dist_val": 1.00},
        ],
        "initial_code_pyspark": "# Calculate cumulative distribution of transaction amounts\nfrom pyspark.sql import Window\nfrom pyspark.sql.functions import cume_dist, col, round as spark_round\n\n# Your code here\n# Result should be assigned to: final_df\n",
        "initial_code_sql": "-- Calculate cumulative distribution of transaction amounts\n-- Columns: transaction_id, amount, cume_dist_val\n-- Round to 2 decimal places\n",
        "solution_code_pyspark": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql.functions import cume_dist, col, round as spark_round\n\n"
            "window_spec = Window.orderBy('amount')\n"
            "final_df = spark.table('transactions')"
            ".withColumn('cume_dist_val', spark_round(cume_dist().over(window_spec), 2))"
            ".select('transaction_id', 'amount', 'cume_dist_val')"
            ".orderBy('amount', 'transaction_id')"
        ),
        "solution_code_sql": (
            "SELECT transaction_id, amount,\n"
            "       ROUND(CUME_DIST() OVER (ORDER BY amount), 2) AS cume_dist_val\n"
            "FROM transactions\n"
            "ORDER BY amount, transaction_id"
        ),
        "explanation": (
            "**CUME_DIST()** = `(number of rows with value <= current) / total_rows`\n\n"
            "- Unlike PERCENT_RANK, CUME_DIST is never 0 and always reaches 1.0.\n"
            "- With 8 rows: amount 100 has 1 row <= it, so CUME_DIST = 1/8 = 0.125 ~ 0.13.\n"
            "- Amount 200 has 3 rows <= it (100, 200, 200), so CUME_DIST = 3/8 = 0.375 ~ 0.38.\n"
            "- Tied values always get the same CUME_DIST.\n\n"
            "CUME_DIST answers: \"What percentage of data falls at or below this value?\" — essential for percentile analysis."
        ),
    },
]


def seed():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        added = 0
        skipped = 0
        for p_data in PROBLEMS:
            existing = db.query(PracticeListProblem).filter(
                PracticeListProblem.title == p_data["title"]
            ).first()
            if existing:
                print(f"  [SKIP] '{p_data['title']}' already exists")
                skipped += 1
                continue
            problem = PracticeListProblem(**p_data)
            db.add(problem)
            print(f"  [ADD]  '{p_data['title']}'")
            added += 1
        db.commit()
        print(f"\nSeeding complete: {added} added, {skipped} skipped, {len(PROBLEMS)} total.")
    finally:
        db.close()


if __name__ == "__main__":
    seed()
