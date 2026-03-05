# test_pyspark.py
import sys
import os
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = [
    {"customer_id": 1, "name": "Alice", "order_amount": 100},
    {"customer_id": 2, "name": "Bob", "order_amount": 200},
    {"customer_id": 3, "name": "Charlie"}
]

try:
    print("Testing DataFrame creation with explicit column mapping:")
    # Collect all unique possible columns
    all_keys = set()
    for d in data:
        all_keys.update(d.keys())
        
    # Standardize dicts by filling missing keys with None
    standardized_data = []
    for d in data:
        std_row = {k: d.get(k, None) for k in all_keys}
        standardized_data.append(std_row)
        
    df = spark.createDataFrame(standardized_data)
    df.show()
    print("Success!")
except Exception as e:
    print("Failed:")
    print(e)
    df.show()
    print("Success!")
except Exception as e:
    print("Failed:")
    print(e)
