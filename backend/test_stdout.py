import sys
import io
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

df = spark.createDataFrame([(1, "Alice")], ["id", "name"])

old_stdout = sys.stdout
redirected_output = io.StringIO()
sys.stdout = redirected_output

df.show()

sys.stdout = old_stdout

print("Captured output:", repr(redirected_output.getvalue()))
