import sys
import os
import io
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Fix Windows "Python worker failed to connect back" error
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize SparkSession globally so it's ready when requests come in
print("Initializing PySpark Session...")
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PySparkPractice") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("PySpark Session Ready!")

def execute_pyspark_code(code: str, datasets: dict):
    """
    Executes user PySpark code locally and captures output.
    """
    try:
        # Create temp views for datasets
        for table_name, data in datasets.items():
            if data and isinstance(data, list):
                # We must standardize jagged dicts (where LLM omits null keys) 
                # Otherwise spark.createDataFrame crashes on schema inference mismatch
                all_keys = set()
                for d in data:
                    all_keys.update(d.keys())
                    
                standardized_data = []
                for d in data:
                    std_row = {k: d.get(k, None) for k in all_keys}
                    standardized_data.append(std_row)

                df = spark.createDataFrame(standardized_data)
                df.createOrReplaceTempView(table_name)
    except Exception as e:
        return {
            "success": False,
            "output": "",
            "error": f"Failed to initialize datasets: {traceback.format_exc()}"
        }

    # Redirect stdout and stderr
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    redirected_output = io.StringIO()
    sys.stdout = redirected_output
    sys.stderr = redirected_output
    
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    
    local_vars = {
        "spark": spark,
        "F": F,
        "T": T,
        "null": None # Ultimate fallback for LLM generated Javascript 'null' in Python code
    }
    
    # Automatically inject all SQL functions (e.g., col, when, sum) and Types into global scope
    for name in dir(F):
        if not name.startswith("_"):
            local_vars[name] = getattr(F, name)
            
    for name in dir(T):
        if not name.startswith("_"):
            local_vars[name] = getattr(T, name)
            
    error_msg = None
    success = False
    
    try:
        exec(code, {}, local_vars)
        success = True
    except Exception as e:
        error_msg = traceback.format_exc()
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        
    output = redirected_output.getvalue()
    
    # Evaluation Logic (Phase 5): Look for 'final_df' explicitly
    final_df_rows = None
    if success and 'final_df' in local_vars:
        try:
            # Convert PySpark DataFrame to a list of dicts for straightforward JSON grading
            final_df = local_vars['final_df']
            
            # Use toPandas() then to_dict() to bypass complex PySpark Row serialization
            import math
            pdf = final_df.toPandas()
            raw_records = pdf.to_dict('records')
            
            # Prevent Out of Range JSON float errors by replacing pandas/python NaN with None
            final_df_rows = []
            for row in raw_records:
                cleaned_row = {}
                for k, v in row.items():
                    if isinstance(v, float) and math.isnan(v):
                        cleaned_row[k] = None
                    else:
                        cleaned_row[k] = v
                final_df_rows.append(cleaned_row)
            
            # Phase 5 QoL: Auto-print final_df if user forgot to use .show()
            try:
                show_str = final_df._jdf.showString(20, 20, False)
                if show_str not in output:
                    if output.strip():
                        output += "\n"
                    output += f"--- Auto-displaying final_df ---\n{show_str}"
            except Exception:
                pass
                
        except Exception as e:
            error_msg = f"Code executed successfully, but failed to parse 'final_df': {str(e)}"
            success = False
    
    return {
        "success": success,
        "output": output,
        "error": error_msg,
        "final_df_rows": final_df_rows
    }
