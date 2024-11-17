from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import os

def is_running_on_databricks():
    """Check if the code is running on Databricks"""
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ

def load(dataset: str = "dbfs:/FileStore/IDS_hwk13/data-engineer-salary-in-2024.csv") -> str:
    """
    Connect to Databricks and create a Delta table
    
    Parameters:
    dataset (str): CSV file path in DBFS
    
    Returns:
    str: Status message
    """
    
    if is_running_on_databricks():
        try:
            spark = SparkSession.builder.appName("Read CSV").getOrCreate()
            
            # 首先检查表是否存在
            table_exists = spark.catalog._jcatalog.tableExists("data_engineer_salary_in_2024")
            
            # 读取CSV文件
            data_engineer_salary_df = spark.read.csv(dataset, header=True, inferSchema=True)
            
            if not table_exists:
                # 如果表不存在，添加id列并创建新表
                data_engineer_salary_df = data_engineer_salary_df.withColumn("id", monotonically_increasing_id())
            
            # 使用saveAsTable，但不添加重复的id列
            data_engineer_salary_df.write.format("delta").mode("overwrite").saveAsTable("data_engineer_salary_in_2024")
            
            num_rows = data_engineer_salary_df.count()
            print(f"Number of rows loaded: {num_rows}")
            
        except Exception as e:
            print(f"Failed to process data: {str(e)}")
            raise
    else:
        print("Not running on Databricks")  

if __name__ == "__main__":
    load()