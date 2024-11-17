from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
import os
from typing import Optional

def load(dataset: str = "dbfs:/FileStore/IDS_hwk13/data-engineer-salary-in-2024.csv") -> str:
    """
    从本地连接Databricks并创建Delta表
    
    Parameters:
    dataset (str): DBFS路径下的CSV文件
    
    Returns:
    str: 状态信息
    """
    try:
        # 创建Databricks连接
        spark = SparkSession.builder.appName("Read CSV").getOrCreate()
        # load csv and transform it by inferring schema 
        data_engineer_salary_df = spark.read.csv(dataset, header=True, inferSchema=True)

        # add unique IDs to the DataFrames
        data_engineer_salary_df = data_engineer_salary_df.withColumn("id", monotonically_increasing_id())

        # transform into a delta lakes table and store it 
        data_engineer_salary_df.write.format("delta").mode("overwrite").saveAsTable("data_engineer_salary_in_2024")
        
        num_rows = data_engineer_salary_df.count()
        print(num_rows)
        
        return "finished transform and load"
        
    except Exception as e:
        print(f"Failed to process data: {str(e)}")
        raise

if __name__ == "__main__":
    load()