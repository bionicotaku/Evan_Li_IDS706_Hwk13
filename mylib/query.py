from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import io
from contextlib import redirect_stdout

def generate_markdown(output):    
    # Generate markdown content
    markdown_content = "# Data Analysis Results\n\n"

    # Add code output
    markdown_content += "## Statistical Analysis\n\n"
    markdown_content += "```\n" + output + "```\n\n"
    
    # use dbutils.fs.put to write file
    dbutils.fs.put("dbfs:/FileStore/IDS_hwk13/analysis_results.md", markdown_content, True)  # noqa: F821

def read_data(table_name="data_engineer_salary_in_2024"):
    """
    Read data from Delta table in Databricks
    
    Parameters:
    table_name (str): Name of the Delta table
    
    Returns:
    DataFrame: PySpark DataFrame with selected columns
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("SalaryAnalysis").getOrCreate()
    
    # Read from Delta table
    salaryData = spark.table(table_name)
    
    columns_to_keep = [
        "work_year",
        "experience_level",
        "job_title",
        "salary_in_usd",
        "remote_ratio",
        "company_size",
    ]
    return salaryData.select(columns_to_keep)

def calculate_stat(data):
    # Calculate descriptive statistics using PySpark
    salaryDataDesc = data.select('salary_in_usd').summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
    )
    return salaryDataDesc

def get_job_title_distribution(data):
    # Get job title counts using PySpark
    return (data.groupBy('job_title')
               .count()
               .orderBy(F.col('count').desc())
               .limit(20))

def get_experience_level_distribution(data):
    # Get experience level counts using PySpark
    return (data.groupBy('experience_level')
               .count()
               .orderBy('experience_level'))

def calculate_salary_stats_by_experience(data):
    # Calculate salary statistics by experience level using PySpark
    return (data.groupBy('experience_level')
                .agg(F.mean('salary_in_usd').alias('mean'),
                     F.expr('percentile_approx(salary_in_usd, 0.5)').alias('median'),
                     F.min('salary_in_usd').alias('min'),
                     F.max('salary_in_usd').alias('max'))
                .orderBy('experience_level'))

def analyze_high_paying_jobs_sql(data):
    """Using Spark SQL to analyze high paying jobs"""
    # Register the DataFrame as a SQL temporary view
    data.createOrReplaceTempView("salary_data")
    
    # Execute Spark SQL query
    sql_query = """
    WITH avg_salaries AS (
        SELECT job_title,
               AVG(salary_in_usd) as avg_salary,
               COUNT(*) as job_count
        FROM salary_data
        GROUP BY job_title
        HAVING COUNT(*) >= 5
    )
    SELECT job_title,
           ROUND(avg_salary, 2) as avg_salary,
           job_count
    FROM avg_salaries
    WHERE avg_salary > (SELECT AVG(salary_in_usd) FROM salary_data)
    ORDER BY avg_salary DESC
    LIMIT 20
    """
    return data.sparkSession.sql(sql_query)

def analyze_salary_trends(data):
    """Complex data transformation using DataFrame API"""
    # Add salary range category
    salary_ranges = F.when(F.col("salary_in_usd") <= 50000, "Entry Level")\
        .when((F.col("salary_in_usd") > 50000) & (F.col("salary_in_usd") <= 100000), "Mid Level")\
        .when((F.col("salary_in_usd") > 100000) & (F.col("salary_in_usd") <= 150000), "Senior Level")\
        .otherwise("Executive Level")
    
    # Complex transformation with multiple aggregations and window functions
    return data.withColumn("salary_range", salary_ranges)\
               .groupBy("experience_level", "salary_range")\
               .agg(
                   F.count("*").alias("count"),
                   F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"),
                   F.round(F.stddev("salary_in_usd"), 2).alias("salary_stddev")
               )\
               .withColumn("pct_of_level", 
                   F.round(F.col("count") * 100 / F.sum("count").over(Window.partitionBy("experience_level")), 2)
               )\
               .orderBy("experience_level", "salary_range")
               
if __name__ == "__main__":
    data = read_data()
    
    captured_output = io.StringIO()
    with redirect_stdout(captured_output):
        # Calculate and print statistics
        print("Salary Statistics:")
        calculate_stat(data).show()
        print()

        # Print job title distribution
        print("Top 20 Job Titles Distribution:")
        get_job_title_distribution(data).show()
        print()

        # Print experience level distribution
        print("Experience Level Distribution:")
        get_experience_level_distribution(data).show()
        print()

        # Print salary statistics by experience level
        print("Salary Statistics by Experience Level:")
        calculate_salary_stats_by_experience(data).show()
        print()

        # Print high paying jobs analysis using Spark SQL
        print("High Paying Jobs Analysis (SQL):")
        analyze_high_paying_jobs_sql(data).show(truncate=False)
        print()

        # Print salary distribution analysis using PySpark
        print(
            """
        Process:
        1. Categorizes salaries into 4 ranges (Entry/Mid/Senior/Executive Level)
        2. Groups data by experience level and salary range
        3. Calculates count, average salary, standard deviation, and percentage for each group
        """
        )
        print(
            "Salary Distribution Analysis by Experience Level and Salary Range (data transformation):"
        )
        analyze_salary_trends(data).show(truncate=False)
        print()
    output = captured_output.getvalue()
    generate_markdown(output)