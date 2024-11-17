# Data Analysis Results

## Statistical Analysis

```
Salary Statistics:
+-------+------------------+
|summary|     salary_in_usd|
+-------+------------------+
|  count|             16534|
|   mean|149686.77797266238|
| stddev| 68505.29315634348|
|    min|             15000|
|    25%|            101100|
|    50%|            141298|
|    75%|            185900|
|    max|            800000|
+-------+------------------+


Top 20 Job Titles Distribution:
+--------------------+-----+
|           job_title|count|
+--------------------+-----+
|       Data Engineer| 3464|
|      Data Scientist| 3314|
|        Data Analyst| 2440|
|Machine Learning ...| 1705|
|  Research Scientist|  531|
|   Applied Scientist|  435|
|      Data Architect|  435|
|  Analytics Engineer|  431|
|   Research Engineer|  306|
|        Data Science|  271|
|Business Intellig...|  248|
|        Data Manager|  212|
|         ML Engineer|  200|
|Business Intellig...|  191|
|Machine Learning ...|  138|
|    Research Analyst|  123|
|Data Science Manager|  122|
|         AI Engineer|  120|
|Business Intellig...|   98|
|        BI Developer|   90|
+--------------------+-----+


Experience Level Distribution:
+----------------+-----+
|experience_level|count|
+----------------+-----+
|              EN| 1325|
|              EX|  501|
|              MI| 4038|
|              SE|10670|
+----------------+-----+


Salary Statistics by Experience Level:
+----------------+------------------+------+-----+------+
|experience_level|              mean|median|  min|   max|
+----------------+------------------+------+-----+------+
|              EN| 92327.41358490566| 83000|15000|774000|
|              EX|195264.28143712576|192000|15000|465000|
|              MI|125923.13125309559|115000|15000|800000|
|              SE|163662.82614807872|155000|15809|750000|
+----------------+------------------+------+-----+------+


High Paying Jobs Analysis (SQL):
+----------------------------------+----------+---------+
|job_title                         |avg_salary|job_count|
+----------------------------------+----------+---------+
|Head of Machine Learning          |299758.43 |7        |
|AI Architect                      |252551.24 |29       |
|Director of Data Science          |218775.33 |33       |
|Head of Data                      |211860.2  |61       |
|Prompt Engineer                   |205093.59 |17       |
|Data Infrastructure Engineer      |204192.05 |22       |
|Robotics Software Engineer        |196625.0  |8        |
|Data Analytics Lead               |195536.74 |23       |
|Principal Data Scientist          |194089.6  |10       |
|Data Science Manager              |193961.32 |122      |
|ML Engineer                       |193910.76 |200      |
|Research Scientist                |192093.09 |531      |
|Deep Learning Engineer            |190807.5  |12       |
|Research Engineer                 |190177.98 |306      |
|Machine Learning Scientist        |190077.85 |138      |
|Applied Scientist                 |189984.12 |435      |
|Machine Learning Engineer         |188623.37 |1705     |
|Machine Learning Software Engineer|188440.27 |15       |
|Head of Data Science              |178387.25 |12       |
|Data Science Director             |177416.5  |8        |
+----------------------------------+----------+---------+



        Process:
        1. Categorizes salaries into 4 ranges (Entry/Mid/Senior/Executive Level)
        2. Groups data by experience level and salary range
        3. Calculates count, average salary, standard deviation, and percentage for each group
        
Salary Distribution Analysis by Experience Level and Salary Range (data transformation):
+----------------+---------------+-----+----------+-------------+------------+
|experience_level|salary_range   |count|avg_salary|salary_stddev|pct_of_level|
+----------------+---------------+-----+----------+-------------+------------+
|EN              |Entry Level    |269  |38025.01  |10284.94     |20.3        |
|EN              |Executive Level|157  |191860.92 |60531.89     |11.85       |
|EN              |Mid Level      |625  |76722.29  |14023.82     |47.17       |
|EN              |Senior Level   |274  |124202.55 |14328.46     |20.68       |
|EX              |Entry Level    |1    |15000.0   |NULL         |0.2         |
|EX              |Executive Level|353  |227860.42 |56152.69     |70.46       |
|EX              |Mid Level      |37   |83603.3   |13048.85     |7.39        |
|EX              |Senior Level   |110  |129857.75 |14365.45     |21.96       |
|MI              |Entry Level    |226  |38809.89  |9904.41      |5.6         |
|MI              |Executive Level|1085 |205062.86 |74231.6      |26.87       |
|MI              |Mid Level      |1405 |78903.92  |13990.19     |34.79       |
|MI              |Senior Level   |1322 |125834.61 |14347.18     |32.74       |
|SE              |Entry Level    |142  |41404.87  |8057.9       |1.33        |
|SE              |Executive Level|5566 |209318.34 |52762.59     |52.16       |
|SE              |Mid Level      |1388 |82574.93  |13440.68     |13.01       |
|SE              |Senior Level   |3574 |128909.62 |14099.12     |33.5        |
+----------------+---------------+-----+----------+-------------+------------+


```

