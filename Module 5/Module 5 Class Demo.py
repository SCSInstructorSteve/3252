# Databricks notebook source
#The following cell is a python cell, although the notebook is a scala notebook
print("hello this is python")

# COMMAND ----------

# MAGIC %md ** Read a file **

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/csv/

# COMMAND ----------

dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load(dataPath)

# COMMAND ----------

display(diamonds)

# COMMAND ----------

# MAGIC %md ** Transformations and Actions **

# COMMAND ----------

diamonds.select("_c0").limit(10).collect()

# COMMAND ----------

diamonds.printSchema()

# COMMAND ----------

diamonds.select("carat").limit(10).collect()

# COMMAND ----------

diamonds.count()

# COMMAND ----------


dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(dataPath)

# COMMAND ----------

diamonds.count()

# COMMAND ----------

# FILTER
display(diamonds.filter(diamonds.cut.contains("Ideal")))

# COMMAND ----------

# Flat Map
from pyspark.sql.functions import split, explode

fav_movies = spark.createDataFrame([("Pulp Fiction",), ("Requiem for a dream",), ("A clockwork Orange",)], ["title"])
fav_movies.select(explode(split("title", " ")).alias("word")).collect()

# COMMAND ----------

# MAGIC %python # Sample
# MAGIC data = spark.createDataFrame([(i,) for i in range(1, 21)], ["value"])
# MAGIC data.sample(0.1).collect()

# COMMAND ----------

# Intersection
java_skills = spark.createDataFrame([("Tom Mahoney",), ("Alicia Whitekar",), ("Paul Jones",), ("Rodney Marsh",)], ["name"])
db_skills = spark.createDataFrame([("James Kent",), ("Paul Jones",), ("Tom Mahoney",), ("Adam Waugh",)], ["name"])
java_skills.intersect(db_skills).collect()

# COMMAND ----------

# Union
java_skills = spark.createDataFrame([("Tom Mahoney",), ("Alicia Whitekar",), ("Paul Jones",), ("Rodney Marsh",)], ["name"])
db_skills = spark.createDataFrame([("James Kent",), ("Paul Jones",), ("Tom Mahoney",), ("Adam Waugh",)], ["name"])
java_skills.union(db_skills).collect()

# COMMAND ----------

# Subtract
java_skills = spark.createDataFrame([("Tom Mahoney",), ("Alicia Whitekar",), ("Paul Jones",), ("Rodney Marsh",)], ["name"])
db_skills = spark.createDataFrame([("James Kent",), ("Paul Jones",), ("Tom Mahoney",), ("Adam Waugh",)], ["name"])
java_skills.subtract(db_skills).collect()

# COMMAND ----------

# Reduced and group by key
from pyspark.sql.functions import sum as spark_sum

# Input Data
store_sales = spark.createDataFrame([("London", 23.4), ("Manchester", 19.8), ("Leeds", 14.7), ("London", 26.6)], ["city", "sales"])

# GroupBy and sum
store_sales.groupBy("city").agg(spark_sum("sales").alias("total_sales")).collect()

# SampleResult
# res2: [(Manchester, 19.8), (London, 50.0), (Leeds, 14.7)]

# COMMAND ----------

# ReduceByKey (using groupBy and sum)
from pyspark.sql.functions import sum as spark_sum

store_sales.groupBy("city").agg(spark_sum("sales").alias("total_sales")).collect()

# Sample Result
# res1: [(Manchester, 19.8), (London, 50.0), (Leeds, 14.7)]

# COMMAND ----------

# CombineByKey (using groupBy with sum and count)
from pyspark.sql.functions import sum as spark_sum, count

sample_data = spark.createDataFrame([("k1", 10), ("k2", 5), ("k1", 6), ("k3", 4), ("k2", 1), ("k3", 4)], ["key", "value"])
sum_count = sample_data.groupBy("key").agg(spark_sum("value").alias("sum"), count("value").alias("count"))
sum_count.take(3)

# COMMAND ----------

# Average by key (using withColumn to calculate avg)
from pyspark.sql.functions import col

avg_by_key = sum_count.withColumn("avg", col("sum") / col("count"))
avg_by_key.select("key", "avg").take(3)

# COMMAND ----------

1

# COMMAND ----------

# MAGIC %md *** SPARK SQL ***
# MAGIC
# MAGIC ## The Data
# MAGIC
# MAGIC ![img](http://training.databricks.com/databricks_guide/USDA_logo.png)
# MAGIC
# MAGIC The first of the two datasets that we will be working with is the **Farmers Markets Directory and Geographic Data**. This dataset contains information on the longitude and latitude, state, address, name, and zip code of farmers markets in the United States. The raw data is published by the Department of Agriculture. The version on the data that is found in Databricks (and is used in this tutorial) was updated by the Department of Agriculture on Dec 01, 2015.
# MAGIC
# MAGIC ![img](http://training.databricks.com/databricks_guide/irs-logo.jpg)
# MAGIC
# MAGIC The second dataset we will be working with is the **SOI Tax Stats - Individual Income Tax Statistics - ZIP Code Data (SOI)**. This study provides detailed tabulations of individual income tax return data at the state and ZIP code level and is provided by the IRS. This repository only has a sample of the data: 2013 and includes "AGI". The ZIP Code data shows selected income and tax items classified by State, ZIP Code, and size of adjusted gross income. Data is based on individual income tax returns filed with the IRS and is available for Tax Years 1998, 2001, 2004 through 2013.
# MAGIC

# COMMAND ----------

# Read The data
taxes2013 = spark.read \
  .option("header", "true") \
  .csv("dbfs:/databricks-datasets/data.gov/irs_zip_code_data/data-001/2013_soi_zipcode_agi.csv")

markets = spark.read \
  .option("header", "true") \
  .csv("dbfs:/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/market_data.csv")

# COMMAND ----------

# Register spark SQL tables
taxes2013.createOrReplaceTempView("taxes2013")
markets.createOrReplaceTempView("markets")

# COMMAND ----------

# MAGIC %md Run SQL Commands

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

display(taxes2013)

# COMMAND ----------

# MAGIC %sql SELECT * FROM taxes2013

# COMMAND ----------

# Table projections
taxes2013.select("STATEFIPS", "zipcode").show(5)

# COMMAND ----------

# MAGIC %md We can see that we've got a variety of columns that you might want to look into further however for let's focus on a very small subset. In addition let's perform some data transformations:
# MAGIC
# MAGIC 1. Type conversions and rename the columns
# MAGIC 2. Shorten each zip code to be four digits instead of 5. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS cleaned_taxes;
# MAGIC
# MAGIC CREATE TABLE cleaned_taxes AS
# MAGIC SELECT state, int(double(zipcode) / 10) as zipcode, 
# MAGIC   int(double(mars1)) as single_returns, 
# MAGIC   int(double(mars2)) as joint_returns, 
# MAGIC   int(double(numdep)) as numdep, 
# MAGIC   double(A02650) as total_income_amount,
# MAGIC   double(A00300) as taxable_interest_amount,
# MAGIC   double(a01000) as net_capital_gains,
# MAGIC   double(a00900) as biz_net_income
# MAGIC FROM taxes2013

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql select * FROM cleaned_taxes

# COMMAND ----------

# Using the dataset API
cleaned_taxes = spark.table("cleaned_taxes")
display(cleaned_taxes.groupBy("state").avg("total_income_amount"))

# COMMAND ----------

display(cleaned_taxes.describe())

# COMMAND ----------

# MAGIC %md  Let's look at the set of zip codes with the lowest total capital gains and plot the results. You can see that we're able to use simple expressive SQL to achieve these results in a very straightforward manner as well as some familiar DataFrame manipulations available in R and Python. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT zipcode, SUM(net_capital_gains) AS cap_gains
# MAGIC FROM cleaned_taxes 
# MAGIC   WHERE NOT (zipcode = 0000 OR zipcode = 9999)
# MAGIC GROUP BY zipcode
# MAGIC ORDER BY cap_gains ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md Let's do some further explorations:Let's look at a combination of capital gains and business net income to see what we find. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT zipcode, 
# MAGIC   SUM(biz_net_income) as business_net_income, 
# MAGIC   SUM(net_capital_gains) as capital_gains, 
# MAGIC   SUM(net_capital_gains) + SUM(biz_net_income) as capital_and_business_income
# MAGIC FROM cleaned_taxes 
# MAGIC   WHERE NOT (zipcode = 0000 OR zipcode = 9999)
# MAGIC GROUP BY zipcode
# MAGIC ORDER BY capital_and_business_income DESC
# MAGIC LIMIT 50

# COMMAND ----------

# equivalent to the above
combo = spark.sql("""
  SELECT zipcode, 
    SUM(biz_net_income) as net_income, 
    SUM(net_capital_gains) as cap_gains, 
    SUM(net_capital_gains) + SUM(biz_net_income) as combo
  FROM cleaned_taxes 
  WHERE NOT (zipcode = 0000 OR zipcode = 9999)
  GROUP BY zipcode
  ORDER BY combo desc
  limit 50""")

display(combo)

# COMMAND ----------

combo.explain()

# COMMAND ----------

# MAGIC %md One thing that is great about Apache Spark is that out of the box it can store and access tables in memory. All that we need to do is to `cache` the data to do so. We can either do this directly in SQL (at which point the cache will be done *eagerly* or right away), or we can do it through the `spark` with the `cacheTable` method which will be performed lazily.

# COMMAND ----------

# Caching not needed on serverless - automatic optimization is enabled
pass

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE  TABLE cleaned_taxes
# MAGIC

# COMMAND ----------

# MAGIC %md Calculation after caching

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT zipcode, 
# MAGIC   SUM(biz_net_income) as net_income, 
# MAGIC   SUM(net_capital_gains) as cap_gains, 
# MAGIC   SUM(net_capital_gains) + SUM(biz_net_income) as combo
# MAGIC FROM cleaned_taxes 
# MAGIC   WHERE NOT (zipcode = 0000 OR zipcode = 9999)
# MAGIC GROUP BY zipcode
# MAGIC ORDER BY combo desc
# MAGIC limit 50

# COMMAND ----------

# MAGIC %md let's explore the second dataset:

# COMMAND ----------

display(markets.groupBy("State").count())

# COMMAND ----------

# MAGIC %md Let's go ahead and prepare our data and join it together to the target variable - the number of farmer's markets in a given zipcode.

# COMMAND ----------

# Convert back to a dataset from a table
cleaned_taxes = spark.sql("SELECT * FROM cleaned_taxes")

summed_taxes = cleaned_taxes \
  .groupBy("zipcode") \
  .sum()  # because of AGI, where groups income groups are broken out

cleaned_markets = markets \
  .filter("zip rlike '^[0-9]'") \
  .selectExpr("*", "int(substring(zip, 1, 5) / 10) as zipcode") \
  .groupBy("zipcode") \
  .count() \
  .selectExpr("double(count) as count", "zipcode as zip")
# selectExpr is short for Select Expression - equivalent to what we
# might be doing in SQL SELECT expression

joined = cleaned_markets \
  .join(summed_taxes, cleaned_markets["zip"] == summed_taxes["zipcode"], "outer")

# COMMAND ----------

display(cleaned_markets)

# COMMAND ----------

display(joined)

# COMMAND ----------

# MAGIC %md deal with na values

# COMMAND ----------

prepped = joined.na.fill(0)
display(prepped)

# COMMAND ----------

display(prepped)

# COMMAND ----------

# MAGIC %md Now that all of our data is prepped. We're going to have to put all of it into one column of a vector type for Spark MLLib. This makes it easy to embed a prediction right in a DataFrame and also makes it very clear as to what is getting passed into the model and what isn't without having to convert it to a numpy array or specify an R formula. This also makes it easy to incrementally add new features, simply by adding to the vector. In the below case rather than specifically adding them in.

# COMMAND ----------

non_feature_cols = ["zip", "zipcode", "count"]
feature_cols = [col for col in prepped.columns if col not in non_feature_cols]

# COMMAND ----------

# VectorAssembler Assembles all of these columns into one single vector. To do this, set the input columns and output column. Then that assembler will be used to transform the prepped data to the final dataset.
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features")

final_prep = assembler.transform(prepped)

# COMMAND ----------

# MAGIC %md Now split the dataset 70-30 for training and testing purposes.A validation set can be created as well, we are omitting it here. It's worth noting that MLLib also supports performing hyperparameter tuning with cross validation and pipelines. All this can be found in [the Databrick's Guide](https://docs.databricks.com).

# COMMAND ----------

training, test = final_prep.randomSplit([0.7, 0.3])

# Going to cache the data to make sure things stay snappy!
training.cache()
test.cache()

print(training.count())  # Why execute count here??
print(test.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC # Apache Spark MLLib
# MAGIC
# MAGIC At a high level, we're going to create an instance of a `regressor` or `classifier`, that in turn will then be trained and return a `Model` type. Whenever you access Spark MLLib you should be sure to import/train on the name of the algorithm you want as opposed to the `Model` type. For example:
# MAGIC
# MAGIC You should import:
# MAGIC
# MAGIC `org.apache.spark.ml.regression.LinearRegression`
# MAGIC
# MAGIC as opposed to:
# MAGIC
# MAGIC `org.apache.spark.ml.regression.LinearRegressionModel`
# MAGIC
# MAGIC In the below example, we're going to use linear regression.
# MAGIC
# MAGIC The linear regression that is available in Spark MLLib supports an elastic net parameter allowing you to set a threshold of how much you would like to mix l1 and l2 regularization, for [more information on Elastic net regularization see Wikipedia](https://en.wikipedia.org/wiki/Elastic_net_regularization).
# MAGIC
# MAGIC As we saw above, we had to perform some preparation of the data before inputting it into the model. We've got to do the same with the model itself. We'll set our hyper parameters, print them out and then finally we can train it! The `explainParams` is a great way to ensure that you're taking advantage of all the different hyperparameters that you have available.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr_model = LinearRegression()
lr_model.setLabelCol("count")
lr_model.setFeaturesCol("features")
lr_model.setElasticNetParam(0.5)

print("Printing out the model Parameters:")
print("-" * 20)
print(lr_model.explainParams())
print("-" * 20)

# COMMAND ----------

# MAGIC %md Now finally we can go about fitting our model! You'll see that we're going to do this in a series of steps. First we'll fit it, then we'll use it to make predictions via the `transform` method. This is the same way you would make predictions with your model in the future however in this case we're using it to evaluate how our model is doing. We'll be using regression metrics to get some idea of how our model is performing, we'll then print out those values to be able to evaluate how it performs.

# COMMAND ----------

from pyspark.mllib.evaluation import RegressionMetrics
lr_fitted = lr_model.fit(training)

# COMMAND ----------

# MAGIC %md Now you'll see that since we're working with exact numbers (you can't have 1/2 a farmer's market for example), I'm going to check equality by first rounding the value to the nearest digital value.

# COMMAND ----------

holdout = lr_fitted \
  .transform(test) \
  .selectExpr("prediction as raw_prediction", 
    "double(round(prediction)) as prediction", 
    "count", 
    """CASE double(round(prediction)) = count 
  WHEN true then 1
  ELSE 0
END as equal""")
display(holdout)

# COMMAND ----------

# MAGIC %md Now let's see what proportion was exactly correct.

# COMMAND ----------

display(holdout.selectExpr("sum(equal)/sum(1)"))

# COMMAND ----------

# have to do a type conversion for RegressionMetrics
rm = RegressionMetrics(
  holdout.select("prediction", "count").rdd.map(lambda x:
  (float(x[0]), float(x[1]))))

print("MSE: " + str(rm.meanSquaredError))
print("MAE: " + str(rm.meanAbsoluteError))
print("RMSE Squared: " + str(rm.rootMeanSquaredError))
print("R Squared: " + str(rm.r2))
print("Explained Variance: " + str(rm.explainedVariance) + "\n")

# COMMAND ----------

# MAGIC %md These results appear to be sub-optimal, so let's try exploring another way to train the model. Rather than training on a single model with hard-coded parameters, let's train using a [pipeline](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.Pipeline). 
# MAGIC
# MAGIC A pipeline is going to give us some nice benefits in that it will allow us to use a couple of transformations we need in order to transform our raw data into the prepared data for the model but also it provides a simple, straightforward way to try out a lot of different combinations of parameters. This is a process called [hyperparameter tuning](https://en.wikipedia.org/wiki/Hyperparameter_optimization) or grid search. To review, grid search is where you set up the exact parameters that you would like to test and MLLib will automatically create all the necessary combinations of these to test.
# MAGIC
# MAGIC For example, below we'll set `numTrees` to 20 and 60 and `maxDepth` to 5 and 10. The parameter grid builder will automatically construct all the combinations of these two variable (along with the other ones that we might specify too). Additionally we're also going to use [cross validation](https://en.wikipedia.org/wiki/Cross-validation_(statistics)) to tune our hyperparameters, this will allow us to attempt to try to control [overfitting](https://en.wikipedia.org/wiki/Overfitting) of our model.
# MAGIC
# MAGIC Lastly we'll need to set up a [Regression Evaluator](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.evaluation.RegressionEvaluator) that will evaluate the models that we choose based on some metric (the default is RMSE). The key take away is that the pipeline will automatically optimize for our given metric choice by exploring the parameter grid that we set up rather than us having to do it manually like we would have had to do above.
# MAGIC
# MAGIC Now we can go about training our random forest! 
# MAGIC
# MAGIC *note: this might take a little while because of the number of combinations that we're trying and limitations in workers available.*

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

rf_model = RandomForestRegressor(
    labelCol="count",
    featuresCol="features")

param_grid = ParamGridBuilder() \
  .addGrid(rf_model.maxDepth, [5, 10]) \
  .addGrid(rf_model.numTrees, [20, 60]) \
  .build()
# Note, that this parameter grid will take a long time
# to run in the community edition due to limited number
# of workers available! Be patient for it to run!
# If you want it to run faster, remove some of
# the above parameters and it'll speed right up!

steps = [rf_model]

pipeline = Pipeline(stages=steps)

cv = CrossValidator()  # you can feel free to change the number of folds used in cross validation as well
cv.setEstimator(pipeline)  # the estimator can also just be an individual model rather than a pipeline
cv.setEstimatorParamMaps(param_grid)
cv.setEvaluator(RegressionEvaluator(labelCol="count"))

pipeline_fitted = cv.fit(training)

# COMMAND ----------

# MAGIC %md Now we've trained our model! Let's take a look at which version performed best!

# COMMAND ----------

print("The Best Parameters:\n--------------------")
print(pipeline_fitted.bestModel.stages[0])
print(pipeline_fitted.bestModel.stages[0].extractParamMap())

# COMMAND ----------

holdout2 = pipeline_fitted.bestModel \
  .transform(test) \
  .selectExpr("prediction as raw_prediction", 
    "double(round(prediction)) as prediction", 
    "count", 
    """CASE double(round(prediction)) = count 
  WHEN true then 1
  ELSE 0
END as equal""")
display(holdout2)

# COMMAND ----------

# MAGIC %md As well as our regression metrics on the test set.

# COMMAND ----------

# have to do a type conversion for RegressionMetrics
rm2 = RegressionMetrics(
  holdout2.select("prediction", "count").rdd.map(lambda x:
  (float(x[0]), float(x[1]))))

print("MSE: " + str(rm2.meanSquaredError))
print("MAE: " + str(rm2.meanAbsoluteError))
print("RMSE Squared: " + str(rm2.rootMeanSquaredError))
print("R Squared: " + str(rm2.r2))
print("Explained Variance: " + str(rm2.explainedVariance) + "\n")

# COMMAND ----------

# MAGIC %md Finally we'll see an improvement in our "exactly right" proportion as well!

# COMMAND ----------

display(holdout2.selectExpr("sum(equal)/sum(1)"))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Conclusion
# MAGIC
# MAGIC We can see from the above that we identified a fairly significant link by leveraging the pipeline, a more sophisticated model, and better hyperparameter tuning. However these results are still a bit disappointing. With that being said, we're working with very few features and we've likely made some assumptions that just aren't quite valid (like the zip code shortening). Also just because a rich zip code exists doesn't mean that the farmer's market would be held in that zip code too. In fact we might want to start looking at neighboring zip codes or doing some sort of distance measure to predict whether or not there exists a farmer's market in a certain mile radius from a wealthy zip code.

# COMMAND ----------

# MAGIC %md
# MAGIC # Homework:
# MAGIC With that being said, we've got a lot of other potential features and plenty of other parameters to tune on our random forest so play around with the above pipeline and see if you can improve it further!
