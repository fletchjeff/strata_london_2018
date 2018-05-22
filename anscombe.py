# Spark to D3 with Anscombe's Quartet
# ==============================
# This is a demonstrations of why visualisation is important using
# Apache Spark and Anscombe's quartet.


# Imports

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,SQLContext
import json
from pyspark.mllib.stat import Statistics
from IPython.display import Javascript,HTML
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create the SparkContext. This doesn't need Spark, but this shows 
# how the same concept could be applied to a much bigger data set.

conf = SparkConf().setAppName("strata_anscombe")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# "Import" d3. This adds d3 v5 to the output window iframe.

HTML("<script src='https://d3js.org/d3.v5.min.js'></script>")

# Create and RRD of the Anscombe data set.

anscombe_array = sc.parallelize(
[[10,8.04,10,9.14,10,7.46,8,6.58],
[8,6.95,8,8.14,8,6.77,8,5.76],
[13,7.58,13,8.74,13,12.74,8,7.71],
[9,8.81,9,8.77,9,7.11,8,8.84],
[11,8.33,11,9.26,11,7.81,8,8.47],
[14,9.96,14,8.1,14,8.84,8,7.04],
[6,7.24,6,6.13,6,6.08,8,5.25],
[4,4.26,4,3.1,4,5.39,19,12.5],
[12,10.84,12,9.13,12,8.15,8,5.56],
[7,4.82,7,7.26,7,6.42,8,7.91],
[5,5.68,5,4.74,5,5.73,8,6.89]]
)


# Showning a dataframe version as its easier to read.

anscombe_array.toDF(
    ['x1','y1','x2','y2','x3','y3','x4','y4']
  ).show()

# Display the summary statistics

summary = Statistics.colStats(anscombe_array)
for i in range(4):
  print "Mean for group {} : x = {} y = {}".format(i+1,summary.mean()[2*i],summary.mean()[2*i+1])
  print "Variance for group {} : x = {} y = {}".format(i+1,summary.variance()[2*i],summary.variance()[2*i+1])
  print "-"

# Display the correlations

for i in range(4):
  correlation = Statistics.corr(anscombe_array.map(lambda row: row[2*i]),anscombe_array.map(lambda row: row[2*i+1]))
  print "Correlation for group {}: {}".format(i+1,correlation)
  print "-"

# The data needs to get to the browser as an object the d3 can use.
# The first step is to conver the Spark dataframe to a pandas dataframe.

pandas_array = anscombe_array.toDF(
    ['x1','y1','x2','y2','x3','y3','x4','y4']
  ).toPandas()

# The next step is to loop through the 4 sets of data and build up
# an array of dictionaries mapped to x and y for each data point.

json_out = []

for i in range(4):
  json_out.append(
    pandas_array[['x'+str(i+1),'y'+str(i+1)]].rename(
      index=str, columns={"x"+str(i+1): "x", "y"+str(i+1): "y"}
    ).to_dict(orient='records')
  )

# This array is then converted to a JSON string and pushed to the 
# browser as the `data` object using the `Javascript` function.

Javascript("window.data = {}".format(json.dumps(json_out)))

# The other requirement is to fit a line for all 4 data sets. This is 
# done using the `LinearRegression` function in SparkML. Plotting the 
# lines will require the coefficient and intercept values.

coeff_out = []

for i in range(4):
  anscombe_va = VectorAssembler(inputCols = ['x'+str(i+1)], outputCol = 'fx'+str(i+1))
  anscombe_df = anscombe_array.toDF(
    ['x1','y1','x2','y2','x3','y3','x4','y4']
  )
  anscombe_df = anscombe_va.transform(anscombe_df)
  lr = LinearRegression(
    featuresCol = 'fx'+str(i+1), 
    labelCol='y'+str(i+1), 
    maxIter=10, 
    regParam=0.3, 
    elasticNetParam=0.8)
  lr_model = lr.fit(anscombe_df)
  coeff_out.append({"coefficient":lr_model.coefficients[0],"intercept":lr_model.intercept})
  print("Coefficient for group {} : {}".format(i+1,lr_model.coefficients[0]))
  print("Intercept for group {} : {}".format(i+1,lr_model.intercept))
  print "-"

# The array of coefficients also needs to be pushed to the browser

Javascript("window.coeff_data = {}".format(json.dumps(coeff_out)))

# Create 4 div elements for each graph to be displayed and run the 
# javascript

HTML("<div id='anscombe_0' style='float:left;'></div><div id='anscombe_1'></div>")
HTML("<div id='anscombe_2' style='float:left;'></div><div id='anscombe_3'></div>")
HTML("<div id='start' onclick='start_loop()' style='width:100px;height:20px;background-color:#ddd;text-align:center'>Start</div>")
Javascript(filename='anscombe.js')