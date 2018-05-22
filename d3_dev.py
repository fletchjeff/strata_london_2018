#Developing a D3 visualistion in CDSW
#====================================
#This is a sample CDSW notebook to use if you need to develope a D3 visualisation.
#The two important things to understand: 
# * The first is how to update the javascript code without
# having the d3 element you are working on keep scrolling up the page. 
# * The second is moving the data from the python driver into the browser so that d3 can
# bind to it.

## Imports
# You will definitely need to use `Javascript` and `HTML` from `IPython.display` which is 
# compatibile with CDSW

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,SQLContext
from IPython.display import Javascript,HTML
import json
import seaborn as sns
import pandas

## Loading D3
# This is also kind of an import. The display page of CDSW runs in it own iframe. While 
# there is a version of d3 used by the main application, you can override the version
# that is shipped with CDSW, to the latest version use the following code:

HTML("<script src='https://d3js.org/d3.v5.min.js'></script>")

# If you want to do any work on the developer console, you need to click inside the output
# window and lanch the developer tool to make sure you select inside the iframe. On Chrome 
# that is right-click -> Inspect

## Loading some data
# This loads the iris data from a csv in HDFS on a cluster into a spark dataframe and 
# then converts it into a pandas dataframe. 


conf = SparkConf().setAppName("strata_d3_dev")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

iris_rdd = sqlContext.read.csv("iris.csv", header=True, inferSchema=True)
iris = iris_rdd.toPandas()

# If you can don't want to run this through spark, 
# comment out the code above and just run `iris = sns.load_dataset('iris')`

iris.sample(10)

# There are only 3 species types in the data, so colors of the bars are assigned with 
# `d3.schemeCategory10`

iris['species'].drop_duplicates().tolist()

## Adding a fixed element to the output
# The most effective way to do d3 development work is to have a DOM element that can be
# accessed by d3, but that stays just above the interactive prompt. The following line is
# to to clear the output and rerun all the code without restarting the workbench. On the 
# first run, it doesn't do anything.

Javascript('d3.select("body").select("#svg_container").remove()')

# This will add a div tag above the interactive prompt that doesn't move when you add and
# execute new code. You can access it using the `#svg_container` selector in d3.

Javascript('d3.select("body").append("div").attr("id","svg_container")')

# While it is possible to have lots of inline Javascript using the `Javascript` tag. Its
# hard to manage as there is no syntax highlighting. Its far simpler to have a .js file in
# the CDSW project and load it.

Javascript(filename='d3_dev.js')

# This is the important part of this code, and some that you will likely need to figure 
# out how to modify for your own code.
# d3 can work with JSON formatted data, and rather than save and load the file, its possible
# create a Javascrpit object and move the data from the python/pyspark process into the
# browser's memory.

# This code below selects a random sample of 10 rows from the iris dataframe. Its pushed to 
# the DOM in d3 specific format by converting the data frame to a dictionary,
# using `orient='records'`. At this point is just a long, correctly formatted
# string being run as code inside the browser that creates an oject that d3 can work with.

record_set = json.dumps(iris.sample(10).to_dict(orient='records'))
Javascript("updater({});".format(record_set))

# if you want to update the bar graph, just select the two rows above and re-run them.
