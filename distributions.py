# Visualising the Normal Distribution using D3
# =============================================
# This is an exploration of different methods of visualising 
# the normal distibution using d3.

# Imports

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,SQLContext
from IPython.display import Javascript,HTML
from pyspark.mllib.random import RandomRDDs
import json
from IPython.display import Javascript,HTML

# "Import" d3. This adds d3 v5 to the output window iframe.

HTML("<script src='https://d3js.org/d3.v5.min.js'></script>")

# Create the spark context

conf = SparkConf().setAppName("strata_distributions")
sc = SparkContext(conf=conf)

# Generate a 10000 random number RDD from that follows a normal 
# distribution.

x = RandomRDDs.normalRDD(sc, 10000, seed=1)
x.take(10)

# The code below creates a histogram with 500 bins from the random number
# RDD. This then converted to a json string using `json.dumps` and the 
# output is pushed a the `hist` variable in the browser using the 
# `Javascript`. This is then accessible as input data for d3.

Javascript("window.hist = {}".format(json.dumps(x.histogram(500)[1])))

# Create an svg element in the output window to use for the first 
# impelemenation

HTML("""
<div id='div_svg'><svg id='svg_hist' width='500' height='200'></svg></div>
""")

# This is the standard visualisation and interpretation of the normal
# distribution. The length of the line corresponds to the number of items
# in that bin.

Javascript("""
const svg = d3.select('#svg_hist');
const margin = ({top: 0, right: 0, bottom: 0, left: 0});
const height = 200, width = 500;

const y = d3.scaleLinear()
    .domain(d3.extent(hist)).nice()
    .range([margin.top, height - margin.bottom]);

const bar = svg.append("g")
      .attr("stroke", "steelblue")
      .attr("stroke-width","1.5")
    .selectAll("line")
    .data(hist)
    .enter()
      .append("line")
      .attr("x1", (d,i) => i )
      .attr("x2", (d,i) => i )
      .attr("y1", (d,i) => height - y(d))
      .attr("y2", d =>  height);
""")

HTML("""
<div id='div_svg'><svg id='svg_fade' width='500' height='200'></svg></div>
""")

# This is color level/denisty interpretation of the normal
# distribution. The darker lines indicate a higher number the number of 
# items in that bin. This is implemented by changing the `opacity` value.

Javascript("""
    const svg = d3.select('#svg_fade');
    const margin = ({top: 0, right: 0, bottom: 0, left: 0});
    const height = 200, width = 500;

    const op = d3.scaleLinear()
        .domain(d3.extent(hist))
        .range([0.001,1]);

    const bar = svg.append("g")
          .attr("stroke", "steelblue")
          .attr("stroke-width","1.5")
        .selectAll("line")
        .data(hist)
        .enter()
          .append("line")
          .attr("x1", (d,i) => i )
          .attr("x2", (d,i) => i )
          .attr("y1", (d,i) => 0)
          .attr("y2", d =>  height)
          .attr("opacity", d => op(d));
""")

HTML("""
<div id='div_svg'><svg id='svg_dots' width='500' height='200'></svg></div>
""")

# This an attempt to show the actual density by plotting each point with
# subtle randomisation to prevent clear repeat patterns. All the points 
# are represented as individual circles

Javascript("""
    const svg = d3.select('#svg_dots');
    const margin = ({top: 0, right: 0, bottom: 0, left: 0});
    const height = 200, width = 500;
    
    var remap_hist = hist.map(function(d,i) { return {"hist_vald":d,"iter":i}})

    const op = d3.scaleLinear()
        .domain(d3.extent(hist))
        .range([height,0]);

    const bar = svg.append("g")
          .attr("stroke", "none")
          .attr("fill","steelblue")
        .selectAll("g")
        .data(hist)
        .enter()
          .append("g")
          .selectAll("circle")
        .data(function (d,i) {return d3.range(d).map(function(e) { return {"hist_val":e,"iter":i,"max":d}})})
           .enter()
           .append("circle")
          .attr("cx", d => d.iter + Math.random())
          .attr("cy", function (d) {
              op.domain([0,d.max]);
              return op(d.hist_val + Math.random());
          })
          .attr("r", 1);

""")