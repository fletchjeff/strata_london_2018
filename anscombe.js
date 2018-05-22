const size = ({ width: 400, height: 200 });

const margin = ({ top: 10, right: 10, bottom: 30, left: 20 });

//CDSW has underscore.js included. Handy!
var xScale = d3.scaleLinear()
    .domain(d3.extent(
        _.flatten(
            data.map(
                function(d) {
                    return d.map(
                        function(e) {
                            return e.x;
                        });
                }))))
    .range([margin.left, size.width - margin.right]);



var yScale = d3.scaleLinear()
    .domain(d3.extent(
        _.flatten(
            data.map(
                function(d) {
                    return d.map(
                        function(e) {
                            return e.y;
                        });
                }))))
    .range([size.height - margin.bottom, margin.top]);

var xAxis = d3.axisBottom(xScale)
    .tickSize(3)
    .ticks(5);

var yAxis = d3.axisLeft(yScale)
    .tickSize(3)
    .ticks(5);

function customXAxis(g) {
    g.call(xAxis);
    g.selectAll(".tick text").attr("fill", "#777");
    g.selectAll(".tick line").attr("stroke", "#777");
    g.select(".domain").remove();
}

function customYAxis(g) {
    g.call(yAxis);
    g.selectAll(".tick text").attr("fill", "#777");
    g.selectAll(".tick line").attr("stroke", "#777");
    g.select(".domain").remove();
}


//this is main loop that draws the 4 graphs
window.start_loop = function() {
  for (i = 0; i < 4; i++) {
    var svg = d3.select("#anscombe_"+i)
        .append("svg")
        .attr("height", size.height)
        .attr("width", size.width);

    svg.append('g')
        .attr('transform', 'translate(0, ' + (size.height - margin.bottom) + ')')
        .call(customXAxis);

    svg.append('g')
        .attr('transform', 'translate(' + margin.left + ', 0)')
        .call(customYAxis);

    svg.append("g")
        .selectAll("circle")
        .data(data[i])
        .enter().append("circle")
        .attr("stroke", "#444")
        .attr("stroke-opacity", 1)
        .attr("fill", "#444")
        .attr("cx", d => xScale(d.x))
        .attr("cy", d => yScale(d.y))
        .attr('r', 0)
        .transition()
        .delay((d,i) => i*200)
        .attr("r", 2.5);

    svg.append("line")
        .attr("x1", xScale.range()[0])
        .attr("y1", yScale(coeff_data[i].intercept))
        .attr("x2", xScale.range()[1])
        .attr("y2", yScale(coeff_data[i].coefficient * xScale.domain()[1] + coeff_data[i].intercept))
        .attr("stroke", "red")
        .attr("stroke-width", 2)
        .attr("opacity",0)
        .transition()
        .delay(2500)
        .attr("opacity",0.3);  
  }
}