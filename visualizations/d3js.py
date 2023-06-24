# Databricks notebook source
displayHTML("""
<div>
<h1>
  <svg width="96" height="91" style="position:relative;top:22px;">
    <clipPath id="clip">
      <path d="M0,0h7.75a45.5,45.5 0 1 1 0,91h-7.75v-20h7.75a25.5,25.5 0 1 0 0,-51h-7.75zm36.2510,0h32a27.75,27.75 0 0 1 21.331,45.5a27.75,27.75 0 0 1 -21.331,45.5h-32a53.6895,53.6895 0 0 0 18.7464,-20h13.2526a7.75,7.75 0 1 0 0,-15.5h-7.75a53.6895,53.6895 0 0 0 0,-20h7.75a7.75,7.75 0 1 0 0,-15.5h-13.2526a53.6895,53.6895 0 0 0 -18.7464,-20z"></path>
    </clipPath>
    <linearGradient id="gradient-1" gradientUnits="userSpaceOnUse" x1="7" y1="64" x2="50" y2="107">
      <stop offset="0" stop-color="#f9a03c"></stop>
      <stop offset="1" stop-color="#f7974e"></stop>
    </linearGradient>
    <linearGradient id="gradient-2" gradientUnits="userSpaceOnUse" x1="2" y1="-2" x2="87" y2="84">
      <stop offset="0" stop-color="#f26d58"></stop>
      <stop offset="1" stop-color="#f9a03c"></stop>
    </linearGradient>
    <linearGradient id="gradient-3" gradientUnits="userSpaceOnUse" x1="45" y1="-10" x2="108" y2="53">
      <stop offset="0" stop-color="#b84e51"></stop>
      <stop offset="1" stop-color="#f68e48"></stop>
    </linearGradient>
    <g clip-path="url(#clip)">
      <path d="M-100,-102m-27,0v300h300z" fill="url(#gradient-1)"></path>
      <path d="M-100,-102m27,0h300v300z" fill="url(#gradient-3)"></path>
      <path d="M-100,-102l300,300" fill="none" stroke="url(#gradient-2)" stroke-width="40"></path>
    </g>
  </svg>
  Data-Driven Documents
</h1>
  </div>
""")

# COMMAND ----------

# MAGIC %md D3.js is a JavaScript library for manipulating documents based on data. D3 helps you bring data to life using HTML, SVG, and CSS. D3’s emphasis on web standards gives you the full capabilities of modern browsers without tying yourself to a proprietary framework, combining powerful visualization components and a data-driven approach to DOM manipulation.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Change these colors to your favorites to change the D3 visualization.
# MAGIC val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (100,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
# MAGIC val colors = colorsRDD.collect()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC displayHTML(s"""
# MAGIC <!DOCTYPE html>
# MAGIC <head>
# MAGIC <meta charset="utf-8">
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
# MAGIC <style>
# MAGIC
# MAGIC path {
# MAGIC   fill: yellow;
# MAGIC   stroke: #000;
# MAGIC }
# MAGIC
# MAGIC circle {
# MAGIC   fill: #fff;
# MAGIC   stroke: #000;
# MAGIC   pointer-events: none;
# MAGIC }
# MAGIC
# MAGIC .PiYG .q0-9{fill:rgb${colors(0)}}
# MAGIC .PiYG .q1-9{fill:rgb${colors(1)}}
# MAGIC .PiYG .q2-9{fill:rgb${colors(2)}}
# MAGIC .PiYG .q3-9{fill:rgb${colors(3)}}
# MAGIC .PiYG .q4-9{fill:rgb${colors(4)}}
# MAGIC .PiYG .q5-9{fill:rgb${colors(5)}}
# MAGIC .PiYG .q6-9{fill:rgb${colors(6)}}
# MAGIC .PiYG .q7-9{fill:rgb${colors(7)}}
# MAGIC .PiYG .q8-9{fill:rgb${colors(8)}}
# MAGIC
# MAGIC </style>
# MAGIC </head>
# MAGIC <body>
# MAGIC
# MAGIC <div id="myd3" style="min-height:10in;">
# MAGIC <script>
# MAGIC
# MAGIC var width = 960,
# MAGIC     height = 500;
# MAGIC
# MAGIC var vertices = d3.range(100).map(function(d) {
# MAGIC   return [Math.random() * width, Math.random() * height];
# MAGIC });
# MAGIC
# MAGIC var svg = d3.select("#myd3").append("svg")
# MAGIC     .attr("width", width)
# MAGIC     .attr("height", height)
# MAGIC     .attr("class", "PiYG")
# MAGIC     .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });
# MAGIC
# MAGIC var path = svg.append("g").selectAll("path");
# MAGIC
# MAGIC svg.selectAll("circle")
# MAGIC     .data(vertices.slice(1))
# MAGIC   .enter().append("circle")
# MAGIC     .attr("transform", function(d) { return "translate(" + d + ")"; })
# MAGIC     .attr("r", 2);
# MAGIC
# MAGIC redraw();
# MAGIC
# MAGIC function redraw() {
# MAGIC   path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
# MAGIC   path.exit().remove();
# MAGIC   path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
# MAGIC }
# MAGIC
# MAGIC </script>
# MAGIC </div>
# MAGIC   """)

# COMMAND ----------


