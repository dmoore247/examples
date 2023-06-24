# Databricks notebook source
# MAGIC %sh
# MAGIC apt-get install phantomjs -y

# COMMAND ----------

# MAGIC %sh phantomjs -h

# COMMAND ----------

# MAGIC %sh cat <<EOF >test-svg.html
# MAGIC <!DOCTYPE html>
# MAGIC <html>
# MAGIC <head>
# MAGIC     <!-- These paths should be relative to wherever you put this page on your website -->
# MAGIC     <script src="/libraries/RGraph.svg.common.core.js" ></script>
# MAGIC     <script src="/libraries/RGraph.svg.line.js" ></script>
# MAGIC     
# MAGIC     <!-- PhantomJS renders the page with a black/transparent background so set it to white and get rid of margins/padding -->
# MAGIC     <style>
# MAGIC         body {
# MAGIC             background-color: white;
# MAGIC             margin: 0 0 0 0;
# MAGIC             padding: 0 0 0 0;
# MAGIC         }
# MAGIC     </style>
# MAGIC     
# MAGIC     <!-- Don't want this page indexed by search engines -->
# MAGIC     <meta name="robots" content="noindex, nofollow" />
# MAGIC </head>
# MAGIC <body>
# MAGIC
# MAGIC     <!-- This is the div tag where the chart will appear -->
# MAGIC     <div id="cc" style="width: 950; height: 300px"></div>
# MAGIC     
# MAGIC     <!--
# MAGIC         This is the script that generates the chart. Like any other page on your server this could integrate with anything
# MAGIC         else on your server - for example MySQL
# MAGIC     -->
# MAGIC     <script>
# MAGIC         new RGraph.SVG.Line({
# MAGIC             id: 'cc',
# MAGIC             data: [8,4,6,3,5,8,9,8,4,6,3,5,2,4,8,6],
# MAGIC             options: {
# MAGIC                 xaxisLabels: ['Barry','Charles','Olga','Lou','Fred','Hoolio','Gary','Mia','Rich','Kev','John','David','Paul','Fred','Lewis','John'],
# MAGIC                 linewidth: 5,
# MAGIC                 spline: true,
# MAGIC                 backgroundGridVlines: false,
# MAGIC                 backgroundGridBorder: false,
# MAGIC                 yaxis: false
# MAGIC             }
# MAGIC         }).draw();
# MAGIC     </script>
# MAGIC
# MAGIC </body>
# MAGIC </html>
# MAGIC EOF

# COMMAND ----------

# MAGIC %sh ls -al test-svg.html

# COMMAND ----------

# MAGIC %sh cat <<EOF >test-svg.js
# MAGIC // This creates an instance of a page. The require() function is provided by PhantomJS
# MAGIC page = require('webpage').create();
# MAGIC
# MAGIC // Open our test webpage (this is a real page that you can view in your browser)
# MAGIC page.open('https://www.rgraph.net/tests/svg.line/phantomjs.html', function()
# MAGIC {
# MAGIC     // This sets the area of the virtual browser to be saved. The chart is positioned
# MAGIC     // in the top right corner so that's the bit we want
# MAGIC     page.viewportSize = {
# MAGIC         width: 850,
# MAGIC         height: 300
# MAGIC     };
# MAGIC     
# MAGIC     // Render the page to this file
# MAGIC     page.render('test-svg.png');
# MAGIC     
# MAGIC     // Exit PhantomJS cleanly
# MAGIC     phantom.exit(0);
# MAGIC });
# MAGIC EOF

# COMMAND ----------

# MAGIC %sh ls -al test-svg.*

# COMMAND ----------

# MAGIC %sh phantomjs ./test-svg.js

# COMMAND ----------

# MAGIC %sh cp test-svg.png /dbfs/FileStore/plots/test-svg.png

# COMMAND ----------

displayHTML("""<div><p style='bgcolor: black'><img src='/files/plots/test-svg.png'></div>""")

# COMMAND ----------


