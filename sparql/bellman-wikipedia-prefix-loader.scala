// Databricks notebook source
// MAGIC %md # Query Wikidata on Cancer using the GSK Bellman Sparql engine performance testing job
// MAGIC 
// MAGIC 
// MAGIC This databricks notebook reproduces the [GSK demo notebook](https://github.com/gsk-aiops/bellman-tools/blob/main/notebooks/Bellman%20Sparql%20Demo.scala)
// MAGIC and the 'bellmans_tools' library by adding a couple helper functions inline
// MAGIC 
// MAGIC Github: https://github.com/gsk-aiops/bellman/tree/v2.0.0
// MAGIC ## Requirements:
// MAGIC 
// MAGIC ### Libraries:
// MAGIC Attach these Maven libraries and jar files to the Databricks cluster in this order
// MAGIC 
// MAGIC | Coordinates | Installation | Path |
// MAGIC | --- | --- | --- |
// MAGIC |`com.github.gsk-aiops:bellman-spark-engine_2.12:2.0.0` | Maven Installed | |
// MAGIC |`activation_1_1_1.jar` | JAR Installed | `dbfs:/FileStore/jars/c2f4bd8d_d450_4d4b_8790_70d96f6a40d7-activation_1_1_1-b8489.jar` |
// MAGIC |`stax_api_1_0_2.jar` | JAR Installed | `dbfs:/FileStore/jars/5577ee01_9f95_439d_80de_f277738c1975-stax_api_1_0_2-7d618.jar` |
// MAGIC |`sansa_rdf_spark_2_12_0_8_0_RC3__1_.jar` | JAR Installed | `dbfs:/FileStore/jars/6c5ef375_1be2_4f2e_84b1_d77a48617548-sansa_rdf_spark_2_12_0_8_0_RC3__1_-55fa6.jar` |
// MAGIC 
// MAGIC 
// MAGIC ### Databricks cluster spec:
// MAGIC ```
// MAGIC {
// MAGIC     "autoscale": {
// MAGIC         "min_workers": 2,
// MAGIC         "max_workers": 8
// MAGIC     },
// MAGIC     "cluster_name": "douglas.moore@databricks.com's Cluster",
// MAGIC     "spark_version": "11.1.x-scala2.12",
// MAGIC     "spark_conf": {},
// MAGIC     "aws_attributes": {
// MAGIC         "first_on_demand": 1,
// MAGIC         "availability": "SPOT_WITH_FALLBACK",
// MAGIC         "zone_id": "us-west-2a",
// MAGIC         "instance_profile_arn": "arn:aws:iam::997819012307:instance-profile/oetrta-IAM-access",
// MAGIC         "spot_bid_price_percent": 100,
// MAGIC         "ebs_volume_count": 0
// MAGIC     },
// MAGIC     "node_type_id": "i3.xlarge",
// MAGIC     "driver_node_type_id": "i3.xlarge",
// MAGIC     "ssh_public_keys": [],
// MAGIC     "custom_tags": {},
// MAGIC     "spark_env_vars": {
// MAGIC         "JNAME": "zulu11-ca-amd64"
// MAGIC     },
// MAGIC     "autotermination_minutes": 120,
// MAGIC     "enable_elastic_disk": false,
// MAGIC     "cluster_source": "UI",
// MAGIC     "init_scripts": [],
// MAGIC     "single_user_name": "douglas.moore@databricks.com",
// MAGIC     "data_security_mode": "SINGLE_USER",
// MAGIC     "runtime_engine": "STANDARD",
// MAGIC     "cluster_id": "0819-212233-fn2u56to"
// MAGIC }
// MAGIC ```

// COMMAND ----------

// MAGIC %md ## Configure

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",100*1024*1024) // 100MB
spark.conf.set("spark.sql.files.maxPartitionBytes", 32 * 1024 * 1024)

// COMMAND ----------

// MAGIC %md ## Setup

// COMMAND ----------

// DBTITLE 1,Helper functions from bellman_tools
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{DataFrame}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang

object Utils {
  org.apache.jena.query.ARQ.init()
  implicit val s = spark.sqlContext
  /**
   * Converts a valid NT file to a dataframe
   * @param i Path to input file
   * @return Dataframe with columns s,p,o
   */
  def ntToDf(i:String):DataFrame = {
    spark.read.rdf(Lang.NTRIPLES)(i)
  }
}

// COMMAND ----------

import com.gsk.kg.engine.syntax._
implicit val sqlcntx = spark.sqlContext // needed by bellman-spark jars

import org.apache.jena.riot.Lang
org.apache.jena.query.ARQ.init()

// COMMAND ----------

/**
The Wikimedia data used in this demo can be found here:
https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.bz2
The size is ~30GB so be sure to have enough disk space!
After downloading and then staging the data either in HDFS, or an S3 bucket or an Azure blob store, you must convert the data to parquet format. Bellman tools has a method for converting .nt to a dataframe. You can either query this dataframe directly or save it to Parquet for querying later. Here we are electing to do the latter in case we'd like to come back and read directly from Parquet.
**/

//val dfz = Utils.ntToDf("s3://oetrta/dmoore/data/wikidata/wikidata-20220819-lexemes-BETA.nt.bz2")

// COMMAND ----------

// MAGIC %md # Load Prefixes
// MAGIC Source: 
// MAGIC http://prefix.cc/popular/all.file.sparql

// COMMAND ----------

// MAGIC %md # Computed columns

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from douglas_moore_bronze.wikidata where s like "https%"

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE douglas_moore_silver.wikidata_typed (
// MAGIC     s           STRING GENERATED ALWAYS AS (CONCAT(s_prefix, s_value))  COMMENT "Subject",
// MAGIC     s_prefix    STRING not null COMMENT "part typically used in Sparql PREFIX",
// MAGIC     s_value     STRING not null COMMENT "value part that follows the prefix",
// MAGIC 
// MAGIC     p           STRING GENERATED ALWAYS AS (CONCAT(p_prefix, p_value))  COMMENT "Predicate",
// MAGIC     p_prefix    STRING not null COMMENT "part typically used in Sparql PREFIX",
// MAGIC     p_value     STRING not null COMMENT "value part that follows the prefix",
// MAGIC 
// MAGIC     o           STRING GENERATED ALWAYS AS (CONCAT(o_prefix, o_value, o_uri, o_type, o_lang))  COMMENT "Object",
// MAGIC     o_prefix    STRING COMMENT "optional sparql prefix",
// MAGIC     o_uri       STRING COMMENT "optional URI value",
// MAGIC     o_value     STRING COMMENT "value, if literal, enclosed in quotes",
// MAGIC     o_type      STRING COMMENT "xmlschema data type",
// MAGIC     o_lang      STRING COMMENT "@lang if value is string"
// MAGIC )
// MAGIC USING delta
// MAGIC LOCATION 's3://oetrta/dmoore/silver/wikidata_typed'
// MAGIC TBLPROPERTIES (
// MAGIC   delta.targetFileSize = 16777000,
// MAGIC   delta.tuneFileSizesForRewrites = false
// MAGIC );

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace table douglas_moore_silver.prefixes (
// MAGIC     alias string not null,
// MAGIC     prefix string not null
// MAGIC )

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC with splits as (
// MAGIC     SELECT
// MAGIC     split(s,"/") as s,
// MAGIC     split(p,"[/|#]") as p,
// MAGIC     split(o,"/") as o
// MAGIC     from douglas_moore_bronze.wikidata
// MAGIC     where o = "http://www.wikidata.org/entity/Q146"
// MAGIC ),
// MAGIC splitter as (
// MAGIC     select s, size(s) as s_length,
// MAGIC            p, size(p) as p_length,
// MAGIC            o, size(o) as o_length
// MAGIC     from splits
// MAGIC ),
// MAGIC vals as (
// MAGIC     SELECT
// MAGIC     array_join(slice(s,1,s_length-1), "/", "/") as s_prefix,
// MAGIC     s[s_length-1] as s_value,
// MAGIC     array_join(slice(p,1,p_length-1), "/", "/") as p_prefix,
// MAGIC     p[p_length-1] as p_value
// MAGIC     from splitter
// MAGIC )
// MAGIC select 
// MAGIC     distinct s_prefix, p_prefix
// MAGIC from vals

// COMMAND ----------

// MAGIC %md #Prefixes

// COMMAND ----------

// MAGIC %python
// MAGIC vals = """PREFIX bd: <http://www.bigdata.com/rdf#>
// MAGIC PREFIX cc: <http://creativecommons.org/ns#>
// MAGIC PREFIX dct: <http://purl.org/dc/terms/>
// MAGIC PREFIX geo: <http://www.opengis.net/ont/geosparql#>
// MAGIC PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>
// MAGIC PREFIX owl: <http://www.w3.org/2002/07/owl#>
// MAGIC PREFIX p: <http://www.wikidata.org/prop/>
// MAGIC PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
// MAGIC PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
// MAGIC PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
// MAGIC PREFIX pr: <http://www.wikidata.org/prop/reference/>
// MAGIC PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>
// MAGIC PREFIX prov: <http://www.w3.org/ns/prov#>
// MAGIC PREFIX prv: <http://www.wikidata.org/prop/reference/value/>
// MAGIC PREFIX ps: <http://www.wikidata.org/prop/statement/>
// MAGIC PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
// MAGIC PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
// MAGIC PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
// MAGIC PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
// MAGIC PREFIX schema: <http://schema.org/>
// MAGIC PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
// MAGIC PREFIX wd: <http://www.wikidata.org/entity/>
// MAGIC PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/>
// MAGIC PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
// MAGIC PREFIX wdref: <http://www.wikidata.org/reference/>
// MAGIC PREFIX wds: <http://www.wikidata.org/entity/statement/>
// MAGIC PREFIX wdt: <http://www.wikidata.org/prop/direct/>
// MAGIC PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>
// MAGIC PREFIX wdv: <http://www.wikidata.org/value/>
// MAGIC PREFIX wikibase: <http://wikiba.se/ontology#>
// MAGIC PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"""

// COMMAND ----------

// MAGIC %python
// MAGIC prefix = []
// MAGIC for lines in vals.split('\n'):
// MAGIC   p,a,uri = lines.split(' ')
// MAGIC   uri = uri.strip('<>')
// MAGIC   a = a.strip(':')
// MAGIC   prefix.append((a,uri))
// MAGIC prefix
// MAGIC df = spark.createDataFrame(prefix,"alias string, uri string")
// MAGIC display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from douglas_moore_silver.prefixes

// COMMAND ----------

// MAGIC %python
// MAGIC df.write.mode('overwrite').option('overwriteSchema','true').saveAsTable("douglas_moore_silver.prefixes")

// COMMAND ----------

// MAGIC %md # Validate

// COMMAND ----------

// MAGIC %sql
// MAGIC use douglas_moore_silver;
// MAGIC 
// MAGIC select 
// MAGIC   pre.alias, pre.uri, 
// MAGIC   count(distinct w.s) s_count, count(distinct w.p) p_count, count(distinct w.o) o_count,
// MAGIC   first(w.s) s_first, first(w.p) p_first
// MAGIC from douglas_moore_bronze.wikidata as w
// MAGIC LEFT JOIN prefixes pre
// MAGIC   ON left(w.s,len(pre.uri)) = pre.uri
// MAGIC --LEFT JOIN prefixes pre_p
// MAGIC --  ON left(w.p,len(pre_p.uri)) = pre_p.uri
// MAGIC where o = "http://www.wikidata.org/entity/Q146" -- cats
// MAGIC GROUP BY 1,2

// COMMAND ----------

// MAGIC %sql
// MAGIC select slice(split(wd.s, '/'),1,4), count(1)
// MAGIC from douglas_moore_bronze.wikidata wd 
// MAGIC group by 1
// MAGIC order by 2 desc

// COMMAND ----------

// MAGIC %sql
// MAGIC with splits as (
// MAGIC     select split(wd.s, '[/#]') s_array, split(wd.p, '[/]') p_array, split(wd.o, '[(^^)(#)@<>]') o_array, s, p, o
// MAGIC     from douglas_moore_bronze.wikidata tablesample(1 percent) wd 
// MAGIC ),
// MAGIC splitl as (
// MAGIC     select 
// MAGIC         s_array, size(s_array) s_len, s, 
// MAGIC         p_array, size(p_array) p_len, p, 
// MAGIC         o_array, size(o_array) o_len, o
// MAGIC     from splits
// MAGIC )
// MAGIC select 
// MAGIC     slice(s_array,1,s_len-1),
// MAGIC     slice(p_array,1,p_len-1),
// MAGIC     CASE o_array[0] WHEN 'http' THEN slice(o_array,1,o_len-1) ELSE o_array END
// MAGIC from splitl
// MAGIC --where o like '%XMLSchema%'

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables

// COMMAND ----------

// MAGIC %md # more testing

// COMMAND ----------

// filter query based on object
val q = """
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?variant ?p
WHERE
{ 
    ?variant ?p wd:Q212961
}
LIMIT 1000
"""
display(dfp.sparql(q))

// COMMAND ----------

val q = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#/>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT ?item 
WHERE 
{
  ?item wdt:P31 wd:Q146. # Must be of a cat
}
"""
display(dfp.sparql(q))

// COMMAND ----------

//Find all gene variants that are positive prognostic indicators of pancreatic cancer. While we are at it, also get the gene
//Check against Wikidata sparql endpoint to ensure consistent results
//This query shows us there is one gene variant that has a positiv prognostic association
display(dfp.sparql("""
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?variant ?p
WHERE
{ 
    ?variant ?p wd:Q212961
}
LIMIT 10
"""))

// COMMAND ----------

//Find all gene variants that are positive prognostic indicators of pancreatic cancer. While we are at it, also get the gene
//Check against Wikidata sparql endpoint to ensure consistent results
//This query shows us there is one gene variant that has a positiv prognostic association
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?variant ?gene
WHERE
{ 
    ?variant wdt:P5137 ?gene ;
}
LIMIT 10
"""))

// COMMAND ----------

//Find all gene variants that are positive prognostic indicators of pancreatic cancer. While we are at it, also get the gene
//Check against Wikidata sparql endpoint to ensure consistent results
//This query shows us there is one gene variant that has a positiv prognostic association
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?variant ?disease #?gene
WHERE
{ 
    ?variant wdt:P5137 wd:Q212961 ;
           #wdt:P3433 ?gene . # P3433 biological variant of
    
  BIND(wd:Q212961 AS ?disease)
}
LIMIT 10
"""))

// COMMAND ----------

// MAGIC %md
// MAGIC [Check above query against Wikidata](https://query.wikidata.org/#PREFIX%20%20schema%3A%20%3Chttp%3A%2F%2Fschema.org%2F%3E%0APREFIX%20%20rdf%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0APREFIX%20%20xml%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2FXML%2F1998%2Fnamespace%3E%0APREFIX%20%20xsd%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23%3E%0APREFIX%20wdt%3A%20%3Chttp%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2F%3E%0APREFIX%20wd%3A%20%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2F%3E%0A%0ASELECT%20%3Fvariant%20%3Fdisease%20%3Fgene%0AWHERE%0A%7B%20%0A%0A%20%20%20%20%3Fvariant%20wdt%3AP3358%20wd%3AQ212961%20%3B%20%23%20P3358%20Positive%20prognostic%20predictor%0A%20%20%20%20%20%20%20%20%20%20wdt%3AP3433%20%3Fgene%20.%20%23%20P3433%20biological%20variant%20of%0A%20%20%20%20%0A%20%20BIND%28wd%3AQ212961%20AS%20%3Fdisease%29%0A%0A%7D%0ALIMIT%2010)

// COMMAND ----------

//Generalize and give me all gene variants that have a positive prognostic indicator, and also give me the gene.
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?variant ?disease ?gene
WHERE
{ 
    ?variant wdt:P3358 ?disease ; # P3358 positive prognostic indicator
      wdt:P3433 ?gene .    
}
"""))

// COMMAND ----------

//Give me all gene variants that have a positive therapeudic indicator. Also give me the gene
display(dfp.sparql("""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?variant ?disease ?gene
WHERE
{ 
    ?variant wdt:P3354 ?disease ; # P3354 positive therapeudic indicator
      wdt:P3433 ?gene .    
}
"""))

// COMMAND ----------

// predicate(p) filter Give me all treatments for all diseases
"""
PREFIX  schema: <http://schema.org/>
PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX  wdt: <http://www.wikidata.org/prop/direct/>
PREFIX  wd: <http://www.wikidata.org/entity/>
SELECT  ?disease ?treatment
"""

// COMMAND ----------

// MAGIC %md # Unit Tests

// COMMAND ----------

import com.gsk.kg.engine.DataFrameTyper
import com.gsk.kg.config.Config

val config = Config.default
val dfx = spark.sql("""select s,p,o from douglas_moore_bronze.wikidata """)
val typed   = DataFrameTyper.typeDataFrame(dfx, config)

// COMMAND ----------

typed.explain

// COMMAND ----------

typed.cache.count

// COMMAND ----------

val filtered = typed.where("""p.value = "<http://www.wikidata.org/entity/Q186969>" """)

// COMMAND ----------

filtered.explain("extended")

// COMMAND ----------

display(filtered)

// COMMAND ----------

display(typed)

// COMMAND ----------


