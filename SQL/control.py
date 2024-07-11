from pyspark.errors import ParseException, AnalysisException
from sqlglot import expressions as exp
from sqlglot.errors import ParseError
import sqlglot
import os
import glob
import re
import logging
logging.basicConfig(level=logging.WARN)

do_validate = False
do_spark = False

from databricks.connect import DatabricksSession

if do_spark:
    spark = DatabricksSession.builder.getOrCreate()

def parse_structure(sql):
    """
        Find unsupported statement types
    """
    try:
        parse = sqlglot.parse_one(sql, read="tsql", error_level=sqlglot.ErrorLevel.WARN)
        r = parse.find_all(exp.Expression)
        keys = [_.key for _ in r]
        if 'select' in keys and 'into' in keys:
            return "SELECT INTO", "CTAS"
        if 'update' in keys and 'from' in keys:
            return "UPDATE FROM", "MERGE INTO"
        if 'create' in keys and 'PROC' in parse.find(exp.Create).args['kind']:
            return "CREATE PROCEDURE", "Notebook"
        if 'command' in keys and 'literal' in keys and 'DECLARE' in parse.alias_or_name:
            return "DECLARE", "SET var.<variable> = "
    except ParseException as e:
        logging.warning(sql, 'ParseException caused ', e)
    except ParseError as e:
        logging.warning(sql, 'ParseError caused ', e)
    except Exception as e:
        logging.warning(sql, 'Exception caused ', e)
    return "",""

def validate(sql:str):
    """
        Run spark explain COST to parse the databricks sql statement.
    """
    if do_validate and do_spark:
        plan = spark.sql("explain COST " + sql).collect()[0][0]
    else:
        plan = ""
    return plan


def explain(sql:str):
    plan = None
    parsed = False
    error_class = None
    structure = None
    strategy = None
    try:
        plan = validate(sql)
        parsed = True
        if "AnalysisException" in plan:
            error_class = "AnalysisException"
            parsed = False
            plan = validate(sql)
    except ParseError as e:
        plan = str(e)
        error_class = e.getErrorClass()
        structure, strategy = parse_structure(sql)
    except ParseException as e:
        plan = str(e)
        error_class = e.getErrorClass()
        structure, strategy = parse_structure(sql)
    except AnalysisException as e:
        plan = str(e)
        error_class = e.getErrorClass()
    except Exception as e:
        plan = str(e)
        error_class = e.getErrorClass()

    return parsed, plan, error_class, structure, strategy

def run_parse(sqls:str):
    statement = 0
    for i,sql in enumerate(sqls.split(';')):
        if sql is not None and len(sql) > 3:
            transpiled = []
            try:
                transpiled = sqlglot.transpile(sql, read="tsql", write="databricks")
            except ParseError as e:
                e_str = str(e.errors)
                logging.warning(e_str)
                continue

            for i,dbx_sql in enumerate(transpiled):
                statement = statement + 1
                parsed, plan, error_class, structure, strategy = explain(dbx_sql)
                yield statement, sql, dbx_sql, parsed, plan, error_class, structure, strategy

def run_parse_report(results):
    html = """<html><body><table width="1000px" rules="all"><tr>
        <th>Statement #</th>
        <th>Parsed?</th>
        <th>Error<th>
        <th>Original</th>
        <th>Transpiled</th>
        <th>Plan</th></tr>\n"""
    for statement, sql, dbx_sql, parsed, plan, error_class in results:
        html = html + f"""<tr>
        <td>{statement}</td>
        <td>{parsed}</td>
        <td>{error_class}</td>
        <td>{sql}</td>
        <td>{dbx_sql}</td>
        <td>{plan}</td>
        </tr><tr background=#333333><td></td></tr>"""
    html = html + ("</table></body></html>")
    return html

def run_files(path:str):
    """ For path with glob, yield sql text"""
    paths = glob.glob(path)
    for sql_file in paths:
        with open(sql_file,'r') as f:
            yield sql_file, f.read()

def run_tsql(sql_blob:str):
    """
        Split SQL Blob on GO or ';' or SQL comments (--)
    """
    pattern = '\nGO\n'
    for statement in re.split(pattern, sql_blob):
        if statement.isspace():
            continue
        yield statement.strip()

def save(results):
    if do_spark:
        schema_string = "id INT, original_sql STRING, dbx_sql STRING, parsed BOOLEAN, plan STRING, error_class STRING, structure STRING, strategy STRING, file_name STRING"
        df = spark.createDataFrame(results, schema=schema_string)
        df.write.mode('append').option('mergeSchema','true').saveAsTable(table_name)

def run_all(path:str, table_name:str):
    assert path is not None
    assert table_name is not None
    
    for file_name, sql_blob in run_files(path):
        logging.info(f"Running {file_name}")
        results = []
        for statement in run_tsql(sql_blob):
            for result in run_parse(statement):
                results.append((*result, file_name))
        logging.debug(results)
        save(results)

if __name__ == "__main__":
    logging.info("Running")
    do_validate = False
    run_all("resources/*.sql", "douglas_moore.sqlglot.project1")