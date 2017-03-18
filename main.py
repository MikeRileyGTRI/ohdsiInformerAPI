from flask import Flask, request
from sqlalchemy import create_engine
from flask_cors import CORS, cross_origin

import json

app = Flask(__name__)
CORS(app)

@app.route("/service1")
def process1():
  return "Hello from process 1!"

@app.route("/query")
def QueryRaw():
  queryRaw = request.args.get('raw')
  queryFormat = "SELECT *  FROM omop_v5.depression_results LIMIT 10"
  response = engine.execute(queryFormat)
#  response_dict = [u[0:2] for u in response.fetchall()]
#  strResponse = ""
#  for row in response:
#    strResponse +=str(row)
#    strResponse +='|'
#  return strResponse
  return json.dumps(response_dict)

def executeAndDump(query):
  print 'DB Query ===================>' + query
  response = engine.execute(query)
  response_dict = [dict(row) for row in response]
  print 'DB Response ================>' + str(response_dict)
  return json.dumps(response_dict)

def accessDB(outColumns,tableName,whereQueries={},schemaName='omop_v5', LIMIT = 100):
  query = "SELECT "
  query += ','.join(outColumns)
  if len(whereQueries) != 0:
    query += ','
  print 'Where Query Keys ==========>' + str(whereQueries.keys())
  query += ','.join(whereQueries.keys())
  query += (" FROM " + '{}.{}'.format(schemaName,tableName))
  if len(whereQueries) != 0:
    query += " WHERE "
  i = 0;
  for condition in whereQueries:
    if ',' in whereQueries[condition]:
      conditionArray = whereQueries[condition].split(',')
      query += condition + ' in (' + ','.join(conditionArray) + ')'
    else: 
      query += condition + " = " + whereQueries[condition]
    if i < len(whereQueries)-1:
      query += ' AND '
      i = i + 1
  query += " LIMIT {} ;".format(LIMIT)
  return executeAndDump(query);

def setQueryParams(request,inColumns):
  whereQueries = {}
  for queryParam in request.args:
    print 'queryParam ============>' + queryParam
    if queryParam in inColumns:
      whereQueries[queryParam] = request.args.get(queryParam)
  return whereQueries 

@app.route("/depression_results")
def depressionResults():
  outColumns = ['calci95lb','calci95ub','calrr','calp','treated']
  inColumns = ['outcomeid','targetid','comparatorid']
  whereQueries = setQueryParams(request,inColumns)
  return accessDB(outColumns,'depression_results',whereQueries)


@app.route('/irs')
def irs():
  inColumns = ['drug_concept_id','drug_name','condition_concept_id','condition_concept_name']
  outColumns = ['num_persons_post_itt','pt_itt','ir_itt_1000pp']
  whereQueries = setQueryParams(request,inColumns)
  return accessDB(outColumns,'irs_clean',whereQueries)

if __name__ == "__main__":
  engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
  app.run()
