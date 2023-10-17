from flask import Flask, jsonify
import cx_Oracle as oracle
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

user = "hr" 
pw = "a1234" 
dsn = "localhost:1521/xe" 

# 연결 
conn = oracle.connect(user=user, password=pw, dsn=dsn)

# 커서 
cursor = conn.cursor()

@app.route('/movieList/')
def index():
#    return 'testData'
    query = 'select * from movie'
    df = pd.read_sql_query(query,conn)
    json_data = df.to_json(orient='records', force_ascii=False)
    return jsonify(json_data)

if __name__ == '__main__':  
   app.run('127.0.0.1',port=5000,debug=True)


