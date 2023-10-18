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
    query = """select m.*
                from (select movie_id, count(movie_id)as cnt 
                        from view_count v
                        where v.reg_date >= sysdate - 30
                        group by movie_id order by cnt desc)a, movie m
                where a.movie_id=m.movie_id"""
    df = pd.read_sql_query(query,conn)
    df.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings']
    json_data = df.to_json(orient='records', force_ascii=False)

    return jsonify(json_data)

if __name__ == '__main__':  
   app.run('127.0.0.1',port=5000,debug=True)


