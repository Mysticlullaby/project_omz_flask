from flask import Flask, jsonify, request
import cx_Oracle as oracle
import pandas as pd
import json
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

@app.route('/movieList/omzPopular')
def index():

    query = """SELECT m.*
                FROM (
                SELECT movie_id, cnt
                FROM (
                    SELECT movie_id, COUNT(movie_id) AS cnt
                    FROM view_count
                    WHERE reg_date >= SYSDATE - 30
                    GROUP BY movie_id
                    ORDER BY COUNT(movie_id) DESC
                ) a
                WHERE ROWNUM <= 5
                ) b
                JOIN movie m ON b.movie_id = m.movie_id"""
    
    df = pd.read_sql_query(query,conn)
    df.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings', 'provider', 'kinoRating', 'rottenRating', 'imdbRating', 'staff', 'tags', 'releaseDate', 'category']
    json_data = df.to_json(orient='records', force_ascii=False)

    return jsonify(json_data)

@app.route('/movieList/netflixPopular')
def netflixPopular():

    query = """SELECT d.*
                FROM (SELECT c.*, rownum as rm
                    FROM (SELECT b.*
                            FROM (SELECT m.movie_Id, count(v.movie_id) as view_count
                                FROM movie m, view_count v
                                WHERE m.movie_id = v.movie_id(+) AND m.provider LIKE '%넷플릭스%'
                                GROUP BY m.movie_id
                                ORDER BY view_count DESC)a, movie b
                            WHERE a.movie_id = b.movie_id)c)d
                WHERE rm <= 5"""
    df = pd.read_sql_query(query,conn)
    df.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings', 'provider', 'kinoRating', 'rottenRating', 'imdbRating', 'staff', 'tags', 'releaseDate', 'category', 'rm']
    json_data = df.to_json(orient='records', force_ascii=False)

    return jsonify(json_data)

@app.route('/movieList/tvingPopular')
def tvingPopular():

    query = """SELECT d.*
                FROM (SELECT c.*, rownum as rm
                    FROM (SELECT b.*
                            FROM (SELECT m.movie_Id, count(v.movie_id) as view_count
                                FROM movie m, view_count v
                                WHERE m.movie_id = v.movie_id(+) AND m.provider LIKE '%티빙%'
                                GROUP BY m.movie_id
                                ORDER BY view_count DESC)a, movie b
                            WHERE a.movie_id = b.movie_id)c)d
                WHERE rm <= 5"""
    df = pd.read_sql_query(query,conn)
    df.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings', 'provider', 'kinoRating', 'rottenRating', 'imdbRating', 'staff', 'tags', 'releaseDate', 'category', 'rm']
    json_data = df.to_json(orient='records', force_ascii=False)

    return jsonify(json_data)

@app.route('/movieList/wavePopular')
def wavePopular():

    query = """SELECT d.*
                FROM (SELECT c.*, rownum as rm
                    FROM (SELECT b.*
                            FROM (SELECT m.movie_Id, count(v.movie_id) as view_count
                                FROM movie m, view_count v
                                WHERE m.movie_id = v.movie_id(+) AND m.provider LIKE '%웨이브%'
                                GROUP BY m.movie_id
                                ORDER BY view_count DESC)a, movie b
                            WHERE a.movie_id = b.movie_id)c)d
                WHERE rm <= 5"""
    df = pd.read_sql_query(query,conn)
    df.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings', 'provider', 'kinoRating', 'rottenRating', 'imdbRating', 'staff', 'tags', 'releaseDate', 'category', 'rm']
    json_data = df.to_json(orient='records', force_ascii=False)

    return jsonify(json_data)

@app.route('/movieList/mbtiPopular')
def mbtiPopular():

    # mbti = request.args.get_json('mbti')
    mbti = request.args.get('mbti')
    print('mbti: ' + mbti)

    mbti_query = f"""SELECT m.*
                FROM (
                SELECT v.movie_id, COUNT(v.viewcount_id) AS view_cnt
                FROM omz_client c
                JOIN view_count v ON c.client_id = v.client_id
                WHERE c.mbti = '{mbti}'
                GROUP BY v.movie_id
                ORDER BY view_cnt DESC
                ) vc
                JOIN movie m ON vc.movie_id = m.movie_id
                WHERE ROWNUM <= 5"""
    df = pd.read_sql_query(mbti_query,conn)
    df.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings', 'provider', 'kinoRating', 'rottenRating', 'imdbRating', 'staff', 'tags', 'releaseDate', 'category']
    json_data = df.to_json(orient='records', force_ascii=False)

    return jsonify(json_data)


@app.route('/movieList/recommand')
def recommandByCorr():
    clientId = request.args.get('clientId')
    # 여기요
    print('clientId: ' + clientId)

    query_movie = 'select * from movie'
    query_client = 'select * from omz_client'
    query_review = 'select * from review'

    movies = pd.read_sql_query(query_movie, conn)
    movies.columns = ['movieId', 'title', 'movieDescription', 'image', 'poster', 'trailer', 'castings', 'provider', 'kinoRating', 'rottenRating', 'imdbRating', 'staff', 'tags', 'releaseDate', 'category']

    clients = pd.read_sql_query(query_client, conn)
    clients.columns = ['clientId', 'clientPass', 'clientName', 'phone', 'email', 'gender', 'age', 'mbti', 'clientRegDate', 'grade']

    reviews = pd.read_sql_query(query_review, conn)
    reviews.columns = ['reviewId', 'movieId', 'clientId', 'reviewContent', 'rating', 'regDate', 'editDate']

    data = movies.merge(reviews).merge(clients)
    
    recommendation_data = data[['movieId', 'clientId', 'rating']]
    recommendation_pivot = recommendation_data.pivot(index='clientId', columns='movieId', values='rating')
    recommendation_pivot.fillna(0, inplace=True)

    reverse_data = recommendation_pivot.T.iloc[:, :]
    corr_data = reverse_data.corr()

    def movie_seen(clientId):
        return recommendation_pivot.loc[clientId][recommendation_pivot.loc[clientId]>0]

    def similar_user(clientId, n):
        return corr_data.loc[clientId].sort_values(ascending=False)[1:n+1]
    
    # 'clientId'와 영화 성향이 비슷한 회원 n 명을 추려서 그 회원들이 재밌게 본 영화들 중에 아직 'clientId'가 아직 보지 않은 영화 추천 
    def recommand_movie(clientId, n):
        user_list = similar_user(clientId, n).index
        user_mv_list = recommendation_data[(recommendation_data.clientId.isin(user_list)) & (recommendation_data.rating>=4)]
        recommendee_mv_list = movie_seen(clientId)
        unseen_id_list = set(user_mv_list['movieId']) - set(recommendee_mv_list.index)
        return movies[movies['movieId'].isin(unseen_id_list)].reset_index(drop=True)
    
    recommandList = recommand_movie(clientId, 2)

    # return 'what the hell...?'
    return recommandList.to_json(orient='records', force_ascii=False)

if __name__ == '__main__':  
   app.run('127.0.0.1',port=5000,debug=True)


