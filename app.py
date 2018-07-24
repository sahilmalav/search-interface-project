from flask import Flask, request, render_template, jsonify, abort, redirect
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from math import ceil

app = Flask(__name__)
es_host = "http://localhost:9200"
es_index_name = "sahildb"

es = Elasticsearch(es_host)

@app.route("/")
def home():
    return render_template('index.html', title='Search Engine')

def countQuery(text):
    s = Search(using=es, index=es_index_name).query("match", text=text)
    total = s.count()
    return total

def performQuery(text,number):
    s = Search(using=es, index=es_index_name).query("match", text=text)
    s = s[number:number+20]
    result = s.execute()
    return result.to_dict()

def printResults(number):
    if 'query' not in request.json:
        abort(400)
    query = request.json['query']
    results = performQuery(query,number)
    return results
    # return jsonify({'msg': "success", "data": results})


@app.route('/search', methods=['POST'])
def search_es():
    print(request.json)
    number = 0
    query = request.json['query']
    temp = countQuery(query)
    results = []
    while (number < temp and number < 500): 
        results.append(printResults(number))
        number+=20    
    return jsonify({'msg': "grand success", "data": results})

if __name__ == "__main__":
    app.run(debug=True) 