from flask import *
import csv
import re
import string
#from flask_api import status  
import requests
import sqlite3
import json
import datetime  
app = Flask(__name__)
global index_add_counter
index_add_counter = 0
def db(i):
	if i==0:
		con = sqlite3.connect("assign.db")
		con.execute("create table User (uid INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, password TEXT NOT NULL)")
		print("Table created successfully")
		con.close()

#1st api
@app.route("/api/v1/users",methods=["PUT"])
def inex():
	global index_add_counter
	index_add_counter=index_add_counter+1

	user=request.get_json()["username"]
	pas=request.get_json()["password"]
	pas=pas.lower()
	params1 = {
		"table":"User",
		"columns":"username",
		"where":""
	}
	if(user==""):
		return make_response("Usename cant be empty",400,{"Content-type":"application/json"})
	r = requests.post("http://localhost/api/v1/db/read",json=params1)  
	try:
		r = requests.post("http://localhost/api/v1/db/read",json=params1)  
	except:
		return make_response("Service not available",500,{"Content-type":"application/json"})	
	res = json.loads(r.text)
	for key,val in res.items():
		if(val.replace(",","")==str(user)):
			return make_response("Usename already exist",400,{"Content-type":"application/json"})
	params = {
		"insert":str(user)+","+str(pas),
		"column":"username,password",
		"table":"User"
	}
	if(len(pas)!=40):
		return make_response("Password not in correct format",400,{"Content-type":"application/json"})
	if(all(c in string.hexdigits for c in pas)==False):
		return make_response("Password not in correct format",400,{"Content-type":"application/json"})
	try:
		r = requests.post("http://localhost/api/v1/db/write",json=params)
	except:
		return make_response("Service not available",500,{"Content-type":"application/json"})
	return make_response("{}",201,{"Content-type":"application/json"})

#2nd api
@app.route("/api/v1/users/<usernam>",methods=["DELETE"])
def delete(usernam): 

	global index_add_counter
	index_add_counter=index_add_counter+1

	with sqlite3.connect("assign.db") as con:  
		cur = con.cursor()
		params1 = {
		"table":"User",
		"columns":"username",
		"where_part":"1",
		"where":"username=",
		"condition":usernam
	}
		r = requests.post("http://localhost/api/v1/db/read",json=params1)  
		user_name_enter=(r.text).split("\n")
		res = json.loads(r.text)
		flag=0
		for k,v in res.items():
			print(v)
			if(v==str(usernam)):
				flag=1
		if(flag==0):
			return make_response("Usename not exist",405,{"Content-type":"application/json"})
		parm = {
		"table":"Ride",
		"columns":"*",
		"where_part":"1",
		"condition":usernam,
		"where":"user1="
		}
		param = {
		"table":"Ride",
		"columns":"*",
		"where":""
		}
		params = {
					"delete":"1",
					"where":usernam,
					"table":"User"
				}
		r = requests.post("http://localhost/api/v1/db/write",json=params)
		return make_response("{}",200,{"Content-type":"application/json"})

#extra api for Ride table view
@app.route("/api/v1/users",methods=["GET"])  
def view1():
	global index_add_counter
	index_add_counter=index_add_counter+1
	params = {
		"table":"User",
		"columns":"username",
		"where":"NULL"
	}
	r = requests.post("http://localhost/api/v1/db/read",json=params)  
	output = json.loads(r.text)
	l = []
	for k,v in output.items():
		l.append(v)
	return jsonify(l)


@app.route("/api/v1/_count",methods=["GET"])
def count():
	global index_add_counter
	return make_response(str(index_add_counter),200,{"Content-type":"application/json"})

@app.route("/api/v1/_count",methods=["DELETE"])
def count1():
	global index_add_counter
	index_add_counter=0
	return {}

i=0
if __name__ == "__main__":
	try:
		db(i)
	except Exception as e:
		pass
	#app.run(debug = True)
	app.run(host="0.0.0.0",port=5000,debug=True)
