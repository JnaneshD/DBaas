from flask import *
import csv
import re
import string
import requests 
import sqlite3
import json
import datetime  
app = Flask(__name__)
global index_add_counter
index_add_counter = 0


#3rd api
@app.route("/api/v1/rides",methods=["POST"])
def ride():
	global index_add_counter
	index_add_counter=index_add_counter+1
	created_by=request.get_json()["created_by"]
	timestamp=request.get_json()["timestamp"]
	source_enum=request.get_json()["source"]
	destination_enum=request.get_json()["destination"]
	source="NULL";
	destination="NULL";
	if(created_by=="" or timestamp=="" or source_enum=="" or destination_enum==""):
		return make_response("Parameters cant be empty",400,{"Content-type":"application/json"})	
	if(source_enum==destination_enum):
		return make_response("Source and destination cannot be same",405,{"Content-type":"application/json"})
	p = re.findall(r'^([1-9]|([012][0-9])|(3[01]))-([0]{0,1}[1-9]|1[012])-\d\d\d\d:[0-5][0-9]-[0-5][0-9]-[012]{0,1}[0-9]$',timestamp)
	if(len(p)==0):
		return make_response("Timestamp format error",400,{"Content-type":"application/json"})
	f = open('AreaNameEnum.csv')
	csv_f = csv.reader(f)
	for row in csv_f:
		if(row[0]==source_enum):
			source=row[1]
		elif(row[0]==destination_enum):
			destination=row[1]
	params = {
		"table":"User",
		"columns":"*",
		"where_part":"1",
		"where":"username=",
		"condition":created_by
	}



	r = requests.post("http://localhost/api/v1/db/read",json=params)
	print(json.loads(r.text))
	row = json.loads(r.text)
	if(len(row)>0):
		params = {
			"insert":str(source)+","+str(source_enum)+","+str(destination)+","+str(destination_enum)+","+timestamp+","+created_by+","+"NULL,NULL,NULL",
			"column":"ride_s, ride_s_enum,ride_d,ride_d_enum,timestamp,user1,user2,user3,user4",
			"table":"Ride"
		}
		if(source=="NULL"or destination=="NULL"):
			return make_response("ENUM NOT EXISTS",400,{"Content-type":"application/json"})
		r = requests.post("http://localhost/api/v1/db/write",json=params)
		return make_response("{}",201,{"Content-type":"application/json"})
	else:
		return make_response("User does not exists",400,{"Content-type":"application/json"})

#4th api to list all the rides given source and destination enum
@app.route("/api/v1/rides",methods=["GET"])
def listrides():
	global index_add_counter
	index_add_counter=index_add_counter+1
	source = request.args.get('source')
	destination = request.args.get('destination')
	if(source=="" or destination==""):
		return make_response("Parameters cant be empty",400,{"Content-type":"application/json"})
	params = {
		"table":"Ride",
		"columns":"*",
		"where":"where ride_s_enum="+source+" AND ride_d_enum="+destination
	}
	r = requests.post("http://localhost/api/v1/db/read",json=params)
	l = json.loads(r.text)
	ret = []
	for k,row in l.items():
		ro = row.split(",")
		print(len(ro))
		d = {}
		if(len(ro)>5):
			d["rideId"]=ro[0]
			d["username"]=ro[6]
			d["timestamp"]=ro[5]
			timestamp = ro[5]
			time = timestamp.split(":")[1]
			date = timestamp.split(":")[0]
			tim_arr = time.split("-")
			tim = ""
			print(ro[0])
			tim = tim+tim_arr[2]+":"+tim_arr[1]+":"+tim_arr[0]
			dat_arr = date.split("-")
			dat = dat_arr[2]+"-"+dat_arr[1]+"-"+dat_arr[0]
			datetim = dat+" "+tim
			date_time =datetime.datetime.strptime(datetim,"%Y-%m-%d %H:%M:%S")
			present = datetime.datetime.now()
			dif = present - date_time
			print(present)
			print(date_time)
			if(present>date_time):
				pass
			else:
				ret.append(d)
	if(len(l)==0):
		return make_response("Rides not exists",204,{"Content-type":"application/json"})
	return jsonify(ret)

#5 and 6 api
@app.route("/api/v1/rides/<rideId>",methods=["POST","GET","DELETE"])
def join(rideId):
	global index_add_counter
	index_add_counter=index_add_counter+1
	if(request.method=="POST"):#Join to a ride complete
		user = request.get_json()["username"]
		params = {
		"table":"User",
		"columns":"*",
		"where_part":"1",
		"where":"username=",
		"condition":user
		}
		r = requests.post("http://localhost/api/v1/db/read",json=params)
		row = json.loads(r.text)
		if(len(row)>0):
			params = {
			"table":"Ride",
			"columns":"user1,user2,user3,user4",
			"where_part":"1",
			"where":"ride_id=",
			"condition":rideId
			}
			r = requests.post("http://localhost/api/v1/db/read",json=params)
			rows = json.loads(r.text)
			print(len(rows))
			if(len(rows)>0):
				rows = rows.get("0").split(",")
				user1=rows[0]
				user2=rows[1]
				user3=rows[2]
				user4=rows[3]
				if(user==user1 or user==user2 or user==user3 or user4==user):
					return make_response("User already in the ride",400,{"Content-type":"application/json"})
				else:
					if(user2=="NULL"):
						params ={
							"update":"1",
							"set":"user2=",

							"user":user,
							"where":"ride_id="+rideId,
							"table":"Ride"
						}
						r = requests.post("http://localhost/api/v1/db/write",json=params)
						return make_response("{}",201,{"Content-type":"application/json"})
					elif(user3=="NULL"):
						params ={
							"update":"1",
							"set":"user3=",
							"user":user,
							"where":"ride_id="+rideId,
							"table":"Ride"
						}
						r = requests.post("http://localhost/api/v1/db/write",json=params)
						return make_response("{}",201,{"Content-type":"application/json"})
					elif(user4=="NULL"):
						params ={
							"update":"1",
							"set":"user4=",
							"user":user,
							"where":"ride_id="+rideId,
							"table":"Ride"
						}
						r = requests.post("http://localhost/api/v1/db/write",json=params)
						#print("successfully joined the ride")
						return make_response("{}",201,{"Content-type":"application/json"})
					else:
						params ={
							"update":"1",
							"set":"user4=",
							"user":user4+","+user,
							"where":"ride_id="+rideId,
							"table":"Ride"
						}
						r = requests.post("http://localhost/api/v1/db/write",json=params)
						print("successfully joined the ride")
						return make_response("{}",201,{"Content-type":"application/json"})
						#return make_response("CAPACITY LIMIT REACHED",401,{"Content-type":"application/json"})

		else:
			return make_response("User not exists",400,{"Content-type":"application/json"})
	if(request.method=="GET"):#details of a ride
		params = {
			"table":"Ride",
			"columns":"*",
			"where":"where ride_id="+rideId
		}
		r = requests.post("http://localhost/api/v1/db/read",json=params)
		row1 = json.loads(r.text)
		if(len(row1)==0):
			abort(400,"RIDE NOT FOUND")
		f= {}
		row=row1.get("0").split(",")
		print(row)
		f["ride_id"]=row[0]
		f["created_by"]=row[6]
		user2 = row[7]
		user3 = row[8]
		user4 = row[9]
		f["users"]=list()
		for i in range(6,len(row)-1):
			ss=row[i].strip("\n")
			if(ss!="NULL"):
				f["users"].append(ss)
		f["timestamp"] = row[5]
		f["source"] = row [1]
		print(jsonify(f["users"]))
		f["destination"] = row[3]
		return jsonify(f)
	if(request.method=="DELETE"):
		if(rideId==""):
			abort(400,"Ride id not found")
		params1 = {
		"table":"Ride",
		"columns":"ride_id",
		"where":""
			}
		r = requests.post("http://localhost/api/v1/db/read",json=params1)  
		reply = json.loads(r.text)
		if(r.text==""):
			abort(400,"Error in reading")
		user_name_enter=(r.text).split("\n")
		flag=0
		for k,i in reply.items():
			print(i)
			if(i.split(",")[0]==str(rideId)):
				flag=1
		if(flag==0):
			return make_response("Ride not exist",405,{"Content-type":"application/json"})
		params = {
			"delete":"1",
			"table":"Ride",
			"where":rideId
		} 
		r = requests.post("http://localhost/api/v1/db/write",json=params)
		return make_response("{}",200,{"Content-type":"application/json"})


@app.route("/api/v1/_count",methods=["GET"])
def count():
	return make_response(str(index_add_counter),200,{"Content-type":"application/json"})

@app.route("/api/v1/_count",methods=["DELETE"])
def count1():
	global index_add_counter
	index_add_counter = 0
	return {}

@app.route("/api/v1/rides/count",methods=["GET"])
def count2():
	params = {
		"table":"Ride",
		"columns":"*",
		"where":""
	}
	r = requests.post("http://localhost/api/v1/db/read",json=params)  
	total = json.loads(r.text)
	if(len(total)==0):
		return {},204
	else:	
		return jsonify(str(len(total))),200

@app.route("/api/v1/db/clear",methods=["POST"])
def clear():
	params = {
		"delete":"1",
		"table":"alltable",
		"where":"NULL"
	}
	r = requests.post("http://localhost/api/v1/db/write",json=params)  
	return {}
i=0
if __name__ == "__main__":
	app.run(host="0.0.0.0",port=8000,debug=True)
#	app.run()
