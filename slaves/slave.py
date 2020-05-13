#!/usr/bin/env python
import csv
import re
import string
import pika
import atexit
import sqlite3
import json
import threading
import time
import datetime
from threading import Thread
import os
import socket
doc_name = socket.gethostname()#docker id
from kazoo.client import KazooClient
from kazoo.client import KazooState
zk = KazooClient(hosts='172.27.0.3:2181')
zk.start()




zk.ensure_path("/slaves")
def db():
    con = sqlite3.connect("assign.db")
    con.execute("create table if not exists Ride (ride_id INTEGER PRIMARY KEY AUTOINCREMENT, ride_s TEXT NOT NULL,ride_s_enum INTEGER , ride_d TEXT NOT NULL,ride_d_enum INTEGER,timestamp TEXT NOT NULL,user1 TEXT NOT NULL,user2 TEXT,user3 TEXT,user4 TEXT)")
    con.execute("create table if not exists User (uid INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, password TEXT NOT NULL)")
    print("Table created successfully")
    con.close()


def write_to_db(input):#includes write update and delete operations
	#cur.execute("INSERT into User (username, password) values (?,?)",(user,pas)) 
	delete = "NULL"
	data = "NULL"
	column1 = "NULL"
	table = "NULL"
	WHERE = "NULL"
	update = "NULL"
	SET = "NULL"
	user = "NULL"
	try:#if the instruction is an update 
		update = input["update"]
		table = input["table"]
		SET = input["set"]
		user = input["user"]
		WHERE = input["where"]
	except:
		pass
	try:#if the instruction is a delete
		delete = input["delete"]
		WHERE = input["where"]
		table = input["table"]
	except:
		pass
	try:#if the instruction is a insert
		data = input["insert"]
		column1 = input["column"]
		table = input["table"]
	except:
		pass
	if(update!="NULL"):
		sql = "UPDATE "+table+" SET "+SET+" ? WHERE "+WHERE
		with sqlite3.connect("assign.db") as con:
				cur = con.cursor()
				cur.execute(sql,(user,))
				con.commit()
				return "update over"
	if(delete!="NULL"):
		if(WHERE=="NULL"):
			if(table=="alltable"):
				sql = "DELETE from User"
				with sqlite3.connect("assign.db") as con:
					cur = con.cursor()
					cur.execute(sql)
					sql = "DELETE from Ride"
					cur.execute(sql)
					con.commit()
					return {}
		if(table=="User"):
			sql = "DELETE from User WHERE username=?"
			with sqlite3.connect("assign.db") as con:
				cur = con.cursor()
				cur.execute(sql,(WHERE,))
				con.commit()
				return "{}"
		if(table=="Ride"):
			sql = "DELETE from Ride WHERE ride_id=?"
			with sqlite3.connect("assign.db") as con:
				cur = con.cursor()
				cur.execute(sql,(WHERE,))
				con.commit()
				return "{}"
	else:
		with sqlite3.connect("assign.db") as con:
			data1 = data.split(",")
			if(table=="Ride"):
				sql = "INSERT into "+str(table)+" ("+str(column1)+") values (?,?,?,?,?,?,?,?,?)"
			if(table=="User"):
				sql = "INSERT into "+str(table)+" ("+str(column1)+") values (?,?)"
			cur = con.cursor()
			cur.execute(sql,([d for d in data1]))
			con.commit()
			return "{}"
#api number 9 complete
def read_db(input):#Read databse funtion 
	a={}
	count=0
	print(input)
	where_part = "NULL"
	condition = "NULL"
	where = "NULL"
	try:
		where_part = input["where_part"]
		condition = input["condition"]
		table = input["table"]
		columns =input["columns"]
		where = input["where"]
	except:
		pass
	if(where_part!="NULL"):
		sql_query = "SELECT "+columns+" from "+table+" WHERE "+where+" ?"
		with sqlite3.connect("assign.db") as con:
			cur = con.cursor()
			cur.execute(sql_query,(condition,))
			rows = cur.fetchall()
			l=[]
			s=''
			for i in rows:
				for col in i:
					s=s+str(col)+','
				a[count]=s
				s=''
				count+=1
			return a
	else:
		table = input["table"]
		columns =input["columns"]
		where = input["where"]
		sql_query = "SELECT "+columns+" from "+table+" "+where
		print(sql_query)
		with sqlite3.connect("assign.db") as con:
			con.row_factory = sqlite3.Row
			cur = con.cursor()
			cur.execute(sql_query)
			rows = cur.fetchall()
			s= ""
			l=[]
			for i in rows:
				for col in i:
					s=s+str(col)+','
				a[count]=s
				s=''
				count+=1
			return a



class read(Thread):
	def __init__(self, *args, **kwargs): 
		super(read, self).__init__(*args, **kwargs)
		self.running = True 
		self.connection=pika.BlockingConnection(
                  pika.ConnectionParameters(host='172.27.0.2'))# Always connecting to this ip address
	def stop(self):
		self.running = False
	def run(self):
		channel = self.connection.channel()
		channel.queue_declare(queue='responseQ')
		channel.queue_declare(queue='readQ')
		channel.basic_qos(prefetch_count=1)
		print("In the read queue ")
		for method_frame,properties,body in channel.consume('readQ'):
			if(self.running):
				query = json.loads(body.decode())# loading the text into json
				response = read_db(query)#Reading the database
				print('The response from the readDb function is {}'.format(response))
				print(response)
				channel.basic_publish(exchange='',
								 routing_key='responseQ',
								 properties=pika.BasicProperties(correlation_id = \
			                                         properties.correlation_id),
								 body=json.dumps(response))	
				channel.basic_ack(method_frame.delivery_tag)
				print("data has been sent")
			else:
				requested_msg = channel.cancel()
				self.connection.close()
class writeprc(Thread):
	def __init__(self):
		Thread.__init__(self)
		self.connection=pika.BlockingConnection(
                  pika.ConnectionParameters(host='172.27.0.2'))
		self._running = True
	def run(self):
		while self._running:
			channel = self.connection.channel()
			channel.queue_declare(queue='writeQ')
			channel.basic_qos(prefetch_count=1)
			print("in the write queue")
			for method_frame,properties,body in channel.consume('writeQ'):
				qry_str = json.loads(body.decode())
				response = write_to_db(qry_str)
				print('The response from the write function is {}'.format(response))
				channel.basic_publish(exchange='',
								 routing_key='responseQ',
								 properties=pika.BasicProperties(correlation_id = \
		                                                 properties.correlation_id),
								 body=json.dumps(response))	
				channel.basic_ack(method_frame.delivery_tag)
				channel.exchange_declare(exchange='logs', exchange_type='fanout')
				channel.basic_publish(exchange='logs', routing_key='', body=body.decode())
				print("data has been sent")
			requested_msg = channel.cancel()
			self.connection.close()	
class consistency(Thread):
	def __init__(self, *args, **kwargs): 
		super(consistency, self).__init__(*args, **kwargs) 
		self.connection=pika.BlockingConnection(
                  pika.ConnectionParameters(host='172.27.0.2'))
		self.running = True
	def stop(self):
		self.running = False#Flag used 
	def run(self):
		channel = self.connection.channel()
		result = channel.queue_declare(queue='', exclusive=True)
		channel.exchange_declare(exchange='logs', exchange_type='fanout')
		queue_name = result.method.queue
		channel.queue_bind(exchange='logs', queue=queue_name)
		channel.basic_consume(queue=queue_name,on_message_callback=self.on_response,auto_ack=True)
		channel.start_consuming()
		print(self._running)
	def on_response(self, ch, method, props, body):
		if(self.running):
			qry_str = json.loads(body.decode())
			response = write_to_db(qry_str)
			print("The response from the write db for the syncQ request {}".format(response))

if __name__=="__main__":
	db()
	zk.create("/slaves/node_"+str(doc_name), b'slave')#A znode is created with the docker id in the name and slave as data
	write = 1
	t1=read()#Read thread which is listening at readQ
	t3=writeprc()#Write thread which is listening at writeQ
	t2=consistency()# Fanout used to sync data from master
	t1.start()
	t2.start()
	while(True):
		data, stat = zk.get("/slaves/node_"+str(doc_name))
		if(data.decode("utf-8")=="master"):#This container is selected as a master
			if(write):
				t3.start()#Write thread is started 
				write=0
				t1.stop()#these two threads will be running but they wont listen anymore in readQ or fanout
				t2.stop()