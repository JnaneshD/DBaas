#!/usr/bin/env python
import pika
import docker
import time
import requests
from threading import Thread
import uuid
import json
import sqlite3
import os
from flask import Flask,jsonify,abort,request
app = Flask(__name__)
from kazoo.client import KazooClient
from kazoo.client import KazooState
zk = KazooClient(hosts='172.27.0.3:2181')
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
zk.start()
global count
count = 0



def spawn(master_id):
	cc = client.containers.run('slaves_slave', ["python", "slave.py"],network="mynet",detach=True,volumes={"/var/run/docker.sock":{"bind":"/var/run/docker.sock","mode":"rw"},"data":{"bind":"/slave/db","mode":"rw"}})
	time.sleep(3)
	master_cont = client.containers.get(master_id)
	master_cont.exec_run("cp assign.db db/")
	cc.exec_run("cp db/assign.db .")
	time.sleep(3)


def stop_c(parm):#A function to scale down
	print("Inside the stopping container")
	inp = int(math.ceil(parm)/20)
	slaves = {}
	children = zk.get_children("/slaves", watch=demo_func)
	for i in children:
		data,stat = zk.get("/slaves/"+i)
		if(data.decode("utf-8")=="slave"):
			cont_id = i.split("_")[-1]
			container_master =  client.containers.get(cont_id)
			pid = container_master.attrs['State']['Pid']
			slaves[cont_id]=pid
	remov = len(slaves) - inp#Number of slaves we need to stop 
	print("Len of slaves is ",len(slaves))
	for i in range(remov):
		if(len(slaves)>1):	
			l = sorted(slaves.items() ,key = lambda kv:(kv[1],kv[0]))
			lowest_contid = l[-1][0]
			zk.delete("/slaves/node_"+str(lowest_contid))
			container =  client.containers.get(str(lowest_contid))
			container.stop()
			time.sleep(1)
			del slaves[lowest_contid]
	return "Ok"

def get_master():
	children = zk.get_children("/slaves", watch=demo_func)
	for i in children:
		data,stat = zk.get("/slaves/"+i)
		if(data.decode("utf-8")=="master"):
			master_id= i.split("_")[-1]
			return master_id


def run_c(master_id,parm):# A function to start the containers
		inp = int(math.ceil(parm/20))
		slaves = {}
		children = zk.get_children("/slaves", watch=demo_func)
		for i in children:
			data,stat = zk.get("/slaves/"+i)
			if(data.decode("utf-8")=="slave"):
				cont_id = i.split("_")[-1]
				container_master =  client.containers.get(cont_id)
				pid = container_master.attrs['State']['Pid']
				slaves[cont_id]=pid
		remov = inp - len(slaves)#Number os containers which needs to be started (requests/20 - slaves running)
		print("Len of slaves is ",len(slaves))
		for i in range(remov):
			spawn(get_master())#New container is spawned
			print("inside spawning the container")

class scale(Thread):#Auto scaling thread which is started only after the first request or db/read comes
	def __init__(self):
		Thread.__init__(self)
	def run(self):
		print("Inside the scale thread")
		counter=0
		while(1):
			r = requests.get("http://localhost/get_count")#a method to get the requests
			start = r.text
			ss=math.ceil(int(start)/20)
			time.sleep(120)#We will wait for two minutes
			r = requests.get("http://localhost/get_count")#Again check the count
			stop = r.text
			print(str(stop))
			st=math.ceil(int(stop)/20)
			check = int(st)-int(ss)#Difference
			if(check):
				run_c(get_master(),int(stop))#if we need to scale up
				print("2")
			else:
				stop_c(int(stop))#if we need to scale down
			r = requests.get("http://localhost/upd")	


class RpcClient(object):# Main RPC client to initialize the connection and communication 
    def __init__(self,call_back_queue=''):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='172.27.0.2'))#Rabbitmq should be running on this ip address
        self.channel = self.connection.channel()
        self.call_back_queue_name = call_back_queue
        result = self.channel.queue_declare(queue=self.call_back_queue_name)
        print("running")
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body.decode())

    def call(self,queue_name,qry):#Function will get the message and queuename
        self.response = None
        self.corr_id = str(uuid.uuid4())#We are using uuid for correlation id
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id),
	            body=qry)#We will put the json of the query inside the rabbitmq queue
        while self.response is None:
            self.connection.process_data_events()
        requeued_messages = self.channel.cancel()
        self.connection.close()
        return self.response

@app.route("/get_count")
def hellobee():
    global count
    return str(count)

@app.route("/upd")
def update():#resetting the counter
    global count
    count = 0
    return "{}"

global st 
st = 1
@app.route("/api/v1/db/read",methods=["GET","POST"])
def hello():
    global count
    count = count + 1#Count is incremented
    global st
    if(st):
        st = 0
    queue = 'readQ'
    param = request.get_json()
    query=json.dumps(param)
    read_obj = RpcClient('responseQ')#An object is created of RPC Client .. ResponseQ is the callback queue
    response = read_obj.call(queue,query)#Putting the json message of the query to the readQ . We will get the response
    print(response)
    if len(response) > 0:
         return response,200
    else:
         return response,200 

@app.route("/api/v1/db/write",methods=["GET","POST"])
def write():
    if request.method == "POST":
        queue_name = 'writeQ'
        json_obj = request.get_json()
        qry = json.dumps(json_obj)
        read_rpc = RpcClient('responseQ')#An rpc client object is created
        response = read_rpc.call(queue_name,qry)#json is sent to the writeQ
        print('response from worker {}'.format(response))
        if len(response) > 0:
            return response,200
        else:
            return response,200 
@app.route("/api/v1/crash/master",methods=["POST"])
def crash_master():
    if request.method=="POST":
        children = zk.get_children("/slaves")
        client = docker.from_env()
        pids={}
        for i in children:
            data,stat = zk.get("/slaves/"+i)
            cont_id = i.split("_")[-1]
            container_master =  client.containers.get(cont_id)
            if(data.decode("utf-8")=="master"):
                container_master.stop()#A master container is stopped
                ret_id = container_master.attrs['State']['Pid']#returning the pid of the stopped container
        return jsonify(ret_id)
    else:
        return {},404
@app.route("/api/v1/crash/slave",methods=["POST"])
def crash_slave():
    if request.method == "POST":
        pids={}
        children = zk.get_children("/slaves")
        client = docker.from_env()
        for i in children:
            data,stat = zk.get("/slaves/"+i)
            cont_id = i.split("_")[-1]
            container_master =  client.containers.get(cont_id)
            pid = container_master.attrs['State']['Pid']
            if(data.decode("utf-8")=="slave"):
                pids[cont_id]=pid
        print(pids)
        l = sorted(pids.items() ,key = lambda kv:(kv[1],kv[0]))#sorting the number of slaves
        if(len(l)>0):#Atleast one container should be running
            lowest_contid = l[-1][0]
            lowest_pid = l[-1][1]
            container_master =  client.containers.get(str(lowest_contid))
            container_master.stop()#A container with the highest pid is stopped
        return jsonify(lowest_pid)
    else:
        return {},404

@app.route("/api/v1/worker/list",methods=["GET"])
def get_workers():
    if request.method == "GET":
        pids={}
        ret = []
        client = docker.from_env()
        children = zk.get_children("/slaves")
        for i in children:
            data,stat = zk.get("/slaves/"+i)
            cont_id = i.split("_")[-1]
            container_master =  client.containers.get(cont_id)
            pid = container_master.attrs['State']['Pid']# PID of the containers which are running
            pids[cont_id]=data
            ret.append(pid)
        print(pids)
        return jsonify(sorted(ret))#sorted array of pids
    else:
        return {},404


if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True)
