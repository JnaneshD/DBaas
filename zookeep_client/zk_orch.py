import logging
import time
from kazoo.client import KazooClient
from kazoo.client import KazooState
import docker
import atexit
import string 
import datetime
import requests
import math
import time
client = docker.DockerClient(base_url='unix://var/run/docker.sock')#We are mapping the host's docker socket into the container's socket using volumes
import os

from threading import Thread
from subprocess import Popen,PIPE


logging.basicConfig()



def spawn(master_id):
	cc = client.containers.run('slaves_slave', ["python", "slave.py"],network="mynet",detach=True,volumes={"/var/run/docker.sock":{"bind":"/var/run/docker.sock","mode":"rw"},"data":{"bind":"/slave/db","mode":"rw"}})
	time.sleep(3)#New slave copying data from the master.Since we are using sqlite3 it is very lightweight and copying isn't that difficult
	master_cont = client.containers.get(master_id)
	master_cont.exec_run("cp assign.db db/")#executing cp command inside a running master.
	#db is a folder which is shared by all the slaves using docker volume called 'data' ($docker volume create data)
	cc.exec_run("cp db/assign.db .")#slave container is copying the data which was just put by the master container for consistency
	time.sleep(3)


total_child = 0
zk = KazooClient(hosts='172.27.0.3:2181')#This is fixed as we didn't use docker links . We created our own docker network and all containers are running on that network
zk.start()	
zk.ensure_path("/slaves")
def get_master():#Getting container id of master. This is used in copying the database from a master to new slave
	children = zk.get_children("/slaves", watch=demo_func)
	for i in children:
		data,stat = zk.get("/slaves/"+i)
		if(data.decode("utf-8")=="master"):
			master_id= i.split("_")[-1]
			return master_id

@zk.ChildrenWatch("/slaves")
def demo_func(event):#We are not using this part ..It was just to debug
    print(event)
    children = zk.get_children("/slaves",include_data=False)
    print("There are %s children with names %s" % (len(children), children))

x=1
thrd = 1
master_id=""
while(x):
	children = zk.get_children("/slaves") #Every master or slave is attached to this path only
	if(len(children)==0):
		cc = client.containers.run('slaves_slave', ["python", "slave.py"],network="mynet",detach=True,volumes={"/var/run/docker.sock":{"bind":"/var/run/docker.sock","mode":"rw"},"data":{"bind":"/slave/db","mode":"rw"}})
		time.sleep(5)
		cc = client.containers.run('slaves_slave', ["python", "slave.py"],network="mynet",detach=True,volumes={"/var/run/docker.sock":{"bind":"/var/run/docker.sock","mode":"rw"},"data":{"bind":"/slave/db","mode":"rw"}})
		masters=[]

	else:

		try:
			masters=[]#Just to ensure a master is running
			slaves = {}#A dictionary with the key as docker id and data as pid used for leader election 
			children = zk.get_children("/slaves", watch=demo_func)
			for i in children:
				data,stat = zk.get("/slaves/"+i)
				print(data.decode("utf-8"))
				if(data.decode("utf-8")=="master"):
					master_id= i.split("_")[-1]
					masters.append(master_id)
				else:
					cont_id = i.split("_")[-1]
					container_master =  client.containers.get(cont_id)
					if(container_master.status=="exited"):#our znodes are persistent so we will have it even if the container is exited and then we can check on that
						zk.delete("/slaves/"+str(i))#Deleting the znode
						spawn(get_master())#This is the fault tolerance part of the slave . 
					else:
						pid = container_master.attrs['State']['Pid']
						slaves[cont_id]=pid
			#after the for loop we will get to know number of slaves and whether a master if running
			if(len(masters)>0):#If there is a master 
				mast = masters[0]
				master_cont = client.containers.get(mast)
				if(master_cont.status=="exited"):#If the master container is not running(fault tolerance of master)
					print("\nMaster is not running ")
					zk.delete("/slaves/node_"+str(mast))
					l = sorted(slaves.items() ,key = lambda kv:(kv[1],kv[0]))#sorting the slaves based on their pid .
					lowest_pid = l[0][1]
					lowest_contid = l[0][0]
					zk.set("/slaves/node_"+str(lowest_contid),b"master")#Leader election = A slave with the lowest pid will become master .
					#znode with "master" is the logic we are using in the slave container to run a thread with write process listening to writeQ and it stops listening in reaQ and syncQ(fanout) 
					master_id=lowest_contid
					spawn(get_master())#A new slave container will be started
					print("\n A master was elected among the slaves with smallest pid\n")
				else:
					print("\nA master is running ")

			else:#If there are no masters
				if(len(slaves)>1):#making sure that always atleast one slave should be running
					l = sorted(slaves.items() ,key = lambda kv:(kv[1],kv[0]))#Again leader election method
					lowest_pid = l[0][1]
					lowest_contid = l[0][0]
					zk.set("/slaves/node_"+str(lowest_contid),b"master")#Elected slave with lowest pid as master
					print("\n A master was elected among the slaves with smallest pid\n")
				else:#We need atleast one slave so start a slave container
					spawn(get_master())

		except:
			pass