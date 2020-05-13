import os
os.system("sudo docker network create --subnet=172.27.0.0/16 --gateway=172.27.0.1 mynet")
os.system("sudo docker volume create data")
os.system("sudo docker pull rabbitmq:3.8.3-alpine")
os.system("sudo docker pull zookeeper:latest")
os.system("sudo docker run --network=mynet --name=rmq rabbitmq:3.8.3-alpine")#IT SHOULD BE ASSIGNED TO HAVE IP AS 172.27.0.2
os.system("sudo docker run --network=mynet --name=zoo zookeeper:latest")#IT SHOULD BE ASSIGNED TO HAVE IP AS 172.27.0.3 . chck by doing docker inspect mynet
