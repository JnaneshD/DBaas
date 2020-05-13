#DBAAS
<h1>Final CC project repository </h1>
<h3>High available scalable and fault tolerant database as a service </h3>
<ul>
	Dependencies
	<li>docker</li>
	<li>docker-compose</li>
</ul
<br>
<h2>Instructions</h2>
<h3>To run the CC Final project these are the following steps . </h3>
<h3>Initial setup</h3>
		I made a setup file you can run it instead of these commands
		- we need to have a network called mynet
		$sudo docker network create --subnet=172.27.0.0/16 --gateway=172.27.0.1 mynet
		For mac os some additional setup needs to be done about network

		- I used docker volumes used for scaling
		$sudo docker volume create data

		- Pull the rabbitmq:3.8.3-alpine and zookeeper images
		$sudo docker pull rabbitmq:3.8.3-alpine
		$sudo docker pull zookeeper:latest

		//Run the rabbitmq on the docker frst so that it gets 172.27.0.2 as IP	
		$sudo docker run --network=mynet --name=rmq rabbitmq:3.8.3-alpine
	
		//Run the zookeeper on docker so that it gets 172.27.0.3 as IP
		$sudo docker run --network=mynet --name=zoo zookeeper:latest

<h2>These commands must be executed to run the project</h2>

	** Build the slave image . Go to the slaves folder and type .. Name of the image should be exactly same
	$sudo docker build -t slaves_slave .

	** After that you need to run the zookeep(a container for fault tolerance)
	Go inside the zookeep_client folder
	$sudo docker-compose up --build -d

	** After that you need to start the orchestrator
	Go inside the orchs_new folder
	$sudo docker-compose up --build -d

	To run the users and rides same concept (It works even without the docker just python app(inside rides) or python users(inside users))
	Check the ports of users and rides before sending the requests
	These are the steps to run our project
	

	
