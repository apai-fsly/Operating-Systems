1. How to run the program	
Running Test case 1:
	python3 main.py test_case1

This will create a network size of 7, out of which 5 of them are sellers and two buyers. This demonstrates an election algorithm, leader falling sick occasionally, reelection to elect new leader and two buyers buying two different products. A seller is capable of restocking a random product from the choice to if it runs out of stock.

Running Test Case 2:
	python3 main.py test_case2

This will create a network size of 5, out of which 4 of them are sellers and one buyer. This demonstrates an election algorithm, a leader falling sick occasionally, reelection to elect a new leader and one buyer sending 1000 requests to  buy a product. Peer 3 gets elected as first leader, as the Peer 4 is assigned sick when setting up the network.

Running Test Case 3:
	python3 main.py test_case3
This will create a network size of 7, out of which 5 of them are sellers and two buyers. This demonstrates an election algorithm, leader falling sick occasionally, reelection to elect new leader and two buyers buying two same products. Concurrent buy requests are resolved using multicast Lamport's clock.

Normal Run, to simulate everything:
	python3 main.py normal
Normal Run, to simulate everything with N given peers: python3 main.py normal <N>

Example: python3 main.py normal 10
This will create a 10 peers network and assign randomly buyers and sellers. Selects a random peer and starts an election using Bully Algorithm to elect a leader. This demonstrates an election, the leader falling sick occasionally, reelection to elect a new leader and buyers buying their respective products. Concurrent buy requests are resolved using multicast Lamport’s clock. A seller is capable of restocking a random product from the choice to if it runs out of stock.