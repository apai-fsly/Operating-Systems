import datetime
import threading
import multiprocessing
import socket
import logging
import time
import csv
import os
import pandas as pd
import random as rand
import random
from queue import Queue

# Turn this to false if you set Number_of_traders more than 2
FAULT_TOLERANT = True
# shared file among the leaders
# Get and display the current working directory
current_directory = os.getcwd()
print(f"Current working directory: {current_directory}")

# Define the file name in the current directory

# leader_file = "leader.csv"
# leader_path = os.path.join(current_directory, leader_file)
# leader_exists = os.path.exists(leader_path)

HEARTBEAT_INTERVAL = 2
TIMEOUT = 5

# Function to check if the file has entries
def is_file_empty(file_path):
    # Check if the file exists and if it has a size greater than 0
    return os.path.getsize(file_path) == 0

# file_exists = os.path.exists(file_path)


class Peer:
    # Class variable to store all peers by their peer_id
    peers_by_id = {}

    def __init__(self, peer_id, role, network_size, use_caching, leader=False, product=None, neighbors=None):
        """
        Initialize a new Peer instance with the given attributes.

        Args:
            peer_id (int): The unique identifier for the peer.
            role (str): The role of the peer (e.g., "seller", "buyer").
            network_size (int): The size of the network (total number of peers).
            leader (bool): Whether the peer is the leader. Default is False.
            product (str): The product that the peer is selling (if applicable).
            neighbors (list): A list of neighbors (other peer_ids) the peer communicates with. Default is empty.
        """
        self.leader = True  # Indicates whether the peer is the leader (default True).
        self.trader_ids = set()  # Set to store the current trader list
        self.election_inprogress = False  # Flag to track if an election is in progress.
        self.alive = True  # Flag indicating whether the peer is alive.
        self.peer_id = peer_id  # The unique identifier for this peer.
        self.role = role  # The role of the peer (e.g., seller, buyer).
        self.network_size = network_size  # Total number of peers in the network.
        self.product = product  # The product being sold by the peer, if any.
        self.neighbors = neighbors or []  # List of neighboring peer IDs.
        self.lock = threading.Lock()  # Lock for thread synchronization.
        Peer.peers_by_id[self.peer_id] = self  # Add this peer to the global dictionary by its peer_id.
        self.lamport_clock = 0  # Initialize the Lamport clock to 0 for synchronization across peers.
        self.request_queue = []  # Queue to manage buy requests based on Lamport timestamps (for FIFO processing).
        self.last_heartbeat = time.time()
        self.pending_requests = Queue()
        self.use_caching = use_caching
        self.cache = {}
        
    def increment_clock(self):
        """
        Increment the Lamport clock by 1.
        
        This function is called when an event occurs locally (e.g., sending a message or processing an action).
        It ensures that the local Lamport clock is increased to reflect the passage of time in the system.
        """
        self.lamport_clock += 1

    def update_clock(self, other_clock):
        """
        Update the Lamport clock based on the clock received from another process.

        This function is called when a message is received from another process. 
        The Lamport clock is updated to the maximum of the local clock and the received clock, 
        and then incremented by 1 to reflect the "happens-before" relationship.
        
        Args:
            other_clock (int): The Lamport clock value from the other process.
        """
        self.lamport_clock = max(self.lamport_clock, other_clock) + 1


    def run_election(self, number_of_peer, number_of_trader):
        """
        This function handles the election process to elect a new leader in a distributed network.
        It sends election requests to higher peers and waits for their responses. If no higher peer
        responds, it assumes itself as the leader and broadcasts the election result to the network.

        The election process follows these steps:
        1. Notify the network that an election is in progress.
        2. Send requests to peers with higher peer IDs.
        3. Wait for responses from peers.
        4. If no higher peer responds, declare itself as the new leader.
        5. Broadcast the new leader's ID to the network and log it to a file.
        """
        step = (int(number_of_peer) - 1) // (int(number_of_trader) - 1)  # Calculate step size
        trader_list = [i * step for i in range(int(number_of_trader))]
        for peer in trader_list:
            print(f"TRADER is {peer}")
            self.send_request_to_specific_id("set_leader", f"{self.peer_id}", peer)

    # heartbeat function sends heartbeat signal to other trader every 2 seconds
    def heartbeat(self):
        if self.peer_id == 0:
            other_trader_id = 6
        else:
            other_trader_id = 0
        while self.alive:
            try:
                self.send_request_to_specific_id("heartbeat", f"", other_trader_id)
            except Exception as e:
                print(f"[{self.name}] Error sending heartbeat: {e}")
            time.sleep(HEARTBEAT_INTERVAL)
 
    # this is for monitoring the heartbeat from other trader, if the last heartbeat time 
    # has not updated for more than 5 seconds, the trader consider itself as the primary trader
    def monitor_trader(self):
        while self.alive:
            print(f"Monitor *** {time.time()}, {self.last_heartbeat}")
            if time.time() - self.last_heartbeat > TIMEOUT:
                print(f"Other trader is unresponsive. Taking over as primary.")
                self.send_request("trader_fail", f"0")
                print("checkpoint")
                break
            time.sleep(1)

    # Define the function to simulate sending an item for sale to peers
    def send_item(self):
        time.sleep(5)
        if self.role == "seller":
            while True:
                leader = random.choice(list(self.trader_ids))
                self.send_request_to_specific_id("sell", f"{self.peer_id},{self.product},6", int(leader))
                logging.info(f"Item sent for sale to trader: {self.peer_id}")
                time.sleep(15)  # Wait for 15 seconds before sending the next item
    
    # This function simulate the buys from each of the buyer peer by sending buy request to randomly choosen active trader
    # The active trader is stored on it's trader_ids list
    def initiate_buy_request(self):
        value = 5
        time.sleep(7)
        if self.role == "buyer":
            while True:
                leader = random.choice(list(self.trader_ids))
                current_time = time.time()
                self.send_request_to_specific_id("buy", f"{self.peer_id},{leader},{self.product},{value},{current_time}", int(leader))
                request = ("buy", self.product, value, current_time)
                self.pending_requests.put(request)
                time.sleep(5)
        else:
            pass

    def load_inital_cache(self, logger):
        logger.info("sending inital cache request")
        self.send_request_to_database("load_cache", f"{self.peer_id}")
    
    def propagate_cache(self, logger):
        """Periodically propagate cache values to traders."""
        time.sleep(5)
        if self.role == "trader":
            logger.info(f"starting cache propagation thread for trader {self.peer_id}")
            while True:
                if self.alive:
                    self.send_request_to_database("load_cache", f"{self.peer_id}")
                    time.sleep(10)

    """
        listen_for_requests(self, host, port)

        Start a Server Peer by listenting on a port. 
        This is done as the initial step to start the server part of the Peer
        in the p2p network.
    """
    def listen_for_requests(self, host, port):
        logger = self.setup_logger(self.peer_id, self.role)
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(5)
        logger.info(f"Peer {self.peer_id} listening on {host}:{port} -- caching: {self.use_caching}")

        # if you are a trader start a thread for updating your cache
        # if self.role == "trader" and self.use_caching: 
        if self.use_caching: 
            threading.Thread(target=self.propagate_cache, daemon=True, args=(logger, )).start()

        # if peer is buy make buy request and if seller send items to sale to trader
        threading.Thread(target=self.initiate_buy_request, args=()).start()
        threading.Thread(target=self.send_item, args=()).start()
      
        # listen on the port indefinetly for requests
        while True:
            #start a thread that polls for incoming requests via the handle_request function. 
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket, logger)).start()

    def handle_request(self, client_socket, logger):
        """
            This function handles incoming requests from other peers in the network. It processes different types of requests
            and performs the corresponding actions based on the request type. These actions include handling buy requests,
            leader election updates, inventory updates, and more.

            Args:
                client_socket (socket): The socket object representing the connection with the client peer.
        """
        try:
            request = client_socket.recv(1024).decode()
            
            request_type, data = request.split('|', 1) # seperates the request body into the request_type, and data on the |
            if request_type == "update_cache": 
                logger.info("warehouse has responded to our update cache request!")

                # data is in the form of product,quantity
                salt, squant, boar, bquant, fish, fquant = data.split(',')

                self.cache = {
                    salt: int(squant),
                    fish: int(fquant),
                    boar: int(bquant)
                }

                logger.info(f"trader {self.peer_id} has loaded their cache")

            elif request_type == "buy":
                buyer_id, trader_id, product_name, value, reqTime = data.split(',') 
                logger.info(f"start buy buyer:{buyer_id}, leader:{trader_id}, item:{product_name}")
                
                if self.use_caching:
                    if self.cache.get(product_name) != None and int(self.cache.get(product_name)) >= int(value):
                        logger.info(f"{product_name} was found in the trader {self.peer_id} cache with value {self.cache.get(product_name) }")
                        self.send_request_to_specific_id("cache_hit", reqTime, eval(buyer_id))
                        self.handle_buy(buyer_id, trader_id, product_name, value, reqTime)       
                    else: 
                        self.send_request_to_specific_id("cache_miss", reqTime, eval(buyer_id))
                else: 
                    self.handle_buy(buyer_id, trader_id, product_name, value, reqTime)
            elif request_type == "sell":
                seller_id, seller_product, value = data.split(',')
                logger.info(f"Seller Peer {seller_id} is selling product: {seller_product}")
                self.handle_sell(seller_product, value)
            elif request_type == "set_leader":
                self.leader = True
                self.role = "trader"
                self.handle_trader()
                if FAULT_TOLERANT:
                    threading.Thread(target=self.monitor_trader,).start()
                    threading.Thread(target=self.heartbeat,).start()
            elif request_type == "i_am_trader":
                trader = data
                self.trader_ids.add(trader)
            elif request_type == "fall_sick":
                self.alive = False
            elif request_type == "trader_fail":
                failed_trader = data
                print(f"trader has failed notification{failed_trader}")
                self.trader_ids.remove(failed_trader)
                leader = random.choice(list(self.trader_ids))
                print(f"leader: {leader}, list:{self.trader_ids}, pending_request: {self.pending_requests.qsize()}")
                if self.role == "buyer":
                    logging.info(f"pending requests size is {self.pending_requests.qsize()}")
                    while not self.pending_requests.empty():
                        request = self.pending_requests.get()
                        action, product, quantity, req_time = request
                        logging.info("resending pending request to new trader")
                        self.send_request_to_specific_id(action, f"{self.peer_id},{leader},{self.product},{quantity},{req_time}", int(leader))

            elif request_type == "ok":
                sender_id = data
                logger.info(f"setting is_leader to false for {self.peer_id}")
                self.leader = False
                self.request_already_sent = False
            elif request_type == "are_you_alive":
                sender_id = data.split(',')
                self.handle_alive(sender_id[0])
            elif request_type == "run_election":
                number_of_peer, number_of_trader = data.split(',')
                self.run_election(number_of_peer, number_of_trader)
            elif request_type == "restock_item":
                self.stock = 100
                self.product = random.choice(["boar", "salt", "fish"])
                logger.info(f"Peer {self.peer_id} is being restocked with {self.stock} {self.product} ")
            elif request_type == "multicast":
                # if the node is getting a mutlicast request we need to compare clocks
                new_clock = data
                self.update_clock(int(new_clock))
                logger.info(f"multicast recieved by Peer {self.peer_id} new clock value is {self.lamport_clock}")
            elif request_type == "out_of_stock":
                buyer_id, product_name , req_time = data.split(',')
                self.handle_out_of_stock(buyer_id, product_name, float(req_time))
            elif request_type == "no_item":
                product, resp_time = data.split(',')
                treqTime = time.time() - float(resp_time)
                logger.info(f"Purchase failed with out of stock, Request took {treqTime} seconds")
                self.pending_requests.get()  # Pop from the queue
            elif request_type == "heartbeat":
                if self.alive == True:
                    self.last_heartbeat = time.time()
                    logging.info(f"Received heartbeat {self.last_heartbeat}")
                    logging.info(f"self trader id are {self.trader_ids}")
            elif request_type == "purchase_success":
                buyer_id, product, reqTime = data.split(",")
                self.send_request_to_specific_id("no_cache_msg", reqTime, eval(buyer_id))
                self.pending_requests.get()  # Pop from the queue
            elif request_type == "cache_hit":
                respTime = data
                treqTime = time.time() - float(respTime)
                logger.info(f"cache hit on trader request took {treqTime} seconds")
            elif request_type == "cache_miss": 
                respTime = data
                treqTime = time.time() - float(respTime)
                logger.info(f"cache failed on trader request took {treqTime} seconds")
            elif request_type == "no_cache_msg":
                respTime = data
                treqTime = time.time() - float(respTime)
                logger.info(f"purchase complete request took {treqTime} seconds")
            else: 
                print(f"request type {request_type} was not supported")
            
        except Exception as e:
            logger.info(f"Error handling request: {e}")
        finally:
            client_socket.close() #close the socket after the connection.

    def handle_trader(self):
        self.send_request("i_am_trader", f"{self.peer_id}")        
      
    def handle_buy(self, buyer_id, trader_id, product_name, value, reqTime):
        if self.alive:
          # if we are using cache update the peers internal cache
          if self.use_caching:
              if self.cache.get(product_name) - int(value) >= 0:
                self.cache[product_name] = self.cache.get(product_name) - int(value)
                print(f"trader {self.peer_id} has updated its internal cache for product {product_name} to {self.cache[product_name]}")

          self.send_request_to_database("decrement", f"{product_name},{value},{buyer_id},{trader_id},{reqTime}")
    
    def handle_sell(self, product, value): 
        self.send_request_to_database("increment", f"{product},{value}")

    def handle_out_of_stock(self, buyer_id, product_name, req_time): 
        self.send_request_to_specific_id("no_item", f"{product_name},{req_time}", eval(buyer_id))

    def multicast_request(self):

        """
        This function sends the current Lamport clock value to all peers in the network.
        It is used to keep all peers' clocks synchronized.
        
        It iterates through all peers in the network and sends a multicast request with the
        current Lamport clock value. This helps to propagate the clock updates across all peers.
        """

        # update all nodes with the latest lamport clock value
        request_type = "multicast"
        lamport_clock = self.lamport_clock

        for peer in Peer.peers_by_id.values():
            self.send_request_to_specific_id(request_type, lamport_clock, peer.peer_id)
            
    
    def fall_sick(self, retry=False):    
        """
        Simulates the leader "falling sick" (becoming unavailable). When the leader falls sick,
        an election is triggered to select a new leader.
        
        The probability of falling sick is random, but can be forced by passing `retry=True`.
        If the leader falls sick, an election is initiated with another peer.

        Args:
            retry (bool): If True, forces the function to retry the election process.
        """
        # randomly make it possible for the leader to fall_sick 
        chance = rand.random()
        if chance < 1 or retry:
            logging.info("============leader is falling sick =======================")

            election_peer = Peer.peers_by_id.get(0)
            if election_peer.alive and election_peer.peer_id != self.peer_id:
                print(f"{election_peer.peer_id} is starting the election")
                self.alive = False
                # self.send_request_to_specific_id("run_election", f"{self.peer_id}", int(0))
                time.sleep(1)
            else: 
                print("peer is not alive retrying running the election")
                time.sleep(1)
                self.fall_sick(retry=True)

    def handle_alive(self, sender_id):
        """
            This function handles an "are_you_alive" request from another peer.

            If the sender's ID is smaller than the current peer's ID, the current peer assumes the role of the leader.
            The peer then sends an "ok" response to acknowledge the sender's request and initiates a new election if necessary.

            Args:
                sender_id (int): The ID of the peer that sent the "are_you_alive" request.
        """
        if(self.alive == True):
            if int(sender_id) < self.peer_id:
                self.leader = True
            logging.info(f"sending ok reply to sender {sender_id} from peer {self.peer_id}")
            self.send_request_to_specific_id("ok", f"{self.peer_id}", eval(sender_id))
            # self.run_election()

    def send_request(self, request_type, data):
        """
            This function sends a request to all peers in the network.

            For each peer in the network, it creates a socket, connects to the peer at a specific port,
            and sends a request with the specified type and data.

            Args:
                request_type (str): The type of the request (e.g., "buy", "set_leader").
                data (str): The data to be sent with the request.
        """
        for peer_number in range(self.network_size):
            try:
                peer = Peer.peers_by_id.get(peer_number)
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + peer.peer_id))
                peer_socket.send(f"{request_type}|{data}".encode())
                peer_socket.close()
            except Exception as e:
                logging.info(f"Error sending request to Peer {peer_number}: {e}")

    def send_request_to_specific_id(self, request_type, data, peer_id):
        """
            This function sends a request to a specific peer identified by peer_id.

            It creates a socket, connects to the specified peer, and sends the request with the given type and data.

            Args:
                request_type (str): The type of the request (e.g., "buy", "set_leader").
                data (str): The data to be sent with the request.
                peer_id (int): The ID of the peer to which the request is being sent.
        """
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect(('127.0.0.1', 5000 + peer_id))
            peer_socket.send(f"{request_type}|{data}".encode())
            peer_socket.close()
        except Exception as e:
            logging.info(f"Error sending request to Peer {peer_id}: {e}")
    
    def send_request_to_database(self, request_type, data):
        """
            This function sends a request to a specific peer identified by peer_id.

            It creates a socket, connects to the specified peer, and sends the request with the given type and data.

            Args:
                request_type (str): The type of the request (e.g., "buy", "set_leader").
                data (str): The data to be sent with the request.
                peer_id (int): The ID of the peer to which the request is being sent.
        """
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect(('127.0.0.1', 8081))
            peer_socket.send(f"{request_type}|{data}".encode())
            peer_socket.close()
        except Exception as e:
            logging.info(f"Error sending request to database: {e}")
    
    @staticmethod
    def setup_logger(peer_id, role):
        """Set up a logger for a specific peer."""
        log_filename = f"peer_{peer_id}_{role}.log"
        logger = logging.getLogger(f"Peer_{peer_id}_Logger")
        logger.setLevel(logging.INFO)

        with open(log_filename, 'w'):
            pass

        # Avoid adding multiple handlers
        if not logger.handlers:
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.INFO)

            formatter = logging.Formatter(
                fmt="%(asctime)s - Peer %(message)s",
                datefmt="%H:%M:%S"
            )
            file_handler.setFormatter(formatter)

            logger.addHandler(file_handler)

        return logger