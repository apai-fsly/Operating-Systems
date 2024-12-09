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


# shared file among the leaders
# Get and display the current working directory
current_directory = os.getcwd()
print(f"Current working directory: {current_directory}")

# Define the file name in the current directory
file_name = "seller_goods.csv"
file_path = os.path.join(current_directory, file_name)

leader_name = "leader.csv"
leader_path = os.path.join(current_directory, leader_name)
leader_exists = os.path.exists(leader_path)

# Function to check if the file has entries
def is_file_empty(file_path):
    # Check if the file exists and if it has a size greater than 0
    return os.path.getsize(file_path) == 0

file_exists = os.path.exists(file_path)


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
        self.leader_id = -1  # The ID of the leader (initialized to -1).
        self.election_inprogress = False  # Flag to track if an election is in progress.
        self.alive = True  # Flag indicating whether the peer is alive.
        self.peer_id = peer_id  # The unique identifier for this peer.
        self.role = role  # The role of the peer (e.g., seller, buyer).
        self.network_size = network_size  # Total number of peers in the network.
        self.product = product  # The product being sold by the peer, if any.
        self.neighbors = neighbors or []  # List of neighboring peer IDs.
        self.lock = threading.Lock()  # Lock for thread synchronization.
        self.stock = 100 if role == "seller" else 0  # Stock of the product, 1000 for sellers, 0 for others.
        self.request_already_sent = False  # Flag to track if a request has already been sent.
        Peer.peers_by_id[self.peer_id] = self  # Add this peer to the global dictionary by its peer_id.
        self.cash_received = 0  # Total amount of cash received by the seller after product sales (1 dollar per product).
        self.lamport_clock = 0  # Initialize the Lamport clock to 0 for synchronization across peers.
        self.request_queue = []  # Queue to manage buy requests based on Lamport timestamps (for FIFO processing).
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


    def run_election(self):
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
        self.send_request("election_inprogress", data=None)
        higher_peer_id = []
        for peer in range(self.network_size):
            if peer > self.peer_id: 
                higher_peer_id.append(peer)

                # all bigger peer IDs are in the list
        for peer in higher_peer_id: 
            self.send_request_to_specific_id("are_you_alive", f"{self.peer_id}", peer)

        # wait for a reply from peers
        time.sleep(5)

        if(self.leader == True and self.request_already_sent == False):
            logging.info(f"New leader elected: {self.peer_id}")
            leader_id = self.peer_id
            self.request_already_sent = True
            # for peer in range(self.network_size):
            self.send_request("set_leader", leader_id)
            self.send_request("give_seller_list", leader_id)
            # write leader to file
            with self.lock:
                try:
                    with open(leader_path, mode ='w', newline='') as file:
                        writer = csv.DictWriter(file, fieldnames=["leader_id", "election_in_progress"])
                        # Write header only if the file is being created for the first time
                        # if not leader_exists:
                        writer.writeheader()
                        election_outcome=[{"leader_id":leader_id, "election_in_progress":0}]
                        print(f"Peer {election_outcome} has been elected as the new leader!")
                        writer.writerows(election_outcome)  # Write each item as a row
                except FileNotFoundError:
                    print(f"Error: The file path {leader_path} could not be found.")
                except IOError as e:
                    print(f"IOError: {e}")  

        
    # send request 
    def load_inital_cache(self, logger):
        logger.info("sending inital cache request")
        self.send_request_to_database("load_cache", f"{self.peer_id}")

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
        logger.info(f"Peer {self.peer_id} listening on {host}:{port}")
        logger.info(f"Peer {self.peer_id} has caching set to {self.use_caching}")

        if self.role == "trader" and self.use_caching: 
            logger.info(f"{self.peer_id} performing intial cache loading")
            self.load_inital_cache(logger)

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
                buyer_id, trader_id, product_name, value = data.split(',') 
                logger.info(f"start buy buyer:{buyer_id}, leader:{trader_id}, item:{product_name}")
                
                if self.use_caching:
                    if self.cache.get(product_name) != None and int(self.cache.get(product_name)) >= int(value):
                        logger.info(f"{product_name} was found in the trader {self.peer_id} cache with value {self.cache.get(product_name) }")
                        self.handle_buy(buyer_id, trader_id, product_name, value)
                    else: 
                        # should we check what the stock is from warehouse
                        self.send_request_to_database("load_cache", f"{self.peer_id}")
                else: 
                    self.handle_buy(buyer_id, trader_id, product_name, value)
            elif request_type == "sell":
                seller_id, seller_product, value = data.split(',')
                logger.info(f"Seller Peer {seller_id} is selling product: {seller_product}")
                self.handle_sell(seller_product, value)
            elif request_type == "set_leader":
                self.leader_id = data
                self.election_inprogress = False
            elif request_type == "ok":
                sender_id = data
                logger.info(f"setting is_leader to false for {self.peer_id}")
                self.leader = False
                self.request_already_sent = False
            elif request_type == "are_you_alive":
                sender_id = data.split(',')
                self.handle_alive(sender_id[0])
            elif request_type == "give_seller_list":
                leader_id = data
                logger.info("requesting seller list from the leader")
                self.handle_seller_list(leader_id)        # you might not even need leader_id here because each peer know 
            elif request_type == "selling_list":
                seller_id, seller_product, product_stock = data.split(',')
                logger.info(f"Seller Peer {seller_id} is selling product: {seller_product}, stock: {product_stock}")
                self.handle_file_write(int(seller_id), seller_product, int(product_stock))
            elif request_type == "item_bought":
                self.stock -= 1
                self.cash_received += 1

                # add a print statement that can be used for comparing timestamps
                # print(f"CASH RECIEVED: {self.cash_received} {datetime.datetime.now()}")
                logger.info(f"Seller {self.peer_id} sold a product and received cash 1$, total cash accumulated: {self.cash_received}$")
            elif request_type == "election_inprogress":
                self.election_inprogress = True
            elif request_type == "run_election":
                self.run_election()
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
                buyer_id, product_name = data.split(',')
                self.handle_out_of_stock(buyer_id, product_name)
            elif request_type == "no_item":
                product = data
                logger.info(f"Unable to complete the buy {product} Out of stock:")
            elif request_type == "purchase_success": 
                logger.info(f"trader {self.peer_id} purchased goods from the warehouse")
            else: 
                print(f"request type {request_type} was not supported")
            
        except Exception as e:
            logger.info(f"Error handling request: {e}")
        finally:
            client_socket.close() #close the socket after the connection.

    def handle_buy(self, buyer_id, trader_id, product_name, value):
        
        # if we are using cache update the peers internal cache
        if self.use_caching: 
            self.cache[product_name] = self.cache.get(product_name) - int(value)
            print(f"trader {self.peer_id} has updated its internal cache for product {product_name} to {self.cache[product_name]}")

        self.send_request_to_database("decrement", f"{product_name},{value},{buyer_id},{trader_id}")
    
    def handle_sell(self, product, value): 
        self.send_request_to_database("increment", f"{product},{value}")

    def handle_out_of_stock(self, buyer_id, product_name): 
        self.send_request_to_specific_id("no_item", f"{product_name}", eval(buyer_id))

    def handle_buy_from_leader(self, buyer_id, leader_id, product_name):

        """
            This function handles the process of a buyer purchasing a product from the leader.
            It checks inventory for product availability, deducts stock, and updates the inventory.
            It also handles a possibility of triggering a leader election based on a random chance.

            Args:
                buyer_id (str): The ID of the buyer making the purchase request.
                leader_id (str): The ID of the leader who is processing the request.
                product_name (str): The name of the product being purchased.
            
            Returns:
                bool: Returns False if an error occurs (e.g., product not found).
        """

        if not file_exists:
            print("Error: No product file found.")
            return False
        

        with self.lock:
            # Queue the request with timestamp
            self.request_queue.append((self.lamport_clock, buyer_id, product_name))
            self.request_queue.sort()  # Sort queue by timestamp for fairness
            # Load current inventory
            inventory = []
            with open(file_path, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                inventory = list(reader)

            # Check if the product is available and has enough stock
            # Process requests in order of Lamport clocks
            while self.request_queue:
                _, current_buyer, requested_product = self.request_queue.pop(0)
                transaction_complete = False
                for entry in inventory:
                    if entry["product_name"] == requested_product and int(entry["product_stock"]) > 0:
                        # Deduct stock
                        entry["product_stock"] = str(int(entry["product_stock"]) - 1)
                        logging.info(f"Purchase successful: Buyer {buyer_id} bought {1} of {requested_product} from Leader {leader_id}.")
                        self.send_request_to_specific_id("item_bought", f"{self.peer_id}", eval(entry["seller_id"]))
                        transaction_complete = True
                        break
                    elif int(entry["product_stock"]) == 0:
                        self.send_request_to_specific_id("restock_item", f"{self.peer_id}", eval(entry["seller_id"]))
                        self.send_request_to_specific_id("give_seller_list", f"{self.peer_id}", eval(entry["seller_id"]))
                        inventory.remove(entry)
                        logging.info(f"{requested_product} removed from inventory as stock is 0.")

                if transaction_complete == False:
                    print(f"Item {product_name} unavailable for sale or out of stock")

            # Update the file with the new stock values
            try:
                with open(file_path, mode='w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                    writer.writeheader()
                    writer.writerows(inventory)
                print(f"Inventory updated in {file_path}")
            except IOError as e:
                print(f"IOError: Could not update the file. {e}")
        
        chance = rand.random()
        if chance < .04:
            if not self.election_inprogress:
                self.election_inprogress = True
        
                try:
                    # Read the existing row (there's only one row)
                    with open(leader_path, mode='r', newline='') as file:
                        reader = csv.DictReader(file)
                        row = next(reader, None)  # Read the single row if it exists
                    
                    # Update the election_in_progress field
                    if row:
                        row["election_in_progress"] =1
                    
                        # Overwrite the file with the updated data
                        with open(leader_path, mode='w', newline='') as file:
                            writer = csv.DictWriter(file, fieldnames=["leader_id", "election_in_progress"])
                            writer.writeheader()
                            writer.writerow(row)
                        
                        print(f"Election status successfully updated to {row['election_in_progress']}.")
                    else:
                        print("Error: No data found in the file to update.")
                
                except FileNotFoundError:
                    print(f"Error: The file {leader_path} could not be found.")
                except IOError as e:
                    print(f"IOError: {e}")
                self.fall_sick()

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
                self.send_request_to_specific_id("run_election", f"{self.peer_id}", int(0))
                time.sleep(1)
            else: 
                print("peer is not alive retrying running the election")
                time.sleep(1)
                self.fall_sick(retry=True)
        
    def handle_file_write(self, seller_id, seller_product, product_stock):
        """
        This function handles writing or updating a product's data in the CSV file.

        It first reads the CSV file into a DataFrame. If a row with the same seller_id and product_name
        already exists, it updates the product_stock. If no such row exists, it appends a new entry for
        that seller and product.

        Args:
            seller_id (int): The ID of the seller.
            seller_product (str): The name of the product being sold.
            product_stock (int): The current stock of the product.
        """
        # Write data to the CSV file in the current directory open(file_path, mode='a' if file_exists else 'w', newline='')
        with self.lock:
            seller_id = int(seller_id)
            df = pd.read_csv(file_path)

            # find rows with matching seller_id and product_name
            query = (df["seller_id"] == seller_id) & (df["product_name"] == seller_product)
            result = df.loc[(query)]

            new_entry = {
                "seller_id": seller_id, 
                "product_name": seller_product, 
                "product_stock": product_stock
            }
            try:
                if not result.empty and len(result) == 1:
                    existing_stock = result['product_stock'].iloc[0]
                    if existing_stock != product_stock:
                        df.loc[query, 'product_stock'] = new_entry["product_stock"]
                elif len(result) > 1: 
                    print("unable to add new entry due to conflicting seller_ids and product_names")
                else:
                    df.loc[len(df)] = new_entry
            except FileNotFoundError:
                print(f"Error: The file path {file_path} could not be found.")
            except IOError as e:
                print(f"IOError: {e}") 

            df.to_csv(file_path, index=False)

    def handle_seller_list(self, leader_id):
        """
        This function handles sending the list of products a seller has available to the leader.

        If the peer is a seller (but not the leader) and is alive, it sends a request to the leader
        with the product's information (product name, stock, and peer ID).

        Args:
            leader_id (int): The ID of the current leader.
        """
        if(self.alive == True):
            if(self.role == "seller" and not self.leader):
                self.send_request_to_specific_id("selling_list", f"{self.peer_id},{self.product},{self.stock}", int(self.leader_id))

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
            self.run_election()

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