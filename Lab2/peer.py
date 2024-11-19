import threading
import multiprocessing
import socket
import logging
import time
import csv
import os
import pandas as pd
import random as rand


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

"""
Peer class defines the peer used within the network

    peer_id: int: representing the ID of the Peer
    role: str: representing whether the peer is a ["buyer", "seller"]
    product: str: representing the produce the peer is selling ["boar", "fish", "salt"] 
    hop_limit: int: representing the number of hops before a request terminates
    stock: int: number of items in inventory (only applies to sellers)
    lock: represents a lock on the peer when accessing resources (stock of items)
    neighbors: list: represents a list of the neighboring peers

    peers_by_id: map: representing a map of peer_id to peer. 
"""
class Peer:
    peers_by_id = {}
    def __init__(self, peer_id, role, network_size, leader=False, product=None, neighbors=None):
        self.leader = True
        self.leader_id = -1
        self.election_inprogress = False
        self.alive = True
        self.peer_id = peer_id
        self.role = role
        self.network_size = network_size
        self.product = product
        self.neighbors = neighbors or []
        # self.lock = threading.Lock()
        self.lock = multiprocessing.Lock()
        self.stock = 1000 if role == "seller" else 0 # if the role is seller set the stock to 10 otherwise 0
        self.request_already_sent = False
        Peer.peers_by_id[self.peer_id] = self
        self.cash_received = 0 # received total amount by seller after product sale, assume 1 dollar for each product
        self.lamport_clock = 0  # Initialize Lamport clock
        self.request_queue = []  # Queue to manage buy requests based on timestamps

    def increment_clock(self):
        self.lamport_clock += 1

    def update_clock(self, other_clock):
        # Update local clock with the max of local and received clocks
        self.lamport_clock = max(self.lamport_clock, other_clock) + 1

    def run_election(self):

        self.send_request("election_inprogress", data=None)
        higher_peer_id = []
        for peer in range(self.network_size):
            print(f"peer is {peer} and {self.peer_id}")
            if peer > self.peer_id: 
                higher_peer_id.append(peer)
                print(f"peer id list {higher_peer_id}")

                # all bigger peer IDs are in the list
        for peer in higher_peer_id: 
            self.send_request_to_specific_id("are_you_alive", f"{self.peer_id}", peer)

        # wait for a reply from peers
        time.sleep(5)

        if(self.leader == True and self.request_already_sent == False):
            print(f"I am the leader {self.peer_id}")
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
                        print(f"Election outcome is  ============== {election_outcome}")
                        writer.writerows(election_outcome)  # Write each item as a row
                    print(f"New Leader written to {leader_path}")
                except FileNotFoundError:
                    print(f"Error: The file path {leader_path} could not be found.")
                except IOError as e:
                    print(f"IOError: {e}")  

    """
        listen_for_requests(self, host, port)

        Start a Server Peer by listenting on a port. 
        This is done as the initial step to start the server part of the Peer
        in the p2p network.
    """
    def listen_for_requests(self, host, port):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(5)
        logging.info(f"Peer {self.peer_id} listening on {host}:{port}")

        # listen on the port indefinetly for requests
        while True:
            #start a thread that polls for incoming requests via the handle_request function. 
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket,)).start()

            # process = multiprocessing.Process(target=self.handle_request, args=(client_socket,))
            # process.start()
            # process.join()  # Ensure the process completes


    """
        handle_request(self, client_sock)

        When the peer is polling for connections it must handle each type of request uniquely.
        There are 3 types of requests that the Peer can handle: [lookup, reply, buy] 

        lookup: is used when the request is being propagated to via the neighboring peers. This code path 
                look to see if they are the seller of the project.

                if y: it will attempt to reply by backtracking on the search path
                if n: it will forward the message along via another lookup and decrement the hop_count
        
        reply: is used when the request has found a buyer and is working backward in the search path until the search path is itself. 
                At this point the buyer has returned to itself and can perform the stock decrement via the handle_buy
        
        buy: is used when the buyer has reached itself and is ready to perform the stock inventory decrement on the seller Peer via the peers_by_id map.  
    """
    def handle_request(self, client_socket):
        try:
            request = client_socket.recv(1024).decode()
            
            request_type, data = request.split('|', 1) # seperates the request body into the request_type, and data on the |

            if request_type == "buy":
                buyer_id, leader_id, product_name, buyer_clock = data.split(',')
                self.update_clock(int(buyer_clock))  # Update clock with buyer's clock
                print(f"start buy buyer:{buyer_id}, leader:{leader_id}, item:{product_name}")
                self.handle_buy_from_leader(buyer_id, leader_id, product_name)
            elif request_type == "set_leader":
                self.leader_id = data
                self.election_inprogress = False
                print(f"leader Set complete on {self.peer_id} and leader is {self.leader_id}")
            elif request_type == "ok":
                sender_id, sender_clock = data.split(',')
                print(f"setting is_leader to false for {self.peer_id}")
                self.leader = False
                self.request_already_sent = False
            elif request_type == "are_you_alive":
                sender_id = data.split(',')
                self.handle_alive(sender_id[0])
            elif request_type == "give_seller_list":
                leader_id = data
                print("requesting seller list from the leader")
                self.handle_seller_list(leader_id)        # you might not even need leader_id here because each peer know 
            elif request_type == "selling_list":
                seller_id, seller_product, product_stock, buyer_clock = data.split(',')
                print(f"Items for sale is from {seller_id}, {seller_product}, {product_stock}")
                self.handle_file_write(int(seller_id), seller_product, int(product_stock))
            elif request_type == "item_bought":
                self.stock -= 1
                self.cash_received += 1
                print(f"Seller {self.peer_id} sold a product and received cash 1$, total cash accumulated: {self.cash_received}$")
            elif request_type == "election_inprogress":
                print(f"{self.peer_id} has detected an election") 
                self.election_inprogress = True
            elif request_type == "run_election":
                self.run_election()
        except Exception as e:
            logging.info(f"Error handling request: {e}")
        finally:
            client_socket.close() #close the socket after the connection.

    def handle_buy_from_leader(self, buyer_id, leader_id, product_name):
        if not file_exists:
            print("Error: No product file found.")
            return False
        

        with self.lock:
            # Queue the request with timestamp
            self.request_queue.append((self.lamport_clock, buyer_id, product_name))
            self.request_queue.sort()  # Sort queue by timestamp for fairness
            print(f"request queue:{self.request_queue}")
            # Load current inventory
            inventory = []
            with open(file_path, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                inventory = list(reader)

            # Check if the product is available and has enough stock
            # Process requests in order of Lamport clocks
            while self.request_queue:
                print(len(self.request_queue))
                _, current_buyer, requested_product = self.request_queue.pop(0)
                transaction_complete = False
                for entry in inventory:
                    print(f'{entry["product_name"]}, {entry["product_stock"]}')
                    if entry["product_name"] == requested_product and int(entry["product_stock"]) > 0:
                        # Deduct stock
                        entry["product_stock"] = str(int(entry["product_stock"]) - 1)
                        print(f"Purchase successful: Buyer {buyer_id} bought {1} of {requested_product} from Leader {leader_id}.")
                        self.send_request_to_specific_id("item_bought", f"{self.peer_id}", eval(entry["seller_id"]))
                        transaction_complete = True
                        break
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
        
        # print("checking if leader is falling sick")
        chance = rand.random()
        if chance < 0.02:
            if not self.election_inprogress:
                self.election_inprogress = True
        
                try:
                    # Read the existing row (there's only one row)
                    with open(leader_path, mode='r', newline='') as file:
                        reader = csv.DictReader(file)
                        row = next(reader, None)  # Read the single row if it exists
                        # print("Just check 111 ====================")
                    
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

    def fall_sick(self, retry=False):    
        # randomly make it possible for the leader to fall_sick 
        # of gaurentee sickness if the 
        # print("checking if leader is falling sick")
        chance = rand.random()
        if chance < 1 or retry:
            print("============leader is falling sick =======================")
            # one of the other nodes should start an election
            election_peer_id = rand.randint(0, self.network_size-1)
            # election_peer = Peer.peers_by_id.get(election_peer_id)
            election_peer = Peer.peers_by_id.get(0)
            if election_peer.alive and election_peer.peer_id != self.peer_id:
                print(f"{election_peer.peer_id} is starting the election")
                self.alive = False
                # election_peer.run_election()
                self.send_request_to_specific_id("run_election", f"{self.peer_id}", int(0))
                time.sleep(1)
            else: 
                print("peer is not alive retrying running the election")
                time.sleep(1)
                self.fall_sick(retry=True)
        
    def handle_file_write(self, seller_id, seller_product, product_stock):
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

        # Load existing entries if file exists
        # existing_entries = []
        # if file_exists:
        #     with open(file_path, mode='r', newline='') as file:
        #         reader = csv.DictReader(file)
        #         existing_entries = list(reader)

        # # df = pd.read_csv(file_path)

        # # # find rows with matching seller_id and product_name
        # # print(df.loc[df['seller_id'] == seller_id & df[seller_product == seller_product]])
        

        # # Check if the new entry is unique
        # new_entry = {
        #     "seller_id": seller_id, 
        #     "product_name": seller_product, 
        #     "product_stock": product_stock
        # }
        # is_unique = all(
        #     entry["seller_id"] != new_entry["seller_id"] or entry["product_name"] != new_entry["product_name"]
        #     for entry in existing_entries
        # )

        # with self.lock:
        #     if is_unique:
        #         try:
        #             with open(file_path, mode ='a' if file_exists else 'w', newline='') as file:
        #                 writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
        #                 # Write header only if the file is being created for the first time
        #                 if not file_exists:
        #                     writer.writeheader()
        #                 seller_goods=[{"seller_id":seller_id, "product_name":seller_product, "product_stock": product_stock}]
        #                 print(f"{seller_goods}")
        #                 writer.writerows(seller_goods)  # Write each item as a row
        #             print(f"Data successfully written to {file_path}")
        #         except FileNotFoundError:
        #             print(f"Error: The file path {file_path} could not be found.")
        #         except IOError as e:
        #             print(f"IOError: {e}") 

    def handle_seller_list(self, leader_id):
        if(self.alive == True):
            if(self.role == "seller" and not self.leader):
                self.send_request_to_specific_id("selling_list", f"{self.peer_id},{self.product},{self.stock}", int(self.leader_id))

    def handle_alive(self, sender_id):
        if(self.alive == True):
            if int(sender_id) < self.peer_id:
                self.leader = True
            print(f"sending ok reply to sender {sender_id} from peer {self.peer_id}")
            self.send_request_to_specific_id("ok", f"{self.peer_id}", eval(sender_id))
            self.run_election()


    """
        send_request(request_type, data)

        is a wrapper function for sending data to its own neighbors. 

        request_type can be: [lookup, reply, buy] as seen in the handle_request function. 
    """
    def send_request(self, request_type, data):
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
        self.increment_clock()  # Increment clock for the outgoing request
        # Include lamport clock value in the data
        data += (f",{self.lamport_clock}")
        print(f"request_type:{request_type} data:{data}")
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect(('127.0.0.1', 5000 + peer_id))
            peer_socket.send(f"{request_type}|{data}".encode())
            peer_socket.close()
        except Exception as e:
            logging.info(f"Error sending request to Peer {peer_id}: {e}")

    """
        send_reply_request(request_type, data, next_peer_id)

        attempts to send a reply request to the next peer to the corresponding peer via the map. 
    """
    def send_reply_request(self, request_type, data, next_peer_id):
        try:
            next_peer = Peer.peers_by_id.get(next_peer_id)
            if next_peer:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + next_peer.peer_id))
                peer_socket.send(f"{request_type}|{data}".encode())
                peer_socket.close()
                logging.debug(f"Reply request 'from Peer {self.peer_id} to Peer {next_peer.peer_id}.")
            else:
                logging.info(f"Next peer {next_peer_id} not found.")
        except Exception as e:
            logging.info(f"Error sending reply request to Peer {next_peer_id}: {e}")