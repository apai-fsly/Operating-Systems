import threading
import socket
import logging
import time
import csv
import os


# shared file among the leaders
# Get and display the current working directory
current_directory = os.getcwd()
print(f"Current working directory: {current_directory}")

# Define the file name in the current directory
file_name = "seller_goods.csv"
file_path = os.path.join(current_directory, file_name)
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
    def __init__(self, peer_id, role, network_size, product=None, neighbors=None):
        self.leader = True
        self.leader_id = -1
        self.alive = True
        self.peer_id = peer_id
        self.role = role
        self.network_size = network_size
        self.product = product
        self.neighbors = neighbors or []
        self.lock = threading.Lock()
        self.stock = 10 if role == "seller" else 0 # if the role is seller set the stock to 10 otherwise 0
        self.hop_limit = 3
        self.request_already_sent = False
        Peer.peers_by_id[self.peer_id] = self


    def run_election(self):
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
        time.sleep(1)

        if(self.leader == True and self.request_already_sent == False):
            print(f"I am the leader {self.peer_id}")
            leader_id = self.peer_id
            self.request_already_sent = True
            # for peer in range(self.network_size):
            self.send_request("set_leader", leader_id)
            self.send_request("give_seller_list", leader_id)
            


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

            if request_type == "lookup":
                buyer_id, buytime, product_name, hopcount_str, search_path_str = data.split(',', 4)
                logging.info(f"Peer {self.peer_id} Lookup call: buyer:{buyer_id}, product:{product_name}, hopcount:{hopcount_str}, search path:{search_path_str}")
                hopcount = int(hopcount_str)
                search_path = eval(search_path_str)
                self.handle_lookup(buyer_id, buytime, product_name, hopcount, search_path)
            elif request_type == "reply":  
                buyer_id, buytime, seller_id, reply_path_str = data.split(',',3)
                logging.debug(f"data buyer, seller, path {buyer_id}, {seller_id}, {reply_path_str}")
                reply_path = eval(reply_path_str)
                self.send_reply(buyer_id,buytime, seller_id, reply_path)
            elif request_type == "buy":
                buyer_id, seller_id = data.split(',')
                self.handle_buy(buyer_id, seller_id)
            elif request_type == "set_leader":
                leader_id = data
                self.leader = leader_id
                print(f"leader Set complete {leader_id}")
            elif request_type == "ok":
                sender_id = data.split(',')
                print(f"setting is_leader to false for {self.peer_id}")
                self.leader = False
                self.request_already_sent = False
            elif request_type == "are_you_alive":
                sender_id = data
                self.handle_alive(sender_id)
            elif request_type == "give_seller_list":
                leader_id = data
                print("requesting seller list from the leader")
                self.handle_seller_list(leader_id)        # you might not even need leader_id here because each peer know 
            elif request_type == "selling_list":
                seller_id, seller_product, product_stock = data.split(',')
                print(f"Items for sale is from {seller_id}, {seller_product}, {product_stock}")
                self.handle_file_write(seller_id, seller_product, product_stock)

        except Exception as e:
            logging.info(f"Error handling request: {e}")
        finally:
            client_socket.close() #close the socket after the connection.

    def handle_file_write(self, seller_id, seller_product, product_stock):
        # Write data to the CSV file in the current directory open(file_path, mode='a' if file_exists else 'w', newline='')
        with self.lock:
            try:
                with open(file_path, mode ='a' if file_exists else 'w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                    # Write header only if the file is being created for the first time
                    if not file_exists:
                        writer.writeheader()
                    seller_goods=[{"seller_id":seller_id, "product_name":seller_product, "product_stock": product_stock}]
                    print(f"{seller_goods}")
                    writer.writerows(seller_goods)  # Write each item as a row
                print(f"Data successfully written to {file_path}")
            except FileNotFoundError:
                print(f"Error: The file path {file_path} could not be found.")
            except IOError as e:
                print(f"IOError: {e}") 

    def handle_seller_list(self, leader_id):
        if(self.alive == True):
            if(self.role == "seller"):
                self.send_request_to_specific_id("selling_list", f"{self.peer_id},{self.product},{self.stock}", self.peer_id)

    def handle_alive(self, sender_id):
        if(self.alive == True):
            print(f"sending ok reply to sender {sender_id} from peer {self.peer_id}")
            self.send_request_to_specific_id("ok", f"{self.peer_id}", eval(sender_id))
            self.run_election()

    """
        handle_lookup(buyer_id, buytime, product_name, hop_count, search_path)

        if the lookup was sucessfull (meaning the buyer has reached the proper seller peer [correct product and in stock])
        start the reply chain by leveraging the search_path. 
    """
    def handle_lookup(self, buyer_id, buytime, product_name, hopcount, search_path):
        if self.role == "seller" and self.product == product_name and self.stock > 0:
            seller_id = self.peer_id
            logging.info(f"Peer {self.peer_id} has found {product_name} via the following path of peers {search_path}")
            logging.info(f"Peer {self.peer_id} (seller) has {product_name}. Sending reply from Peer(seller):{seller_id} to Peer(buyer):{buyer_id} ")
            self.send_reply(buyer_id,buytime, seller_id, search_path)

        elif hopcount > 0:
            if self.peer_id not in search_path:
                search_path.append(self.peer_id)
                self.send_request("lookup", f"{buyer_id},{buytime},{product_name},{hopcount-1},{search_path}")

    """
        handle_buy(buyer_id, buytime, seller_id)

        Fetch the seller peer via the peers_by_id map. 
        If a peer is found attempt to take the Peer.lock and decrement 
        the inventory by 1.

        In addition, for logging purposes this function marks the end of the
        transaction and the time can be calcualted.  
    """
    def handle_buy(self, buyer_id, buytime, seller_id):
        seller = Peer.peers_by_id.get(eval(seller_id))
        if seller is None:
            logging.info(f"Seller {seller_id} not found.")
            return

        with seller.lock:
            if seller.stock > 0:
                seller.stock -= 1
                logging.info(f"Peer {seller.peer_id} sold item to {buyer_id}. Stock left: {seller.stock}")
                current_time = time.time()
                time_diff = current_time - float(buytime)
                logging.info(f"Time to complete the transaction: {time_diff}")
            else:
                logging.info(f"Peer {seller.peer_id} is out of stock.") # restocking occurs in the main function. 

    """
        send_reply(buyer_id, buytime, seller_id, reply_path)

        Checks the reply_path to see if there is a value present.
        If there is a value present it means we have not yet reached a seller - pop the last element of the list
        Send the reply to the next neighbor that was popped of the list via send_reply_to_next_peer

        If the peer_id matches the buyer_id it means the reply has reached the buyer. 
        Initiate the transaction process via the handle_buy function.  
    """
    def send_reply(self, buyer_id, buytime, seller_id, reply_path):
        if reply_path:
            logging.debug(f"reply path{reply_path}")
            next_peer_id = reply_path.pop()  # Get the next peer ID from the reply path
            logging.debug(f"next peer id{next_peer_id}")
            self.send_reply_to_next_peer(buyer_id, buytime, reply_path, next_peer_id, seller_id)
        else:
            if self.peer_id == eval(buyer_id):  # Ensure that this reached back to buyer
                logging.info(f"Transaction initiated: Peer {seller_id} is ready to sell to buyer {buyer_id}")
                self.handle_buy( buyer_id,buytime, seller_id)


    """
        send_reply_to_next_peer(buyer_id, buytime, reply_path, next_peer_id, seller_id)

        attempts to send a reply to the next peer via the next_peer_id that was passed into the function. 
    """
    def send_reply_to_next_peer(self, buyer_id, buytime, reply_path, next_peer_id, seller_id):
        next_peer = Peer.peers_by_id.get(next_peer_id)
        if next_peer:
            logging.debug(f"Sending reply to next peer {next_peer.peer_id}")
            next_peer.send_reply_request("reply", f"{buyer_id},{buytime},{seller_id},{reply_path}", next_peer_id)
        else:
            logging.info(f"Next peer {next_peer_id} not found.")


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