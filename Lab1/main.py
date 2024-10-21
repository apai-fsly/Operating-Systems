import json
import os
import signal
import socket
import threading
import random
import time
import sys
from peer import Peer

class Peer:
    # Class-level dictionary to map peer IDs to Peer objects
    peers_by_id = {}

    def __init__(self, peer_id, role, product=None, neighbors=None):
        """
        Initialize a peer with a unique ID, role (buyer or seller), 
        optional product (for sellers), and neighbors (other peers).
        """
        self.peer_id = peer_id  # Unique identifier for the peer
        self.role = role  # Role can be 'buyer' or 'seller'
        self.product = product  # Product available for sale (only for sellers)
        self.neighbors = neighbors or []  # List of neighboring peers for communication
        self.lock = threading.Lock()  # Lock for thread safety during transactions
        self.stock = 10 if role == "seller" else 0  # Sellers start with 10 items, buyers have none
        self.hop_limit = 3  # Maximum number of hops for lookup requests
        Peer.peers_by_id[self.peer_id] = self  # Register this peer in the class-level dictionary

    def listen_for_requests(self, host, port):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Peer {self.peer_id} listening on {host}:{port}")

        while not exit_event.is_set():  # Keep listening until exit_event is set
            try:
                server_socket.settimeout(1)  # Timeout periodically to check the exit flag
                client_socket, address = server_socket.accept()
                threading.Thread(target=self.handle_request, args=(client_socket,)).start()
            except socket.timeout:
                continue  # Loop again to check exit_event

        server_socket.close()  # Cleanly close the socket when shutting down
        print(f"Peer {self.peer_id} has stopped listening.")

    def handle_request(self, client_socket):
        """
        Handle incoming requests from other peers. It processes different types of requests 
        such as lookup, reply, and buy requests.
        """
        request = client_socket.recv(1024).decode()  # Receive request from the socket
        print(f"Peer {self.peer_id} received request: {request}")
        request_type, data = request.split('|', 1)  # Split the request into type and data

        if request_type == "lookup":
            # Process lookup request from a buyer
            buyer_id, product_name, hopcount_str, search_path_str = data.split(',', 3)
            hopcount = int(hopcount_str)  # Convert hopcount to integer
            search_path = eval(search_path_str)  # Convert string representation of list back to a list
            self.handle_lookup(buyer_id, product_name, hopcount, search_path)  # Call method to handle lookup
        elif request_type == "reply":
            # Process reply from a seller indicating availability of the product
            seller_id, reply_path_str = data.split(',', 1)
            reply_path = eval(reply_path_str)  # Convert string representation of list back to a list
            self.send_reply(seller_id, reply_path)  # Send reply back to the previous peer
        elif request_type == "buy":
            # Process buy request from a buyer
            buyer_id = data
            self.handle_buy(buyer_id)  # Call method to handle buy request

        client_socket.close()  # Close the connection after processing the request

    def handle_lookup(self, buyer_id, product_name, hopcount, search_path):
        """
        Handle lookup requests from buyers. If the peer is a seller and has the requested product, 
        it responds. If not, it forwards the request to its neighbors if the hop count is not exhausted.
        """
        if self.role == "seller" and self.product == product_name and self.stock > 0:
            # If this peer is a seller and has the product, respond to the buyer
            print(f"Peer {self.peer_id} (seller) has {product_name}. Sending reply to {buyer_id}.")
            self.send_reply(buyer_id, search_path + [self.peer_id])  # Send reply back through the search path
        elif hopcount > 0:
            # If not found or not a seller, forward the request to neighbors
            search_path.append(self.peer_id)  # Add this peer to the search path
            for neighbor in self.neighbors:
                # Send the lookup request to each neighbor
                neighbor.send_request("lookup", f"{buyer_id},{product_name},{hopcount-1},{search_path}")

    def send_reply(self, buyer_id, reply_path):
        """
        Send a reply message back to the buyer through the reverse path.
        If the path is empty, it indicates that the transaction can begin.
        """
        if reply_path:
            # Continue sending the reply to the next peer in the path
            next_peer_id = reply_path.pop()  # Get the next peer ID
            next_peer = Peer.peers_by_id[next_peer_id]  # Look up the next peer object
            next_peer.send_request("reply", f"{self.peer_id},{reply_path}")  # Send reply to next peer
        else:
            # If the reply path is empty, initiate the transaction
            print(f"Transaction initiated: Peer {self.peer_id} is ready to sell.")
            if self.role == "seller":
                # Notify the buyer to initiate the buy request
                self.send_request("buy", str(buyer_id))  # Send buy request to the buyer
    
    def check_restock(self, config_used): 
        if self.stock == 0 and self.role == "seller": 
            self.stock = 10
            if not config_used: 
                self.product = random.choice("boar", "fish", "salt")
            
        print(f"{self.peer_id} is restocking with {self.stock} items of type {self.product}")

    def handle_buy(self, buyer_id):
        """
        Handle buy requests from buyers. If the seller has stock, it decreases the stock and confirms the sale.
        """
        with self.lock:
            if self.stock > 0:
                self.stock -= 1  # Reduce stock by one
                print(f"Peer {self.peer_id} sold item to {buyer_id}. Stock left: {self.stock}")
            else:
                print(f"Peer {self.peer_id} is out of stock.")  # Notify if out of stock

    def send_request(self, request_type, data):
        """
        Send a request to neighboring peers. It establishes a socket connection to each neighbor and 
        sends the request.
        """
        for neighbor in self.neighbors:
            try:
                # Create a socket and connect to the neighbor's listening port
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + neighbor.peer_id))  # Assuming peers listen on sequential ports
                peer_socket.send(f"{request_type}|{data}".encode())  # Send the request with type and data
                peer_socket.close()  # Close the socket after sending
            except Exception as e:
                print(f"Error sending request to Peer {neighbor.peer_id}: {e}")  # Handle any connection errors

def setup_peers(num_peers):
    """
    Create a specified number of peers with random roles (buyer or seller) and 
    assign products to sellers. Each peer is also assigned random neighbors.
    """
    peers = []
    roles = ['buyer', 'seller']
    products = ['fish', 'salt', 'boar']

    # Create peers with random roles
    for i in range(num_peers):
        role = random.choice(roles)  # Randomly assign a role
        product = random.choice(products) if role == 'seller' else None  # Assign a product if seller
        peer = Peer(peer_id=i, role=role, product=product)  # Create a new Peer object
        peers.append(peer)

    # Assign neighbors (up to 3 neighbors for each peer)
    for peer in peers:
        peer.neighbors = random.sample([p for p in peers if p != peer], 3)  # Randomly select neighbors

    return peers  # Return the list of peers

def run_peer(peer, host, port):
    """
    Start a separate thread for each peer to listen for incoming requests.
    """
    threading.Thread(target=peer.listen_for_requests, args=(host, port)).start()

def load_config(config_path):
    """
    Load the network configuration from a JSON file.
    The JSON file should specify peers, their roles, products, and neighbors.
    """
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    
    peers = []
    peer_map = {}

    # Create peers based on the configuration
    for peer_info in config['peers']:
        peer_id = peer_info['peer_id']
        role = peer_info['role']
        product = peer_info.get('product')  # Only sellers will have products
        peer = Peer(peer_id, role, product)
        peers.append(peer)
        peer_map[peer_id] = peer

    # Assign neighbors based on configuration
    for peer_info in config['peers']:
        peer = peer_map[peer_info['peer_id']]
        peer.neighbors = [peer_map[n] for n in peer_info['neighbors']]

    return peers


# Shared flag to signal threads to stop
exit_event = threading.Event()

def signal_handler(sig, frame):
    print("\nCtrl+C caught. Shutting down the network...")
    exit_event.set()  # Set the exit event to notify all threads to stop
    sys.exit(0)

if __name__ == "__main__":
    """
        Main function that sets up and runs the peer network.
        Accepts either a number of peers (for random generation) or a config file path (for loading a preset network).
        If no arguments are provided, it defaults to 6 peers.
    """

    signal.signal(signal.SIGINT, signal_handler)

    if len(sys.argv) < 2:
        print("Usage: python main.py <num_peers> or python main.py <config_file>")
        sys.exit(1)
    
    # Determine if the argument is a number (number of peers) or a config file
    config_used = False
    arg = sys.argv[1]
    if arg.isdigit():
        num_peers = int(sys.argv[1])
        peers = setup_peers(num_peers)
    else:
        config_file = sys.argv[1]
        if not os.path.isfile(config_file):
            print(f"Error: Config file '{config_file}' not found.")
            sys.exit(1)
        peers = load_config(config_file)
        config_used = True

    # Start each peer in its own thread
    for i, peer in enumerate(peers):
        run_peer(peer, host='127.0.0.1', port=5000 + i) 

    # Simulate buyers looking for products periodically
    while True:
        for peer in peers:
            if peer.role == 'buyer':

                if config_used:
                    print(f"Peer {peer.peer_id} is looking for {peer.product}")
                    peer.send_request('lookup', f'{peer.peer_id},{peer.product},{peer.hop_limit},[]')
                else: 
                    # Randomly select a product to look for
                    product = random.choice(['fish', 'salt', 'boar'])
                    peer.send_request('lookup', f'{peer.peer_id},{product},{peer.hop_limit},[]')  # Send lookup request
            elif peer.role == "seller" and peer.stock == 0: 
                peer.check_restock(config_used)
            time.sleep(random.uniform(1, 3))