import socket
import threading
import random
import time

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
        print(f"checking {self.peer_id} stock")
        time.sleep(3)
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

