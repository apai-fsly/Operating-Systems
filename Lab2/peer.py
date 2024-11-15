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
        self.stock = 10 if role == "seller" else 0  # stock of items if seller, otherwise 0
        self.request_already_sent = False
        Peer.peers_by_id[self.peer_id] = self
        self.lamport_clock = 0  # Initialize Lamport clock
        self.cash_recieved = 0
        self.buy_requests = []  # Queue for buy requests to process in order

    def increment_clock(self):
        """Increment the Lamport clock."""
        self.lamport_clock += 1

    def update_clock_on_receive(self, received_clock):
        """Update the Lamport clock on receiving a message."""
        self.lamport_clock = max(self.lamport_clock, received_clock) + 1
    
    def multicast_request(self, request_type, data):
        """Send a multicast message to all peers."""
        self.increment_clock()
        data_with_clock = f"{self.lamport_clock}|{data}"
        for peer_number in range(self.network_size):
            if peer_number != self.peer_id:
                try:
                    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peer_socket.connect(('127.0.0.1', 5000 + peer_number))
                    peer_socket.send(f"{request_type}|{data_with_clock}".encode())
                    peer_socket.close()
                except Exception as e:
                    logging.info(f"Error sending multicast to Peer {peer_number}: {e}")
    
    def offer_new_item(self, item_name, quantity):
        """Allow sellers to offer new items."""
        if self.role != "seller":
            print(f"Peer {self.peer_id} is not a seller and cannot offer items.")
            return
        with self.lock:
            self.product = item_name
            self.stock += quantity
            print(f"Seller {self.peer_id} now offering {quantity} units of {item_name}.")
            self.multicast_request("selling_list", f"{self.peer_id},{item_name},{self.stock}")

    def handle_sale_credit(self, seller_id, quantity, total_price):
        """Credit the seller after a successful sale."""
        if self.peer_id == seller_id:
            with self.lock:
                self.cash_received += total_price
                print(f"Seller {self.peer_id} credited with ${total_price}. Total balance: ${self.cash_received}.")


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

        if self.leader and not self.request_already_sent:
            print(f"I am the leader {self.peer_id}")
            leader_id = self.peer_id
            self.request_already_sent = True
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

    def handle_request(self, client_socket):
        try:
            request = client_socket.recv(1024).decode()
            request_type, clock_data, data = request.split('|', 2)

            received_clock = int(clock_data)  # Extract received Lamport clock
            self.update_clock_on_receive(received_clock)  # Update local Lamport clock

            if request_type == "buy":
                buyer_id, leader_id, product_name = data.split(',')
                print(f"start buy buyer:{buyer_id}, leader:{leader_id}, item:{product_name}")
                self.handle_buy_from_leader(buyer_id, leader_id, product_name)
            elif request_type == "set_leader":
                self.leader_id = data
                print(f"leader Set complete {self.leader_id}")
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
            elif request_type == "item_bought":
                self.stock -= 1
                self.cash_received += 1
                print(f"Seller {self.peer_id} sold a product and received cash 1$, total cash accumulated: {self.cash_received}$")

        except Exception as e:
            logging.info(f"Error handling request: {e}")
        finally:
            client_socket.close()

    def handle_buy_from_leader(self, buyer_id, leader_id, product_name):
        if not file_exists:
            print("Error: No product file found.")
            return False
        
        
        print("checkpoint 1")

        print("checkpoint 1")
        with self.lock:
            inventory = []
            with open(file_path, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                inventory = list(reader)

            transaction_complete = False
            for entry in inventory:
                if entry["product_name"] == product_name and int(entry["product_stock"]) > 0:
                    entry["product_stock"] = str(int(entry["product_stock"]) - 1)
                    print(f"Purchase successful: Buyer {buyer_id} bought {1} of {product_name} from Leader {leader_id}.")
                    self.send_request_to_specific_id("item_bought", f"{self.peer_id}", eval(entry["seller_id"]))
                    transaction_complete = True
                    break

            if not transaction_complete:
                print(f"Item {product_name} unavailable for sale or out of stock")

            try:
                with open(file_path, mode='w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                    writer.writeheader()
                    writer.writerows(inventory)
                print(f"Inventory updated in {file_path}")
            except IOError as e:
                print(f"IOError: Could not update the file. {e}")    

    def handle_file_write(self, seller_id, seller_product, product_stock):
        existing_entries = []
        if file_exists:
            with open(file_path, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                existing_entries = list(reader)

        new_entry = {"seller_id": seller_id, "product_name": seller_product, "product_stock": product_stock}
        is_unique = all(
            entry["seller_id"] != new_entry["seller_id"] or entry["product_name"] != new_entry["product_name"]
            for entry in existing_entries
        )

        with self.lock:
            if is_unique:
                try:
                    with open(file_path, mode='a' if file_exists else 'w', newline='') as file:
                        writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                        if not file_exists:
                            writer.writeheader()
                        seller_goods = [{"seller_id": seller_id, "product_name": seller_product, "product_stock": product_stock}]
                        print(f"{seller_goods}")
                        writer.writerows(seller_goods)
                    print(f"Data successfully written to {file_path}")
                except FileNotFoundError:
                    print(f"Error: The file path {file_path} could not be found.")
                except IOError as e:
                    print(f"IOError: {e}") 

    def handle_seller_list(self, leader_id):
        if self.alive:
            if self.role == "seller":
                self.send_request_to_specific_id("selling_list", f"{self.peer_id},{self.product},{self.stock}", self.peer_id)

    def handle_alive(self, sender_id):
        if self.alive:
            print(f"sending ok reply to sender {sender_id} from peer {self.peer_id}")
            self.send_request_to_specific_id("ok", f"{self.peer_id}", eval(sender_id))
            self.run_election()

    def send_request(self, request_type, data):
        self.increment_clock()  # Increment clock before sending
        data_with_clock = f"{self.lamport_clock}|{data}"  # Attach clock to message
        for peer_number in range(self.network_size):
            try:
                peer = Peer.peers_by_id.get(peer_number)
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + peer.peer_id))
                peer_socket.send(f"{request_type}|{data_with_clock}".encode())
                peer_socket.close()
            except Exception as e:
                logging.info(f"Error sending request to Peer {peer_number}: {e}")

    def send_request_to_specific_id(self, request_type, data, peer_id):
        self.increment_clock()  # Increment clock before sending
        data_with_clock = f"{self.lamport_clock}|{data}"  # Attach clock to message
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect(('127.0.0.1', 5000 + peer_id))
            peer_socket.send(f"{request_type}|{data_with_clock}".encode())
            peer_socket.close()
        except Exception as e:
            logging.info(f"Error sending request to Peer {peer_id}: {e}")
