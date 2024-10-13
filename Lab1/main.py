import random
import sys
import socket
import threading

DEFAULT_NUM_PEERS = 3
DEFAULT_NUM_ITEMS = 10
DEFAULT_HOP_COUNT = 3
ROLES = ["FISH SELLER", "BOAR SELLER", "SALT SELLER", "BUYER"]
ITEMS = {
    "FISH SELLER": "FISH",
    "BOAR SELLER": "BOAR",
    "SALT SELLER": "SALT"
}
# Peer class defines how a peer is setup
# port: defines the port the peer can be reached at
# neighbors: defines the list of ports of neighboring peers
# running: defines the running state of the peer
# role: defines if the peer is a type of seller or buyer (num items defaults to 10)
class Peer:
    def __init__(self, port, neighbors, index_role):
        self.port = port
        self.neighbors = neighbors
        self.running = True
        self.get_role(index_role=index_role)
        # Start the listening thread
        self.listener_thread = threading.Thread(target=self.listen)
        self.listener_thread.start()
    
    def get_role(self, index_role): 
       
        if index_role == 0: # making the first peer created as a buyer just for testing
             self.role = "BUYER"
        else: 
            self.role = random.choice(ROLES) # randomly assign a role
            self.product_quantity = DEFAULT_NUM_ITEMS
        

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            print(f'Peer is a {self.role} listening on port {self.port}...')
            while self.running:
                conn, addr = s.accept()
                with conn:
                    data = conn.recv(1024)
                    if data.startswith("LOOKUP"):
                        #print(f'Peer {self.port} received: {data.decode()}')
                        self.handle_lookup(data,conn)
                    elif data.startswith("BUY"):
                        self.handle_buy(data,conn)

    def handle_lookup(self, data, conn):
        _, buyer_id, product_name, hop_count, search_path = data.split()
        buyer_id = int(buyer_id)
        hop_count = int(hop_count)

        if hop_count > 0:
            if self.role == 'SELLER' and self.item == product_name:
                self.reply(buyer_id, search_path)
            else:
                hop_count -= 1
                search_path = search_path + f",{self.peer_id}"
                for neighbor in self.neighbors:
                    if neighbor != buyer_id:  # Don't send back to the buyer
                        self.propagate_lookup(neighbor, buyer_id, product_name, hop_count, search_path)

    def propagate_lookup(self, neighbor, buyer_id, product_name, hop_count, search_path):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', neighbor))
            message = f"LOOKUP {buyer_id} {product_name} {hop_count} {search_path}"
            s.send(message.encode())

    def handle_buy(self, data, conn):
        _, buyer_id = data.split()
        buyer_id = int(buyer_id)
        if self.stock > 0:
            self.stock -= 1
            print(f"Peer {self.peer_id} sold item to Buyer {buyer_id}. Stock remaining: {self.stock}.")
            conn.send(f"SUCCESS {self.peer_id} {self.item}".encode())
        else:
            conn.send(f"OUT_OF_STOCK {self.peer_id}".encode())

    def send_message(self, message):
        for neighbor in self.neighbors:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', neighbor))
                    s.sendall(message.encode())
            except Exception as e:
                print(f"Failed to send message to {neighbor}: {e}")
    
    def lookup(self, product_name):
        for neighbor in self.neighbors:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', neighbor))
                message = f"LOOKUP {self.peer_id} {product_name} {DEFAULT_HOP_COUNT} {self.peer_id}"
                s.send(message.encode())
    
    def reply(self, buyer_id, search_path):
        path = search_path.split(',')
        next_peer = int(path[-2])  # Send the reply to the next peer along the path
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', next_peer))
            message = f"REPLY {self.peer_id} {buyer_id} {','.join(path[:-1])}"
            s.send(message.encode()) 

    def stop(self):
        self.running = False
        self.listener_thread.join()

def main():
    num_peers = get_num_peers()
    peers = []
    ports = [5000 + i for i in range(num_peers)]

    # Create peers and establish neighbors
    for i in range(num_peers):
        neighbors = []
        # Connect to up to 3 neighbors (circular connection)
        for j in range(-3, 0):
            neighbor_index = (i + j) % num_peers
            if neighbor_index != i:  # Avoid self-connection
                neighbors.append(ports[neighbor_index])
        peers.append(Peer(ports[i], neighbors, i))

    try:
        # while True:
        #     message = input("Enter message to send to neighbors (or type 'exit' to quit): \n")
        #     if message.lower() == 'exit':
        #         break
            # Send the message from the first peer as an example
            # peers[0].send_message(message)
        time.sleep(2)  # Give time for all servers to start
        for peer in peers:
            if peer.role == 'BUYER':
                peer.lookup(random.choice(['FISH', 'SALT', 'BOAR']))

    finally:
        for peer in peers:
            peer.stop()

# Get number of peers from command line argument or use default
def get_num_peers(): 
    if len(sys.argv) != 2:
        number = DEFAULT_NUM_PEERS
    else: 
        number = int(sys.argv[1])
    
    print(f"Creating network with {number} peers")
    return number

if __name__ == "__main__":
    main()
