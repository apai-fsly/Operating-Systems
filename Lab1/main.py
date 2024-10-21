import socket
import threading
import random
import time
import sys

class Peer:
    peers_by_id = {}

    def __init__(self, peer_id, role, product=None, neighbors=None):
        self.peer_id = peer_id
        self.role = role
        self.product = product
        self.neighbors = neighbors or []
        self.lock = threading.Lock()
        self.stock = 10 if role == "seller" else 0
        self.hop_limit = 3
        Peer.peers_by_id[self.peer_id] = self

    def listen_for_requests(self, host, port):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Peer {self.peer_id} listening on {host}:{port}")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket,)).start()

    def handle_request(self, client_socket):
        try:
            request = client_socket.recv(1024).decode()
            print(f"Peer {self.peer_id} received request: {request}")
            request_type, data = request.split('|', 1)

            if request_type == "lookup":
                buyer_id, product_name, hopcount_str, search_path_str = data.split(',', 3)
                hopcount = int(hopcount_str)
                search_path = eval(search_path_str)
                self.handle_lookup(buyer_id, product_name, hopcount, search_path)
            elif request_type == "reply":
                
                buyer_id, seller_id, reply_path_str = data.split(',',2)
                print(f"data buyer, seller, path {buyer_id}, {seller_id}, {reply_path_str}")
                # print(f"reply path str {reply_path_str}")
                reply_path = eval(reply_path_str)
                # reply_path = reply_path_str
                
                self.send_reply(buyer_id,seller_id, reply_path)
            elif request_type == "buy":
                buyer_id, seller_id = data.split(',')
                self.handle_buy(buyer_id, seller_id)

        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            client_socket.close()

    def handle_lookup(self, buyer_id, product_name, hopcount, search_path):
        if self.role == "seller" and self.product == product_name and self.stock > 0:
            seller_id = self.peer_id
            print(f"Peer {self.peer_id} (seller) has {product_name}. Sending reply to {buyer_id} from {seller_id}")
            # self.send_reply(buyer_id, search_path + [self.peer_id], seller_id)
            self.send_reply(buyer_id, seller_id, search_path)

        elif hopcount > 0:
            if self.peer_id not in search_path:
                search_path.append(self.peer_id)
            # for neighbor in self.neighbors:
                self.send_request("lookup", f"{buyer_id},{product_name},{hopcount-1},{search_path}")

    def handle_buy(self, buyer_id, seller_id):
        seller = Peer.peers_by_id.get(eval(seller_id))
        if seller is None:
            print(f"Seller {seller_id} not found.")
            return

        with seller.lock:
            if seller.stock > 0:
                seller.stock -= 1
                print(f"Peer {seller.peer_id} sold item to {buyer_id}. Stock left: {seller.stock}")
                # self.send_request("transaction_complete", f"{buyer_id},{seller.peer_id}")
            else:
                print(f"Peer {seller.peer_id} is out of stock.")

    def send_reply(self, buyer_id, seller_id,reply_path):
        if reply_path:
            print(f"reply path{reply_path}")
            next_peer_id = reply_path.pop()  # Get the next peer ID from the reply path
            print(f"next peer id{next_peer_id}")
            self.send_reply_to_next_peer(buyer_id, reply_path, next_peer_id, seller_id)
        else:
            if self.peer_id == eval(buyer_id):  # Ensure that this reached back to buyer
                print(f"Transaction initiated: Peer {seller_id} is ready to sell to buyer {buyer_id}")
                # self.send_request("buy", f"{buyer_id},{seller_id}")
                self.handle_buy( buyer_id, seller_id)
            else:
                print(f"this is bad {self.peer_id}, {buyer_id}")

    def send_reply_to_next_peer(self, buyer_id, reply_path, next_peer_id, seller_id):
        next_peer = Peer.peers_by_id.get(next_peer_id)
        if next_peer:
            print(f"Sending reply to next peer {next_peer.peer_id}")
            next_peer.send_reply_request("reply", f"{buyer_id},{seller_id},{reply_path}", next_peer_id)
        else:
            print(f"Next peer {next_peer_id} not found.")


    def send_request(self, request_type, data):
        for neighbor in self.neighbors:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + neighbor.peer_id))
                peer_socket.send(f"{request_type}|{data}".encode())
                peer_socket.close()
            except Exception as e:
                print(f"Error sending request to Peer {neighbor.peer_id}: {e}")
            print(f"request done")

    def send_reply_request(self, request_type, data, next_peer_id):
        try:
            next_peer = Peer.peers_by_id.get(next_peer_id)
            if next_peer:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + next_peer.peer_id))
                peer_socket.send(f"{request_type}|{data}".encode())
                peer_socket.close()
                print(f"Reply request '{request_type}' sent to Peer {next_peer.peer_id}.")
            else:
                print(f"Next peer {next_peer_id} not found.")
        except Exception as e:
            print(f"Error sending reply request to Peer {next_peer_id}: {e}")


def setup_peers(num_peers):
    peers = []
    roles = ['buyer', 'seller']
    products = ['fish', 'salt', 'boar']

    for i in range(num_peers):
        role = random.choice(roles)
        product = random.choice(products) if role == 'seller' else None
        peer = Peer(peer_id=i, role=role, product=product)
        peers.append(peer)

    for peer in peers:
        peer.neighbors = random.sample([p for p in peers if p != peer], min(len(peers)-1, 3))

    return peers

def run_peer(peer, host, port):
    threading.Thread(target=peer.listen_for_requests, args=(host, port)).start()

def setup_test_case():
    peer_0 = Peer(peer_id=0, role="buyer")
    peer_1 = Peer(peer_id=1, role="seller", product="salt")
    peer_2 = Peer(peer_id=2, role="seller", product="salt")
    peer_3 = Peer(peer_id=3, role="seller", product="fish")
    peer_4 = Peer(peer_id=4, role="seller", product="boar")

    peer_0.neighbors = [peer_1]
    peer_1.neighbors = [peer_0, peer_3]
    peer_3.neighbors = [peer_1]
    peer_2.neighbors = []
    peer_4.neighbors = []

    return [peer_0, peer_1, peer_2, peer_3, peer_4]
"""
if __name__ == "__main__":
    peers = setup_test_case()
    for i, peer in enumerate(peers):
        run_peer(peer, host='127.0.0.1', port=5000 + i)

    try:
        time.sleep(2)
        print(f"\nPeer 0 (buyer) is looking for 'fish'")
        peers[0].send_request('lookup', f'0,fish,3,[0]')
        time.sleep(10)
    except KeyboardInterrupt:
        print("Shutting down peers...")
        for peer in peers:
            # Optionally define close_all_sockets here or clean up if needed
            pass
        print("All peers shut down.")
"""
if __name__ == "__main__":
    num_peers = int(sys.argv[1]) if len(sys.argv) > 1 else 6  # Get the number of peers from command line or default to 6
    peers = setup_peers(num_peers)  # Set up peers

    # Run each peer in a separate thread to listen for requests
    for i, peer in enumerate(peers):
        run_peer(peer, host='127.0.0.1', port=5000 + i)  # Each peer listens on a unique port

    # Simulate buyers looking for products periodically
    while True:
        for peer in peers:
            if peer.role == 'buyer':
                # Randomly select a product to look for
                product = random.choice(['fish', 'salt', 'boar'])
                print(f"Peer {peer.peer_id} is looking for {product}")  # Log the lookup action
                peer.send_request('lookup', f'{peer.peer_id},{product},{peer.hop_limit},[{peer.peer_id}]')  # Send lookup request
                time.sleep(random.uniform(1, 3))  # Wait for a random period before the next lookup
