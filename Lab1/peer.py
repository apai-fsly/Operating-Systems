import threading
import socket
import logging

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
        logging.info(f"Peer {self.peer_id} listening on {host}:{port}")

        while True:
            client_socket, address = server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket,)).start()

    def handle_request(self, client_socket):
        try:
            request = client_socket.recv(1024).decode()
            
            request_type, data = request.split('|', 1)

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

        except Exception as e:
            logging.info(f"Error handling request: {e}")
        finally:
            client_socket.close()

    def handle_lookup(self, buyer_id,buytime, product_name, hopcount, search_path):
        if self.role == "seller" and self.product == product_name and self.stock > 0:
            seller_id = self.peer_id
            logging.info(f"Peer {self.peer_id} has found {product_name} via the following path of peers {search_path}")
            logging.info(f"Peer {self.peer_id} (seller) has {product_name}. Sending reply from Peer(seller):{seller_id} to Peer(buyer):{buyer_id} ")
            self.send_reply(buyer_id,buytime, seller_id, search_path)

        elif hopcount > 0:
            if self.peer_id not in search_path:
                search_path.append(self.peer_id)
                self.send_request("lookup", f"{buyer_id},{buytime},{product_name},{hopcount-1},{search_path}")

    def handle_buy(self, buyer_id,buytime, seller_id):
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
                # self.send_request("transaction_complete", f"{buyer_id},{seller.peer_id}")
            else:
                logging.info(f"Peer {seller.peer_id} is out of stock.")

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

    def send_reply_to_next_peer(self, buyer_id, buytime, reply_path, next_peer_id, seller_id):
        next_peer = Peer.peers_by_id.get(next_peer_id)
        if next_peer:
            logging.debug(f"Sending reply to next peer {next_peer.peer_id}")
            next_peer.send_reply_request("reply", f"{buyer_id},{buytime},{seller_id},{reply_path}", next_peer_id)
        else:
            logging.info(f"Next peer {next_peer_id} not found.")


    def send_request(self, request_type, data):
        for neighbor in self.neighbors:
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + neighbor.peer_id))
                peer_socket.send(f"{request_type}|{data}".encode())
                peer_socket.close()
            except Exception as e:
                logging.info(f"Error sending request to Peer {neighbor.peer_id}: {e}")

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