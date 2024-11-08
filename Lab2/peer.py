import threading
import socket
import logging
import time

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
        self.leader = -1
        self.peer_id = peer_id
        self.role = role 
        self.network_size = network_size
        self.product = product
        self.neighbors = neighbors or []
        self.lock = threading.Lock()
        self.stock = 10 if role == "seller" else 0 # if the role is seller set the stock to 10 otherwise 0
        self.hop_limit = 3
        Peer.peers_by_id[self.peer_id] = self


    def elect(self): 
        # send a message to every node with a greater ID. 

        self.peer_id # 0
        higher_peer_id = []
        for peer in range(self.network_size): 
            if peer > self.peer_id: 
                higher_peer_id.append(peer)

        # all bigger peer IDs are in the list
        for peer in range(higher_peer_id): 
            self.send_request()
            time.sleep(3)




        request = client_socket.recv(1024).decode()

            # wait for a reply from peers

        



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
            elif request_type == "elect": 
                sender_id = data
                self.handle_election(sender_id)

        except Exception as e:
            logging.info(f"Error handling request: {e}")
        finally:
            client_socket.close() #close the socket after the connection.

    
    def send_elect_reply(self, sender_id): 
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect(('127.0.0.1', 5000 + sender_id))
            peer_socket.send(f"ok".encode())
            peer_socket.close()
        except Exception as e:
            logging.info(f"Error responing back to election intiator {sender_id}: {e}")

       


    """
    
    """
    def handle_election(self, sender_id): 
        # check the status of the peer before responding
        self.send_elect_reply(sender_id)


    
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
            try:
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.connect(('127.0.0.1', 5000 + neighbor.peer_id))
                peer_socket.send(f"{request_type}|{data}".encode())
                peer_socket.close()
            except Exception as e:
                logging.info(f"Error sending request to Peer {neighbor.peer_id}: {e}")

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