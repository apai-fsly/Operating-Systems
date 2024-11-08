import json
import os
import socket
import threading
import random
import time
import sys
import logging
from peer import Peer

# adding logging handler to easily get timestamps
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(message)s',
    datefmt='%H:%M:%S'
)

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




# global for number of nodes in network    
number_of_nodes = 3
def setup_test_case():
    peer_0 = Peer(peer_id=0, role="buyer")
    peer_1 = Peer(peer_id=1, role="buyer")
    peer_2 = Peer(peer_id=2, role="buyer")

    # 0 --> 3
    # 1 --> 3
    # 2 --> 3

    # seller <--> leader <--> buyer
    peer_0.neighbors = [peer_1, peer_2]
    peer_1.neighbors = [peer_0, peer_2]
    peer_2.neighbors = [peer_0, peer_1]

    return [peer_0, peer_1, peer_2]

if __name__ == "__main__":
    peers = setup_test_case()
    for i, peer in enumerate(peers):
        run_peer(peer, host='127.0.0.1', port=5000 + i)

    try:
        time.sleep(2)
        for i in range(1000): 
            logging.info(f"\nPeer 0 (buyer) is looking for 'salt'")
            buytime = time.time()
            peers[0].send_request('lookup', f'0,{buytime},salt,3,[0]')
    except KeyboardInterrupt:
        logging.info("Shutting down peers...")
        for peer in peers:
            # Optionally define close_all_sockets here or clean up if needed
            pass
        logging.info("All peers shut down.")
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
                logging.info(f"Peer {peer.peer_id} is looking for {product}")  # Log the lookup action
                buytime = time.time()
                peer.send_request('lookup', f'{peer.peer_id},{buytime},{product},{peer.hop_limit},[{peer.peer_id}]')  # Send lookup request
                time.sleep(random.uniform(1, 3))  # Wait for a random period before the next lookup

            elif peer.role == 'seller' and peer.stock == 0:
                peer.stock = 10
                peer.product = random.choice(["boar", "salt", "fish"])
                logging.info(f"Peer {peer.peer_id} is being restocked with {peer.stock} {peer.product} ")
"""