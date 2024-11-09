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

def setup_test_case1():
    # peer 0 is salt buyer and peer 1 is salt seller, direct connection
    peer_0 = Peer(peer_id=0, role="buyer", network_size=3)
    # 0 --> 1 --> 2 --> 3 --> 4
    peer_1 = Peer(peer_id=1, role="seller", network_size=3, product="salt")
    peer_2 = Peer(peer_id=2, role="seller", network_size=3, product="boar")


    peer_0.neighbors = [peer_1, peer_2]
    peer_1.neighbors = [peer_0, peer_2]
    peer_2.neighbors = [peer_0, peer_1]

    return [peer_0, peer_1, peer_2]

def setup_test_case2():
    # peer 0 is fish buyer and peer 3 is fish seller, can connect with seller with passing two middle peers 1 and 2
    peer_0 = Peer(peer_id=0, role="buyer")
    # 0 --> 1 --> 2 --> 3 --> 4
    peer_1 = Peer(peer_id=1, role="seller", product="salt")
    peer_2 = Peer(peer_id=2, role="seller", product="boar")
    peer_3 = Peer(peer_id=3, role="seller", product="fish")
    peer_4 = Peer(peer_id=4, role="seller", product="boar")

    peer_0.neighbors = [peer_1]
    peer_1.neighbors = [peer_0, peer_2]
    peer_2.neighbors = [peer_3, peer_1]
    peer_3.neighbors = [peer_2, peer_4]
    peer_4.neighbors = [peer_3]

    return [peer_0, peer_1, peer_2, peer_3, peer_4]

def setup_test_case3():
    # peer 0, 1 and 2 are salt buyer and peer 3 is salt seller, all three buyers are trying to buy from single seller
    peer_0 = Peer(peer_id=0, role="buyer")
    peer_1 = Peer(peer_id=1, role="buyer")
    peer_2 = Peer(peer_id=2, role="buyer")
    # 0 --> 3
    # 1 --> 3
    # 2 --> 3
    peer_3 = Peer(peer_id=3, role="seller", product="salt")
    peer_4 = Peer(peer_id=4, role="seller", product="salt")
    peer_0.neighbors = [peer_3]
    peer_1.neighbors = [peer_3]
    peer_2.neighbors = [peer_3]
    peer_3.neighbors = [peer_0, peer_1, peer_2]
    peer_4.neighbors = []

    return [peer_0, peer_1, peer_2, peer_3, peer_4]


if __name__ == "__main__":
    # Check command-line arguments
    if len(sys.argv) < 2:
        logging.error("Please specify 'test_case1' or 'normal' as an argument.")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == 'test_case1':
        logging.info("Running Test Case 1")
        peers = setup_test_case1()
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        try:
            time.sleep(2)
            print(f"start election node {peers[0].peer_id}")
            higher_peer_id = []
            for peer in range(peers[0].network_size):
                print(f"peer is {peer} and {peers[0].peer_id}")
                if peer > peers[0].peer_id: 
                    higher_peer_id.append(peer)
                    print(f"peer id list {higher_peer_id}")

                    # all bigger peer IDs are in the list
            for peer in higher_peer_id: 
                peers[0].send_request_to_specific_id("are_you_alive", f"{peers[0].peer_id}", peer)
            # for i in range(10): 
            #     logging.info(f"Peer 0 (buyer) is looking for 'salt'")
            #     buytime = time.time()
                # peers[0].send_request('lookup', f'0,{buytime},salt,1,[0]')
                
        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            for peer in peers:
                pass  # Clean up if needed
            logging.info("All peers shut down.")
    elif mode == 'test_case2':
        logging.info("Running Test Case 2")
        peers = setup_test_case2()
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        try:
            time.sleep(2)
            for i in range(10): 
                logging.info(f"Peer 0 (buyer) is looking for 'fish'")
                buytime = time.time()
                peers[0].send_request('lookup', f'0,{buytime},fish,3,[0]')
        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            for peer in peers:
                pass  # Clean up if needed
            logging.info("All peers shut down.")

    if mode == 'test_case3':
        logging.info("Running Test Case 3")
        peers = setup_test_case3()
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        try:
            time.sleep(2)
            for i in range(1000): 
                logging.info(f"Peer 0 (buyer) is looking for 'salt'")
                buytime = time.time()
                peers[0].send_request('lookup', f'0,{buytime},salt,3,[0]')
                peers[1].send_request('lookup', f'1,{buytime},salt,3,[1]')
                peers[2].send_request('lookup', f'2,{buytime},salt,3,[2]')
        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            for peer in peers:
                pass  # Clean up if needed
            logging.info("All peers shut down.")

    elif mode == 'normal':
        logging.info("Running Normal Mode")
        num_peers = int(sys.argv[2]) if len(sys.argv) > 2 else 6  # Get the number of peers from command line or default to 6
        peers = setup_peers(num_peers)  # Set up peers

        # Run each peer in a separate thread to listen for requests
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)  # Each peer listens on a unique port

        # Simulate buyers looking for products periodically
        try:
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
                        logging.info(f"Peer {peer.peer_id} is being restocked with {peer.stock} {peer.product}")
        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            for peer in peers:
                pass  # Clean up if needed
            logging.info("All peers shut down.")

    else:
        logging.error("Invalid mode. Please specify either 'test_case1', 'test_case2', 'test_case3' or 'normal'.")
        sys.exit(1)