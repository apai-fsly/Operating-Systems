import datetime
import threading
import multiprocessing
import random
import time
import sys
import logging
import os
import csv
from peer import Peer
from warehouse import run_warehouse

# Configure number of trraderssby channginn this variable, default to two traders
Number_of_trader = 2

current_directory = os.getcwd()

processes = []

def setup_peers(num_peers):
    peers = []
    roles = ['buyer', 'seller']
    products = ['fish', 'salt', 'boar']

    # hardcoding network size of 3 to ensure
    # that there is at least 1 buyer and 1 seller. 
    if num_peers == 3: 
        role = random.choice(products)

        p1 = Peer(peer_id=0, role="seller", product=random.choice(products), network_size=3, leader=True)
        p2 = Peer(peer_id=1, role="buyer", product=random.choice(products), network_size=3, leader=True)
        
        role=random.choice(roles)
        product = random.choice(products)
        p3 = Peer(peer_id=2, role=role, product=product, network_size=3, leader=True)
        peers.append(p1, p2, p3)

    # if the network size is more than 3 randomize everything
    else:
        for i in range(num_peers):
            role = random.choice(roles)
            product = random.choice(products)
            peer = Peer(peer_id=i, role=role, product=product, network_size=num_peers)

            logging.info(f"{peer.peer_id}, {peer.role}, {peer.product}")
            peers.append(peer)


    # after the peers are created - each peer should be a neighbor of all other peers
    # this is needed for the bully election algorithm. 
    for peer in peers:
        peer.neighbors = [p for p in peers if p != peer]
        print(f"Peer {peer.peer_id}'s neighbors: {[neighbor.peer_id for neighbor in peer.neighbors]}")

    return peers

def run_peer(peer, host, port):
    p = multiprocessing.Process(target=peer.listen_for_requests, args=(host, port))
    p.daemon = True
    p.start()
    processes.append(p)

def shutdown_processes(processes):
    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join()

def setup_test_case1(use_caching):
    peer_0 = Peer(peer_id=0, role="buyer", network_size=7, product="salt", leader=False, use_caching=use_caching)
    peer_1 = Peer(peer_id=1, role="seller", network_size=7, product="fish", leader=False,  use_caching=use_caching)
    peer_2 = Peer(peer_id=2, role="seller", network_size=7, product="boar", leader=False,  use_caching=use_caching)
    peer_3 = Peer(peer_id=3, role="seller", network_size=7, product="boar", leader=True,  use_caching=use_caching)
    peer_4 = Peer(peer_id=4, role="buyer", network_size=7, product="fish", leader=False,  use_caching=use_caching)
    peer_5 = Peer(peer_id=5, role="seller", network_size=7, product="salt", leader=False,  use_caching=use_caching)
    peer_6 = Peer(peer_id=6, role="buyer", network_size=7, product="boar", leader=True,  use_caching=use_caching)


    peer_0.neighbors = [peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_1.neighbors = [peer_0, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_2.neighbors = [peer_0, peer_1, peer_3, peer_4, peer_5, peer_6]
    peer_3.neighbors = [peer_0, peer_1, peer_2, peer_4, peer_5, peer_6]
    peer_4.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_5, peer_6]
    peer_5.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_6]
    peer_6.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5]

    return [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]

def setup_test_case2(use_caching):
    peer_0 = Peer(peer_id=0, role="buyer", network_size=7, product="salt", leader=False, use_caching=use_caching)
    peer_1 = Peer(peer_id=1, role="seller", network_size=7, product="fish", leader=False, use_caching=use_caching)
    peer_2 = Peer(peer_id=2, role="seller", network_size=7, product="boar", leader=False, use_caching=use_caching)
    peer_3 = Peer(peer_id=3, role="seller", network_size=7, product="salt", leader=False, use_caching=use_caching)
    peer_4 = Peer(peer_id=4, role="buyer", network_size=7, product="fish", leader=False, use_caching=use_caching)
    peer_5 = Peer(peer_id=5, role="seller", network_size=7, product="salt", leader=False, use_caching=use_caching)
    peer_6 = Peer(peer_id=6, role="buyer", network_size=7, product="boar", leader=False, use_caching=use_caching)


    peer_0.neighbors = [peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_1.neighbors = [peer_0, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_2.neighbors = [peer_0, peer_1, peer_3, peer_4, peer_5, peer_6]
    peer_3.neighbors = [peer_0, peer_1, peer_2, peer_4, peer_5, peer_6]
    peer_4.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_5, peer_6]
    peer_5.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_6]
    peer_6.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5]

    return [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]

# def initialize_csv():
#     """Ensure the CSV file exists."""
#     if not os.path.exists(leader_path):
#         open(leader_path, mode='w').close()  # Create an empty file
#         print(f"{leader_path} initialized.")
#     else:
#         print(f"{leader_path} already exists.")

def clear_leaders():
    """Clear all leaders from the CSV file."""
    if os.path.exists(leader_path):
        open(leader_path, mode='w').close()  # Truncate the file
        print("All leaders cleared from the file.")
    else:
        print(f"{leader_path} does not exist.")

def read_election_in_progress(leader_path):
    try:
        # Open the file in read mode
        with open(leader_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            # Read the first (and only) row
            row = next(reader, None)
            if row:
                return row['election_in_progress']  # Return the election_in_progress field
            else:
                print("Error: No data found in the file.")
                return None
    
    except FileNotFoundError:
        print(f"Error: The file {leader_path} could not be found.")
    except IOError as e:
        print(f"IOError: {e}")
        return None

if __name__ == "__main__":
    # Check command-line arguments
    USE_CACHING = False
    multiprocessing.set_start_method("fork", force=True)
    if len(sys.argv) < 2:
        logging.error("Please specify 'test_case1' or 'normal' as an argument.")
        sys.exit(1)
    elif len(sys.argv) == 3:
        if sys.argv[2] == "use_caching":
            USE_CACHING = True

    mode = sys.argv[1].lower()
    if mode == 'test_case1':
        logging.info("Running Test Case 1")

        run_warehouse(host='127.0.0.1', port=8081)

        time.sleep(3)

        peers = setup_test_case1(use_caching=USE_CACHING)
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        Number_of_peer = 7

        print(f"start election node {peers[0].peer_id}")
        peers[0].send_request_to_specific_id("run_election", f"{Number_of_peer},{Number_of_trader}", int(peers[0].peer_id))

        try:
            time.sleep(1)
            while True:
                pass

        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            shutdown_processes(processes)
            logging.info("All peers shut down.")

    if mode == 'test_case2':
        logging.info("Running Test Case 2")
        run_warehouse(host='127.0.0.1', port=8081)

        peers = setup_test_case2(use_caching=USE_CACHING)
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        time.sleep(1)
        Number_of_peer = 7
        print(f"start election node {peers[0].peer_id}")
        peers[0].send_request_to_specific_id("run_election", f"{Number_of_peer},{Number_of_trader}", int(peers[0].peer_id))
        
        time.sleep(2)

        try:

            time.sleep(10)

            # Simulate trader1 failure this will call fall sick on one of the trader
            logging.info("[Simulation] Simulating Trader1 failure.")
            peers[0].send_request_to_specific_id("fall_sick", f"", 0)

            while True:
                pass
        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            clear_leaders()
            shutdown_processes(processes)
            logging.info("All peers shut down.")

    else:
        logging.error("Invalid mode. Please specify either 'test_case1', 'test_case2', 'test_case3' or 'normal'.")
        if 'processes' in locals():
            shutdown_processes(processes)
        logging.info("All peers shut down.")
        sys.exit(1)
    


