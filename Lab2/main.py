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

# adding logging handler to easily get timestamps
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(message)s',
    datefmt='%H:%M:%S'
) 

current_directory = os.getcwd()

file_name = "seller_goods.csv"
file_path = os.path.join(current_directory, file_name)

leader_name = "leader.csv"
leader_path = os.path.join(current_directory, leader_name)

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

def setup_test_case1():
    peer_0 = Peer(peer_id=0, role="buyer", network_size=7, product="salt", leader=True)
    peer_1 = Peer(peer_id=1, role="seller", network_size=7, product="salt", leader=True)
    peer_2 = Peer(peer_id=2, role="seller", network_size=7, product="boar", leader=True)
    peer_3 = Peer(peer_id=3, role="buyer", network_size=7, product="fish", leader=True)
    peer_4 = Peer(peer_id=4, role="seller", network_size=7, product="fish", leader=True)
    peer_5 = Peer(peer_id=5, role="seller", network_size=7, product="salt", leader=True)
    peer_6 = Peer(peer_id=6, role="seller", network_size=7, product="boar", leader=True)


    peer_0.neighbors = [peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_1.neighbors = [peer_0, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_2.neighbors = [peer_0, peer_1, peer_3, peer_4, peer_5, peer_6]
    peer_3.neighbors = [peer_0, peer_1, peer_2, peer_4, peer_5, peer_6]
    peer_4.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_5, peer_6]
    peer_5.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_6]
    peer_6.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5]

    return [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]

def setup_test_case2():
    peer_0 = Peer(peer_id=0, role="buyer", network_size=5, product="salt", leader=True)
    peer_1 = Peer(peer_id=1, role="seller", network_size=5, product="salt", leader=True)
    peer_2 = Peer(peer_id=2, role="seller", network_size=5, product="boar", leader=True)
    peer_3 = Peer(peer_id=3, role="seller", network_size=5, product="fish", leader=True)
    peer_4 = Peer(peer_id=4, role="seller", network_size=5, product="boar", leader=True)

    peer_0.neighbors = [peer_1, peer_2, peer_3, peer_4]
    peer_1.neighbors = [peer_0, peer_2, peer_3, peer_4]
    peer_2.neighbors = [peer_0, peer_1, peer_3, peer_4]
    peer_3.neighbors = [peer_0, peer_1, peer_2, peer_4]
    peer_4.neighbors = [peer_0, peer_1, peer_2, peer_3]

    # peer_4.alive = False

    return [peer_0, peer_1, peer_2, peer_3, peer_4]

def setup_test_case3():
    peer_0 = Peer(peer_id=0, role="buyer", network_size=7, product="salt", leader=True)
    peer_1 = Peer(peer_id=1, role="seller", network_size=7, product="salt", leader=True)
    peer_2 = Peer(peer_id=2, role="seller", network_size=7, product="boar", leader=True)
    peer_3 = Peer(peer_id=3, role="buyer", network_size=7, product="salt", leader=True)
    peer_4 = Peer(peer_id=4, role="seller", network_size=7, product="fish", leader=True)
    peer_5 = Peer(peer_id=5, role="seller", network_size=7, product="salt", leader=True)
    peer_6 = Peer(peer_id=6, role="seller", network_size=7, product="boar", leader=True)


    peer_0.neighbors = [peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_1.neighbors = [peer_0, peer_2, peer_3, peer_4, peer_5, peer_6]
    peer_2.neighbors = [peer_0, peer_1, peer_3, peer_4, peer_5, peer_6]
    peer_3.neighbors = [peer_0, peer_1, peer_2, peer_4, peer_5, peer_6]
    peer_4.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_5, peer_6]
    peer_5.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_6]
    peer_6.neighbors = [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5]

    return [peer_0, peer_1, peer_2, peer_3, peer_4, peer_5, peer_6]

def setup_test_case4(): 
    peer_0 = Peer(peer_id=0, role="buyer", network_size=3, leader=True)
    peer_1 = Peer(peer_id=1, role="buyer", network_size=3, leader=True)
    peer_2 = Peer(peer_id=2, role="buyer", network_size=3, leader=True)

    return [peer_0, peer_1, peer_2]

def setup_test_case5(): 
    peer_0 = Peer(peer_id=0, role="buyer", network_size=2, leader=True)
    peer_1 = Peer(peer_id=1, role="seller", network_size=2, leader=True)

    return [peer_0, peer_1]

def setup_test_case6(): 
    peer_0 = Peer(peer_id=0, role="buyer", network_size=4, leader=False)
    peer_1 = Peer(peer_id=1, role="buyer", network_size=4, leader=False)
    peer_2 = Peer(peer_id=2, role="seller", network_size=4, leader=True)
    peer_3 = Peer(peer_id=3, role="seller", network_size=4, leader=False)

    peer_2.leader_id = peer_2.peer_id

    peer_1.leader_id = peer_2.peer_id
    peer_0.leader_id = peer_2.peer_id
    peer_3.leader_id = peer_2.peer_id

    return [peer_0, peer_1, peer_2, peer_3]

def read_leader_id(leader_path):
    try:
        with open(leader_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            # Read the first row and return the leader_id
            for row in reader:
                return row['leader_id']  # Return the first leader_id found
            
    except FileNotFoundError:
        print(f"Error: The file {leader_path} could not be found.")
    except IOError as e:
        print(f"IOError: {e}")
    
    return None  # Return None if no leader_id is found or an error occurs

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
    multiprocessing.set_start_method("fork", force=True)
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
            with open(file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                writer.writeheader()
            print("Clean up for seller_goods.csv done")
        except IOError as e:
            print(f"IOError: Could not update the file. {e}")

        try:
            time.sleep(1)
            print(f"start election node {peers[0].peer_id}")
            peers[0].send_request_to_specific_id("run_election", f"{peers[0].peer_id}", int(peers[0].peer_id))
            time.sleep(10)
            for i in range(1000):
                election_flag = int(read_election_in_progress(leader_path))
                while election_flag == 1 or election_flag == None:
                    print("waiting for election to complete...")
                    time.sleep(15)
                    election_flag = int(read_election_in_progress(leader_path))
                leader = read_leader_id(leader_path)
                peers[0].send_request_to_specific_id("buy", f"{peers[0].peer_id},{leader},{peers[0].product},{peers[0].lamport_clock}", int(leader))
                time.sleep(1)
                peers[3].send_request_to_specific_id("buy", f"{peers[3].peer_id},{leader},{peers[3].product},{peers[3].lamport_clock}", int(leader))
                # time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            shutdown_processes(processes)
            logging.info("All peers shut down.")

    elif mode == 'test_case2':
        logging.info("Running Test Case 2")
        now = datetime.datetime.now()
        peers = setup_test_case2()
        # processes = run_peers(peers)
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        try:
            with open(file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                writer.writeheader()
            print("Clean up for seller_goods.csv done")
        except IOError as e:
            print(f"IOError: Could not update the file. {e}")

        try:
            time.sleep(1)
            print(f"start election node {peers[0].peer_id}")
            peers[0].send_request_to_specific_id("run_election", f"{peers[0].peer_id}", int(peers[0].peer_id))
            

            time.sleep(10)
            for i in range(1000):
                election_flag = int(read_election_in_progress(leader_path))
                while election_flag == 1 or election_flag == None:
                    print("waiting for election to complete...")
                    time.sleep(1)
                    election_flag = int(read_election_in_progress(leader_path))
                leader = read_leader_id(leader_path)
                time.sleep(.05)
                peers[0].send_request_to_specific_id("buy", f"{peers[0].peer_id},{leader},{peers[0].product},{peers[0].lamport_clock}", int(leader))
                # time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            shutdown_processes(processes)
            logging.info("All peers shut down.")

    if mode == 'test_case3':
        peers = setup_test_case3()
        logging.info("Running Test Case 3")
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        try:
            with open(file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                writer.writeheader()
            print("Clean up for seller_goods.csv done")
        except IOError as e:
            print(f"IOError: Could not update the file. {e}")

        try:
            time.sleep(1)
            # start timer here
            print(f"start election node {peers[0].peer_id}")
            peers[0].send_request_to_specific_id("run_election", f"{peers[0].peer_id}", int(peers[0].peer_id))
            

            time.sleep(10)
            for i in range(1000):
                election_flag = int(read_election_in_progress(leader_path))
                while election_flag == 1 or election_flag == None:
                    print("waiting for election to complete...")
                    time.sleep(15)
                    election_flag = int(read_election_in_progress(leader_path))
                leader = read_leader_id(leader_path)
                peers[0].send_request_to_specific_id("buy", f"{peers[0].peer_id},{leader},{peers[0].product},{peers[0].lamport_clock}", int(leader))
                time.sleep(1)
                peers[3].send_request_to_specific_id("buy", f"{peers[3].peer_id},{leader},{peers[3].product},{peers[3].lamport_clock}", int(leader))
                # time.sleep(1)
            
            # end time

        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            shutdown_processes(processes)
            logging.info("All peers shut down.")
    elif mode == 'normal':
        logging.info("Running Normal Mode")
        
        # clear out the seller_goods.csv file
        try:
            with open(file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["seller_id", "product_name", "product_stock"])
                writer.writeheader()
            print("Clean up for seller_goods.csv done")
        except IOError as e:
            print(f"IOError: Could not update the file. {e}")

        num_peers = int(sys.argv[2]) if len(sys.argv) > 2 else 6  # Get the number of peers from command line or default to 6
        if num_peers < 3: 
            print("Network requires at least 3 peers to be a valid network")
            sys.exit(1)
        
        peers = setup_peers(num_peers)  # Set up peers

        # Run each peer in a separate thread to listen for requests
        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)  # Each peer listens on a unique port

        # # Simulate buyers looking for products periodically
        random_election_starter = random.randrange(0, num_peers-1)

        peers[random_election_starter].send_request_to_specific_id("run_election", f"{peers[random_election_starter].peer_id}", int(peers[random_election_starter].peer_id))
        time.sleep(10)
        leader = read_leader_id(leader_path)
        try:
            for x in range(1000):
                for i, peer in enumerate(peers):
                    election_flag = int(read_election_in_progress(leader_path))
                    while election_flag == 1 or election_flag == None:
                        logging.info("waiting for election to complete...")
                        time.sleep(10)
                        election_flag = int(read_election_in_progress(leader_path))
                    
                    leader = read_leader_id(leader_path)

                    if peer.role == "buyer" and peer.peer_id != leader:
                        logging.info(f"buy: {peers[i].peer_id}, {leader}, {peers[i].product}")
                        peer.send_request_to_specific_id("buy", f"{peers[i].peer_id},{leader},{peers[i].product},{peers[i].lamport_clock}", int(leader))

        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            for peer in peers:
                pass  # Clean up if needed
            logging.info("All peers shut down.")

    else:
        logging.error("Invalid mode. Please specify either 'test_case1', 'test_case2', 'test_case3' or 'normal'.")
        if 'processes' in locals():
            shutdown_processes(processes)
        logging.info("All peers shut down.")
        sys.exit(1)

