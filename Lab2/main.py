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

    for i in range(num_peers):
        role = random.choice(roles)
        product = random.choice(products) if role == 'seller' else None
        peer = Peer(peer_id=i, role=role, product=product)
        peers.append(peer)

    for peer in peers:
        peer.neighbors = random.sample([p for p in peers if p != peer], min(len(peers)-1, 3))

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
    # peer 0 is fish buyer and peer 3 is fish seller, can connect with seller with passing two middle peers 1 and 2
    peer_0 = Peer(peer_id=0, role="buyer", network_size=7, product="salt", leader=True)
    # 0 --> 1 --> 2 --> 3 --> 4
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
    # peer 0 is fish buyer and peer 3 is fish seller, can connect with seller with passing two middle peers 1 and 2
    peer_0 = Peer(peer_id=0, role="buyer", network_size=5, product="salt", leader=True)
    # 0 --> 1 --> 2 --> 3 --> 4
    peer_1 = Peer(peer_id=1, role="seller", network_size=5, product="salt", leader=True)
    peer_2 = Peer(peer_id=2, role="seller", network_size=5, product="boar", leader=True)
    peer_3 = Peer(peer_id=3, role="seller", network_size=5, product="fish", leader=True)
    peer_4 = Peer(peer_id=4, role="seller", network_size=5, product="boar", leader=True)

    peer_0.neighbors = [peer_1, peer_2, peer_3, peer_4]
    peer_1.neighbors = [peer_0, peer_2, peer_3, peer_4]
    peer_2.neighbors = [peer_0, peer_1, peer_3, peer_4]
    peer_3.neighbors = [peer_0, peer_1, peer_2, peer_4]
    peer_4.neighbors = [peer_0, peer_1, peer_2, peer_3]

    peer_4.alive = False

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
            print(f"row is {row}")
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
            higher_peer_id = []
            for peer in range(peers[0].network_size):
                print(f"peer is {peer} and {peers[0].peer_id}")
                if peer > peers[0].peer_id: 
                    higher_peer_id.append(peer)
                    print(f"peer id list {higher_peer_id}")
            
                    # all bigger peer IDs are in the list
            for peer in higher_peer_id: 
                peers[0].send_request_to_specific_id("are_you_alive", f"{peers[0].peer_id}", peer)

            time.sleep(10)
            for i in range(1000):
                election_flag = int(read_election_in_progress(leader_path))
                while election_flag:
                    print("waiting")
                    time.sleep(15)
                    election_flag = int(read_election_in_progress(leader_path))
                leader = read_leader_id(leader_path)
                print(f"Buy:: {peers[0].peer_id}, {leader}, {peers[0].product}")
                peers[0].send_request_to_specific_id("buy", f"{peers[0].peer_id},{leader},{peers[0].product},{peers[0].lamport_clock}", int(leader))
                time.sleep(1)
                print(f"Buy:: {peers[3].peer_id}, {leader}, {peers[3].product}")
                peers[3].send_request_to_specific_id("buy", f"{peers[3].peer_id},{leader},{peers[3].product},{peers[3].lamport_clock}", int(leader))
                # time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            shutdown_processes(processes)
            logging.info("All peers shut down.")

    elif mode == 'test_case2':
        logging.info("Running Test Case 2")
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
            higher_peer_id = []
            for peer in range(peers[0].network_size):
                print(f"peer is {peer} and {peers[0].peer_id}")
                if peer > peers[0].peer_id: 
                    higher_peer_id.append(peer)
                    print(f"peer id list {higher_peer_id}")
            
                    # all bigger peer IDs are in the list
            for peer in higher_peer_id: 
                peers[0].send_request_to_specific_id("are_you_alive", f"{peers[0].peer_id}", peer)

            time.sleep(10)
            for i in range(1000):
                election_flag = int(read_election_in_progress(leader_path))
                while election_flag:
                    print("waiting")
                    time.sleep(15)
                    election_flag = int(read_election_in_progress(leader_path))
                leader = read_leader_id(leader_path)
                print(f"Buy:: {peers[0].peer_id}, {leader}, {peers[0].product}")
                peers[0].send_request_to_specific_id("buy", f"{peers[0].peer_id},{leader},{peers[0].product},{peers[0].lamport_clock}", int(leader))
                # time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Shutting down peers...")
            shutdown_processes(processes)
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
    if mode == 'test_case4':
        logging.info("Running Test Case 4")

        peers = setup_test_case4()

        new_entry = {
            "seller_id": 1, 
            "product_name": "salt", 
            "product_stock": 10
        }

        new_entry2 = {
            "seller_id": 2, 
            "product_name": "fish", 
            "product_stock": 100
        }


        peers[0].handle_file_write(new_entry["seller_id"], new_entry["product_name"], new_entry["product_stock"])
        peers[1].handle_file_write(new_entry2["seller_id"], new_entry2["product_name"], new_entry2["product_stock"])

    if mode == 'test_case5':
        peers = setup_test_case5()

        peers[0].fall_sick()
    if mode == 'test_case6':

        # returns peers with peer_2 assigned as the leader/tradesman
        peers = setup_test_case6() 

        for i, peer in enumerate(peers):
            run_peer(peer, host='127.0.0.1', port=5000 + i)

        peer_0 = peers[0]
        peer_0.product = "salt"
        peer_1 = peers[1]
        peer_2 = peers[2]
        peer_3 = peers[3]

        #    peer_3
        #      |
        #    peer_2
        #   /      \
        # peer_0   peer_1
        buyer_id = peer_3.peer_id
        leader_id = peer_2.peer_id
        peer_0.send_request_to_specific_id("buy", f"{peers[0].peer_id},{peers[0].leader_id},{peers[0].product},{peers[0].lamport_clock}", int(peers[0].leader_id))
        time.sleep(3)
        peer_1.send_request_to_specific_id("buy", f"{peers[1].peer_id},{peers[1].leader_id},{peers[0].product},{peers[1].lamport_clock}", int(peers[1].leader_id))



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
        if 'processes' in locals():
            shutdown_processes(processes)
        logging.info("All peers shut down.")
        sys.exit(1)

