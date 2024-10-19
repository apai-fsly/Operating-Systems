import subprocess
import time
import os
import random

def start_peers(num_peers):
    """
    Starts the specified number of peers in separate processes.
    """
    processes = []
    for i in range(num_peers):
        process = subprocess.Popen(['python', 'main.py', str(num_peers)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(process)
        time.sleep(1)  # Give some time for the peers to start

    return processes

def stop_peers(processes):
    """
    Stops the running peer processes.
    """
    for process in processes:
        process.terminate()
        process.wait()

def simulate_buyers(num_buyers, products, wait_time):
    """
    Simulates buyers looking for products.
    """
    for _ in range(num_buyers):
        buyer_id = random.randint(0, num_buyers - 1)
        product = random.choice(products)
        print(f"Simulating Buyer {buyer_id} looking for {product}")
        time.sleep(wait_time)  # Wait between buyer actions

def main():
    num_peers = 6  # Number of peers
    num_buyers = 3  # Number of buyers to simulate
    products = ['fish', 'salt', 'boar']
    wait_time = 2  # Wait time between buyer actions

    # Start the peers
    processes = start_peers(num_peers)

    try:
        # Simulate buyers looking for products
        simulate_buyers(num_buyers, products, wait_time)

    finally:
        # Stop the peers
        stop_peers(processes)
        print("Stopped all peer processes.")

if __name__ == "__main__":
    main()
