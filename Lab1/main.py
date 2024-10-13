import sys
import socket
import threading

DEFAULT_NUM_PEERS = 3

# Peer class defines how a peer is setup
# port: defines the port the peer can be reached at
# neighbors: defines the list of ports of neighboring peers
# running: defines the running state of the peer
class Peer:
    def __init__(self, port, neighbors):
        self.port = port
        self.neighbors = neighbors
        self.running = True
        
        # Start the listening thread
        self.listener_thread = threading.Thread(target=self.listen)
        self.listener_thread.start()

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', self.port))
            s.listen()
            print(f'Peer listening on port {self.port}...')
            while self.running:
                conn, addr = s.accept()
                with conn:
                    data = conn.recv(1024)
                    if data:
                        print(f'Peer {self.port} received: {data.decode()}')

    def send_message(self, message):
        for neighbor in self.neighbors:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', neighbor))
                    s.sendall(message.encode())
            except Exception as e:
                print(f"Failed to send message to {neighbor}: {e}")

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
        peers.append(Peer(ports[i], neighbors))

    try:
        while True:
            message = input("Enter message to send to neighbors (or type 'exit' to quit): \n")
            if message.lower() == 'exit':
                break
            # Send the message from the first peer as an example
            peers[0].send_message(message)
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
