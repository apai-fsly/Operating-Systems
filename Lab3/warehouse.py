import multiprocessing
import threading
import time
import pandas as pd
import socket

csv_lock = {
    "salt.csv": threading.Lock(),
    "fish.csv": threading.Lock(),
    "boar.csv": threading.Lock(),
}

def run_warehouse(host, port): 
    p = multiprocessing.Process(target=server_function, args=(host, port))
    p.daemon = True
    p.start()
    # p.join()

def main():
    # Dictionary to define products and initial stock values
   
    run_warehouse("127.0.0.1", 8081)


    # Example of starting a multiprocessing process
    # server = multiprocessing.Process(target=server_function)
    # server.start()
    # server.join()

def server_function(host="127.0.0.1", port=8081):

    productDictionary = {
        "salt": 0,
        "boar": 0,
        "fish": 0,
    }


    generate_csv(productDictionary=productDictionary)

    print("completed creation of CSV file")


    print(f"database server starting on {host}:{port}...")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    # listen on the port indefinetly for requests
    while True:
        #start a thread that polls for incoming requests via the handle_request function. 
        client_socket, address = server_socket.accept()
        threading.Thread(target=handle_request_warehouse, args=(client_socket,)).start()
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    #     server_socket.bind((host, port))
    #     server_socket.listen(5)  # Allow up to 5 simultaneous connections
    #     print(f"Server listening on {host}:{port}.")
    #     try: 
    #         while True:

    #             threading.Thread(target=handle_request, args=(client_socket,)).start()
    #             conn, addr = server_socket.accept()
    #             print(f"Connection received from {addr}.")
    #             # Keep the connection open for multiple messages
    #             with conn:
    #                 data = conn.recv(1024)
    #                 if not data:
    #                     print(f"Connection closed by {addr}.")
    #                     break  # Exit the inner loop if client disconnects

    #                 message = data.decode().strip()
    #                 print(f"Received: {data.decode()}")

    #                 action, product, value = parse_message(msg=message)

    #                 msg = handle_action(action, product, value)

    #                 conn.sendall((msg+'\n').encode())  # Echo back the received data
    #     except KeyboardInterrupt:
    #         print("server shutting down....")
    #         server_socket.close()
    #         time.sleep(5)


def handle_request_warehouse(client_socket): 
    print("inside handle_request_warehouse")
    try:
        request = client_socket.recv(1024).decode()
        request_type, data = request.split('|', 1) # seperates the request body into the request_type, and data on the |
        if request_type == "decrement":
            product, value, buyer_id, trader_id = data.split(",")
            value = int(value)
            resp = decrement(product=product, value=value)
            if resp != "out_of_stock": 
                print(f"{product} was successfully decremented by {value} at {time.time()}\n")
            else: 
                # respond back to trader
                send_request_to_trader("out_of_stock", f"{buyer_id},{product}", eval(trader_id))
        elif request_type == "increment": 
            product, value = data.split(",")
            value = int(value)
            print("detected increment call")
            restock(product=product, value=value)
            print(f"{product} was successfully restocked with value {value} at {time.time()}\n")
        else: 
            print(f"action {request_type} is currently unsupported")
    except: 
        None
    finally:
        client_socket.close()

def send_request_to_trader(request_type, data, trader_id): 
    try:
        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.connect(('127.0.0.1', 5000 + trader_id))
        peer_socket.send(f"{request_type}|{data}".encode())
        peer_socket.close()
    except Exception as e:
        print(f"Error sending request to Peer {trader_id}: {e}")



def handle_action(action, product, value): 
    if action == "decrement":
        decrement(product=product, value=value)
        return f"{product} was successfully decremented by {value} at {time.time()}\n"
    elif action == "increment": 
        restock(product=product, value=value)
        return f"{product} was successfully restocked with value {value} at {time.time()}\n"
    else: 
        return f"action {action} is currently unsupported"

def worker_function(name):
    print(f"Worker {name} is running.\n")

# generate_csv will create the warehouse files detailing the stock per item
def generate_csv(productDictionary): 
     for product, stock in productDictionary.items():
        # Create a new file for each product
        csvFilePath = f"{product}.csv"

        # Initialize the CSV file with headers if it doesn't exist or is empty
        try:
            df = pd.read_csv(csvFilePath)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            df = pd.DataFrame(columns=["stock"])
            df.to_csv(csvFilePath, index=False)

        # Add or update the stock value
        row_index = 0  # Row index
        df.at[row_index, "stock"] = stock
        df.to_csv(csvFilePath, index=False)


def decrement(product, value): 
        csvFilePath = f"{product}.csv"

        # here we want to take a lock on the file
        with csv_lock[product+".csv"]:
            print(f"{product} file is currently locked")
            # Initialize the CSV file with headers if it doesn't exist or is empty
            try:
                df = pd.read_csv(csvFilePath)
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = pd.DataFrame(columns=["stock"])
                df.to_csv(csvFilePath, index=False)

            # Add or update the stock value
            row_index = 0  # Row index
            stock = df.at[row_index, "stock"]

            if stock < value or stock == 0: 
                return "out_of_stock"

            df.at[row_index, "stock"] = stock - value
            df.to_csv(csvFilePath, index=False)
        

def restock(product, value): 
    csvFilePath = f"{product}.csv"

    # Initialize the CSV file with headers if it doesn't exist or is empty
    with csv_lock[product+".csv"]:
        print(f"{product} file is currently locked")
        try:
            df = pd.read_csv(csvFilePath)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            df = pd.DataFrame(columns=["stock"])
            df.to_csv(csvFilePath, index=False)
        
        # Add or update the stock value
        row_index = 0  # Row index
        stock = df.at[row_index, "stock"]
        df.at[row_index, "stock"] = stock + value
        df.to_csv(csvFilePath, index=False)




if __name__ == "__main__":
    main()
