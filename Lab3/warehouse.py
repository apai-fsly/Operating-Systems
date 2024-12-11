import multiprocessing
import os
import sys
import threading
import time
import pandas as pd
import socket
import logging

csv_lock = {
    "salt.csv": threading.Lock(),
    "fish.csv": threading.Lock(),
    "boar.csv": threading.Lock(),
}

def setup_logger():
    logger = logging.getLogger("WarehouseLogger")
    logger.setLevel(logging.INFO)  # Set the logging level

    # Create a file handler for logging to warehouse.txt
    file_handler = logging.FileHandler("warehouse.txt")
    file_handler.setLevel(logging.INFO)

    # Create a stream handler (for terminal output)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    # Define a common format for both handlers
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d - %(message)s",
        datefmt="%H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Add handlers to the logger
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
    
    logger.propagate = False
    
    return logger

logger = setup_logger()

# Runs ware house as a separate process
def run_warehouse(host, port): 
    p = multiprocessing.Process(target=server_function, args=(host, port))
    p.daemon = True
    p.start()

def server_function(host="127.0.0.1", port=8081):

    productDictionary = {
        "salt": 0,
        "boar": 0,
        "fish": 0,
    }


    generate_csv(productDictionary=productDictionary)
    generate_warehouse_txt()

    logger.info(f"database server starting on {host}:{port}...")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    # listen on the port indefinetly for requests
    while True:
        #start a thread that polls for incoming requests via the handle_request function. 
        client_socket, address = server_socket.accept()
        threading.Thread(target=handle_request_warehouse, args=(client_socket, logger, )).start()

# This function handles incoming requests to the warehouse 
def handle_request_warehouse(client_socket, logger): 
    try:
        request = client_socket.recv(1024).decode()
        request_type, data = request.split('|', 1) # seperates the request body into the request_type, and data on the |
        if request_type == "decrement":
            product, value, buyer_id, trader_id, reqTime = data.split(",")
            value = int(value)
            resp = decrement(product=product, value=value)
            if resp != "out_of_stock": 
                logger.info(f"warehouse successfully decremented {product} stock by {value} for trader_peer {trader_id}")
                send_request_to_trader("purchase_success", f"{buyer_id},{product},{reqTime}", eval(trader_id))
            else: 
                # respond back to trader
                send_request_to_trader("out_of_stock", f"{buyer_id},{product},{reqTime}", eval(trader_id))
                logger.info(f"warehouse failed to decrement {product} stock by {value} for trader_peer {trader_id} as it was out of stock")
        elif request_type == "increment": 
            product, value = data.split(",")
            value = int(value)
            restock(product=product, value=value)
            logger.info(f"warehouse successfully incremented {product} stock by {value} for trader_peer {trader_id}")
        elif request_type == "load_cache": 
            trader_id = data
            logger.info(f"trader {trader_id} is requesting to update their cache!")
            salt = get_inventory("salt")
            boar = get_inventory("boar")
            fish = get_inventory("fish")
            send_request_to_trader("update_cache", f"salt,{salt},boar,{boar},fish,{fish}", eval(trader_id))

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
        logger.info(f"Error sending request to Peer {trader_id}: {e}")


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
    
     print("completed creation of CSV file")


def generate_warehouse_txt():
    file_name = "warehouse.txt"
    if os.path.exists(file_name):
    # If file exists, clear its content by opening it in write mode
        with open(file_name, "w") as file:
            pass  # File is opened in write mode to clear its content
            print(f"{file_name} already exists and has been cleared.")
    else:
        # If file does not exist, create it
        with open(file_name, "w") as file:
            pass  # Empty file is created
        print(f"{file_name} has been created.")

def get_inventory(product): 
    # here we want to take a lock on the file
    csvFilePath = f"{product}.csv"
    with csv_lock[product+".csv"]:
        # Initialize the CSV file with headers if it doesn't exist or is empty
        try:
            df = pd.read_csv(csvFilePath)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            df = pd.DataFrame(columns=["stock"])
            df.to_csv(csvFilePath, index=False)

        # Add or update the stock value
        row_index = 0  # Row index
        stock = df.at[row_index, "stock"]
        stock_str = str(stock)
        return stock_str
    

def decrement(product, value): 
        csvFilePath = f"{product}.csv"

        # here we want to take a lock on the file
        with csv_lock[product+".csv"]:
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
