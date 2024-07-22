import argparse
import threading
import socket
import time
import random
import csv
from queue import Queue  

class Broker:
    def __init__(self, host, port):
        self.markets = []
        self.clients = []
        self.connected_clients = []
        self.market_ready_count = 0
        self.active = False
        self.lock = threading.Lock()
        self.pairs = [
            "BRENTCMDUSD",
            "BTCUSD",
            "EURUSD",
            "GBPUSD",
            "USA30IDXUSD",
            "USA500IDXUSD",
            "USATECHIDXUSD",
            "XAGUSD",
            "XAUUSD"
        ]
        self.period = "D1"
        self.host = host
        self.port = port
        self.server_socket = None
        self.max_markets = 9
        self.data_file = "market_data.csv"
        self.historical_data_buffer = Queue()
        self.first_client = None  
        self.historical_data_sent = False
        self.realtime_data_enabled = False
        self.first_client_connected = False

    def load_historical_data_to_buffer(self):
        with open(self.data_file, 'r') as file:
            for line in file:
                self.historical_data_buffer.put(line)
    
    """ def send_historical_data_to_clients(self, client_socket):
        with open(self.data_file, 'r') as file:
            for line in file:
                historical_data = line
                try:
                    client_socket.send(f"HISTORICAL_DATA {historical_data} ".encode())
                    time.sleep(0.1)
                except Exception as e:
                    print(f"Error al enviar datos históricos al cliente: {str(e)}") """

    def handle_market_connection(self, market_socket):
        with self.lock:
            if len(self.markets) < self.max_markets:
                self.markets.append(market_socket)
                market_socket.send("MARKET_READY_ACK".encode())
                time.sleep(0.1)
            else:
                market_socket.send("MAX_MARKETS_REACHED".encode())

    def connect_market(self, market_socket):
        print("Conectado al mercado")
        with self.lock:
            self.markets.append(market_socket)
            market_socket.send("MARKET_READY_ACK".encode())
            time.sleep(0.1)

    def connect_client(self, client_socket):
        print("Conectado al cliente")
        with self.lock:
            self.clients.append(client_socket)
            if not self.first_client:
                # Si este es el primer cliente, asignarlo como primer cliente
                self.first_client = client_socket
                self.connected_clients.append(client_socket)
                client_socket.send("CLIENT_READY_ACK".encode())
            else:
                # Clientes posteriores solo recibirán datos históricos
                self.connected_clients.append(client_socket)
                client_socket.send("CLIENT_READY_ACK".encode())

    def assign_pairs_and_period_to_markets(self, market_socket):
        if len(self.pairs) > 0:
                random.shuffle(self.pairs)
                selected_pair = self.pairs.pop(0)
                market_socket.send(f"PAIR {selected_pair}".encode())
                market_socket.send(f"PERIOD {self.period}".encode())

    def start_simulation(self, market_socket):
        print("Comienza simulación")
        with self.lock:
            if len(self.markets) >= 1:
                self.assign_pairs_and_period_to_markets(market_socket)

        while len(self.connected_clients) == 0:
            time.sleep(1)

        for market_socket in self.markets:
            market_socket.send("START_SIMULATION_ACK".encode())

    def receive_market_data(self, data):
        with self.lock:
            for client in self.connected_clients:
                try:
                    if client == self.first_client:
                        client.send(f"MARKET_DATA {data['date']} {data['timeframe']} {data['pair']} {data['open']} {data['high']} {data['low']} {data['close']} {data['volume']} ".encode())

                        with open(self.data_file, mode='a', newline='') as file:
                            writer = csv.writer(file, delimiter=' ', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                            writer.writerow([data['date'], data['timeframe'], data['pair'], data['open'], data['high'], data['low'], data['close'], data['volume']])
                    else:
                        with open(self.data_file, 'r') as file:
                            for line in file:
                                historical_data = line
                                try:
                                    client.send(f"HISTORICAL_DATA {historical_data} ".encode())
                                    time.sleep(0.1)
                                except Exception as e:
                                    print(f"Error al enviar datos históricos al cliente: {str(e)}")
                    time.sleep(0.1)
                except Exception as e:
                    print(f"Error al enviar datos al cliente: {str(e)}")
            if not self.first_client_connected:
                self.first_client_connected = True

    def run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1000)

        print(f"Broker listening on {self.host}:{self.port}")

        self.load_historical_data_to_buffer()

        try:
            while True:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(target=self.handle_client, args=(client_socket,)).start()
        except KeyboardInterrupt:
            pass

    def handle_client(self, client_socket):
        with client_socket:
            try:
                while True:
                    data = client_socket.recv(32767)
                    if not data:
                        break
                    data = data.decode()
                    #print(data)
                    if data == "MARKET_READY":
                        self.handle_market_connection(client_socket)
                        self.connect_market(client_socket)
                    elif data == "CLIENT_READY":
                        self.connect_client(client_socket)
                        self.start_simulation(client_socket)
                    elif data == "START_SIMULATION":
                        self.start_simulation(client_socket)
                    elif data.startswith("MARKET_DATA"):
                        _, date, timeframe, pair, open, high, low, close, volume = data.split()
                        self.receive_market_data({
                            'date': date,
                            'timeframe': timeframe,
                            'pair': pair,
                            'open': open,
                            'high': high,
                            'low': low,
                            'close': close,
                            'volume': volume
                        })

            except Exception as e:
                print(f"Error en la comunicación con el cliente: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", type=str, default="localhost", help="Broker host")
    parser.add_argument("--broker-port", type=int, default=54321, help="Broker port")
    args = parser.parse_args()

    broker = Broker(args.broker_host, args.broker_port)
    broker.run()
