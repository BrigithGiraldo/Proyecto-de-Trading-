import argparse
import socket
import threading
import time
import csv

class Market(threading.Thread):
    def __init__(self, broker_host, broker_port):
        super().__init__()
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.pair = ""
        self.period = ""

    def connect_to_broker(self):
        print("mercado conectado")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((self.broker_host, self.broker_port))

    def run(self):
        print("mercado corriendo")
        self.connect_to_broker()

        self.server_socket.send("MARKET_READY".encode())

        time.sleep(0.1)

        self.server_socket.send("START_SIMULATION".encode())
        
        self.receive_pair_and_period()
        
        self.server_socket.send("PAIR_AND_PERIOD_RECEIVED".encode())

        self.start_simulation()

    def receive_pair_and_period(self):
        pair_received = False
        period_received = False

        while not (pair_received and period_received):
            data = self.server_socket.recv(32767).decode()
            #print(data)

            if data.startswith("PAIR"):
                _, pair = data.split()
                self.pair = pair
                self.server_socket.send("PAIR_RECEIVED".encode())
                pair_received = True
                print(f"Se recibe el par del Broker {self.pair}")
            elif data.startswith("PERIOD"):
                _, period = data.split()
                self.period = period
                self.server_socket.send("PERIOD_RECEIVED".encode())
                period_received = True
                print(f"Se recibe el período del Broker {self.period}")

    def start_simulation(self):
        print("mercado inicio simulación")
        file_name = self.generate_file_name()
        data = self.read_data_from_file(file_name)

        start_signal = self.server_socket.recv(32767).decode()
        print(start_signal)
        if start_signal != "START_SIMULATION_ACK":
            print("Error: No se recibió la señal de inicio adecuada del broker.")
            return

        for candle in data:
            self.send_data_to_broker(candle)
            time.sleep(1)

        self.send_data_to_broker(None)

    def generate_file_name(self):
        #print("entra generate file")
        file_extension = "csv"
        file_name = f"{self.pair.upper()}_{self.period.upper()}.{file_extension}"
        return file_name

    def read_data_from_file(self, file_name):
        data = []
        try:
            if file_name.endswith('.csv'):
                with open(file_name, 'r') as file:
                    data_reader = csv.reader(file, delimiter=",")
                    next(data_reader, None)
                    for row in data_reader:
                        if len(row) >= 6:
                            date = row[0]
                            highest = row[2]
                            lowest = row[3]
                            start = row[1]
                            end = row[4]
                            volume = row[5]
                            data.append((date, start, highest, lowest, end, volume))
                    #print(data)
        except Exception as e:
            print(f"Error al leer el archivo: {e}")
            #print(data)
        return data
    

    def send_data_to_broker(self, candle):
        if candle is None:
            self.server_socket.send("SIMULATION_COMPLETE".encode())
        else:
            message = f"MARKET_DATA {candle[0]} {self.pair} {candle[1]} {candle[2]} {candle[3]} {candle[4]} {candle[5]}"
            self.server_socket.send(message.encode())
            time.sleep(0.0001)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", type=str, default="localhost", help="Broker host")
    parser.add_argument("--broker-port", type=int, default=5000, help="Broker port")
    parser.add_argument("--pair", type=str, help="Par de monedas")
    parser.add_argument("--period", type=str, help="Periodo de tiempo")
    args = parser.parse_args()
    
    market = Market(args.broker_host, args.broker_port)
    market.start() 
    market.join()


