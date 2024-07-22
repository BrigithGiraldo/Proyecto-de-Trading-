import socket
import matplotlib.pyplot as plt
import mplfinance as mpf
from datetime import datetime
import pandas as pd
from collections import deque

class Client:
    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.data = [deque(maxlen=1000000) for _ in range(9)]
        self.fig, self.axes = plt.subplots(3, 3, figsize=(12, 8))
        self.market_titles = ['Mercado 1', 'Mercado 2', 'Mercado 3', 'Mercado 4', 'Mercado 5', 'Mercado 6', 'Mercado 7', 'Mercado 8', 'Mercado 9']
        self.data_dict = {}
        self.desired_pairs = self.get_desired_pairs() 

    def connect_to_broker(self):
        print("Conectado al broker")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((self.broker_host, self.broker_port))

    def get_desired_pairs(self):
        print("Opciones de pares de monedas:")
        print("1. Ver todos los pares")
        print("2. Seleccionar pares específicos")
        choice = input("Seleccione una opción (1/2): ")
        
        if choice == '1':
            return ["ALL"] 
        elif choice == '2':
            num_pairs = int(input("Ingrese cuántos pares de monedas desea ver: "))
            pairs = []
            for i in range(num_pairs):
                pair = input(f"Par de monedas:\nBRENTCMDUSD\nBTCUSD\nEURUSD\nGBPUSD\nUSA30IDXUSD\nUSA500IDXUSD\nUSATECHIDXUSD\nXAGUSD\nXAUUSD\nIngrese el nombre del par de monedas #{i+1} (por ejemplo, EURUSD): ")
                pairs.append(pair)
            return pairs
        else:
            print("Opción no válida. Seleccionando todos los pares.")
            return ["ALL"]

    def start_simulation(self):
        print(f"Simulación comenzada para los pares de monedas: {', '.join(self.desired_pairs)}")
        self.server_socket.send("CLIENT_READY".encode())
        plt.ion()
        current_market_index = None

        for i, title in enumerate(self.market_titles):
            ax = self.axes[i // 3, i % 3]
            ax.set_title(title)

        self.fig.suptitle(f"Gráficas de Velas de Mercados ({', '.join(self.desired_pairs)})")

        # Agregar un bucle para manejar datos históricos
        historical_data = []
        while True:
            try:
                serialized_data = self.server_socket.recv(32767).decode()
                if not serialized_data:
                    break
                data = serialized_data.split()
                print(data)
                if data[0] == "MARKET_DATA":
                    if len(data) >= 9:
                        received_pair = data[3]

                        if "ALL" in self.desired_pairs or received_pair in self.desired_pairs:
                            datetime_str = data[1] + ' ' + data[2]
                            datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                            pair = data[3]
                            open_price = float(data[4])
                            high_price = float(data[5])
                            low_price = float(data[6])
                            close_price = float(data[7])
                            volume = int(data[8])

                            market_index = self.get_market_index(pair)
                            if market_index != current_market_index:
                                current_market_index = market_index

                            if pair not in self.data_dict:
                                self.data_dict[pair] = []

                            self.data_dict[pair].append({
                                'datetime': datetime_obj,
                                'open': open_price,
                                'high': high_price,
                                'low': low_price,
                                'close': close_price,
                                'volume': volume
                            })

                            self.candlestick(market_index, pair)
                elif data[0] == "HISTORICAL_DATA":
                    # Almacena los datos históricos
                    historical_data.append(serialized_data)
                
            except ConnectionResetError:
                print("La conexión con el servidor se cerró inesperadamente. Terminando la simulación.")
                break

        # Procesar y graficar los datos históricos
        for historical_item in historical_data:
            data = historical_item.split()
            print(data)
            if data[0] == "HISTORICAL_DATA":
                # Procesa los datos históricos (date, timeframe, pair, open, high, low, close, volume)
                datetime_str = data[1] + ' ' + data[2]
                datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                pair = data[3]
                open_price = float(data[4])
                high_price = float(data[5])
                low_price = float(data[6])
                close_price = float(data[7])
                volume = int(data[8])

                market_index = self.get_market_index(pair)
                if market_index != current_market_index:
                    current_market_index = market_index

                if pair not in self.data_dict:
                    self.data_dict[pair] = []

                self.data_dict[pair].append({
                    'datetime': datetime_obj,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                })

                self.candlestick(market_index, pair)

        plt.show()

    def get_market_index(self, pair):
        market_mapping = {
            "BRENTCMDUSD": 0,
            "BTCUSD": 1,
            "EURUSD": 2,
            "GBPUSD": 3,
            "USA30IDXUSD": 4,
            "USA500IDXUSD": 5,
            "USATECHIDXUSD": 6,
            "XAGUSD": 7,
            "XAUUSD": 8
        }
        return market_mapping.get(pair, 0)

    def candlestick(self, market_index, pair):
        ohlc_data = self.data_dict[pair]

        for row in self.data[market_index]:
            ohlc_data.append([
                row['datetime'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ])

        num_displayed_candles = len(ohlc_data)

        if num_displayed_candles > 20:
            ohlc_data = ohlc_data[-20:]

        ax = self.axes[market_index // 3, market_index % 3]
        ax.clear()
        df = pd.DataFrame(ohlc_data, columns=['open', 'high', 'low', 'close', 'volume'], index=[candle['datetime'] for candle in ohlc_data])

        mc = mpf.make_marketcolors(up='g', down='r')
        colors = mpf.make_mpf_style(marketcolors=mc)
        kwargs=dict(type='candle', mav=(5,13), style=colors)

        mpf.plot(df, **kwargs, ax=ax)

        ax.set_title(f"Mercado {market_index + 1} - {pair}")
        ax.set_xlabel('Fecha')
        ax.set_ylabel('Precio')

        plt.tight_layout()
        plt.draw()
        plt.pause(0.05)

if __name__ == "__main__":
    broker_host = "localhost"
    broker_port = 5000

    client = Client(broker_host, broker_port)
    client.connect_to_broker()
    client.start_simulation()
