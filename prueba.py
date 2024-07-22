import csv
import json
import argparse
import threading
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import time

data_lock = threading.Lock()
shared_data = []

def file(period, format, currency):
    file_format = format.lower()
    if file_format not in ["csv", "json"]:
        raise ValueError("formato no valido.")
    
    file_extension = file_format if file_format == "json" else "csv"
    file_name = f"{currency.upper()}_{period.upper()}.{file_extension}"
    
    return file_name

def simtrading(file_name):
    date = start = highest = lowest = end = volume = None

    try:
        if file_name.endswith('.csv'):
            with open(file_name, 'r') as file:
                data = csv.reader(file, delimiter=",")
                next(data, None)
                for row in data:
                    if len(row) >= 6:
                        date = row[0]
                        highest = row[1]
                        lowest = row[2]
                        start = row[3]
                        end = row[4]
                        volume = row[5]
                    with data_lock:
                        shared_data.append((date, start, highest, lowest, end, volume))
                        #print(shared_data)
        elif file_name.endswith('.json'):
            with open(file_name, 'r') as file:
                data = json.load(file)
                for row in range(len(data['time'])):
                    date = data['time'][row]
                    start = data['open'][row]
                    highest = data['high'][row] 
                    lowest = data['low'][row]   
                    end = data['close'][row]
                    volume = data['volume'][row]
                    with data_lock:
                        shared_data.append((date, start, highest, lowest, end, volume))
                        #print(shared_data)

    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return 1

    return shared_data

def candlestick(data, max_displayed_candles=10):
    ohlc_data = []
    num_displayed_candles = 0  

    fig, ax = plt.subplots()

    short_window = 5
    long_window = 10

    for i, row in enumerate(data):
        if isinstance(row[0], str):
            date, start, highest, lowest, end, volume = row
            ohlc_data.append([
                mdates.date2num(datetime.strptime(date, '%Y-%m-%d %H:%M')),
                float(start), float(highest), float(lowest), float(end), float(volume)
            ])
        elif isinstance(row[0], int):
            start_date = datetime(2000, 1, 1)
            date = start_date + timedelta(minutes=row[0])
            start, highest, lowest, end, volume = row[1:]
            ohlc_data.append([
                mdates.date2num(date),
                float(start), float(highest), float(lowest), float(end), float(volume)
            ])

        num_displayed_candles += 1

        if num_displayed_candles > max_displayed_candles:
            ohlc_data.pop(0)
            num_displayed_candles -= 1

        plot_ohlc_data = ohlc_data

        ax.clear()
        candlestick_ohlc(ax, plot_ohlc_data, width=0.5, colorup='g', colordown='r')

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.xticks(rotation=45)

        close_prices = [candle[4] for candle in plot_ohlc_data]
        short_moving_avg = pd.Series(close_prices).rolling(window=short_window, min_periods=1).mean()
        long_moving_avg = pd.Series(close_prices).rolling(window=long_window, min_periods=1).mean()

        ax.plot([candle[0] for candle in plot_ohlc_data], short_moving_avg, 'b-', label=f'{short_window}-period MA')
        ax.plot([candle[0] for candle in plot_ohlc_data], long_moving_avg, 'm-', label=f'{long_window}-period MA')

        plt.xlabel('Fecha')
        plt.ylabel('Precio')
        plt.title('Grafica de Velas Japonesas')

        plt.tight_layout()
        plt.legend()
        ax.autoscale_view()
        plt.draw()
        plt.pause(1.0)

    plt.show()

def main(args):
    if args.currency is not None and args.period is not None and args.format is not None:
        currency = args.currency
        period = args.period
        file_format = args.format

        try:
            name = file(period, file_format, currency)
        except ValueError as e:
            print(e)
            return

        t_std = threading.Thread(target=simtrading, args=(name,))

        t_std.start()
        t_std.join()

        candlestick(shared_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--period", type=str, help="Periodo de tiempo")
    parser.add_argument("-f", "--format", type=str, help="formato del archivo (csv o json)")
    parser.add_argument("-m", "--currency", type=str, help="moneda")
    args = parser.parse_args()
    main(args)