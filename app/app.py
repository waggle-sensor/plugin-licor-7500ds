"""
Waggle plugin for Licor SmartFlux data acquisition - Simplest Workflow.
Reads DATA, saves to CSV.
"""

import socket
import logging
from waggle.plugin import Plugin
import pandas as pd
import threading
import time
import datetime
import os
from pathlib import Path
import csv
import argparse
import timeout_decorator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TIMEOUT_SECONDS = 120
DEFAULT_CSV_ROTATE_MINUTES = 30
FLUSH_INTERVAL_SECONDS = 300  

# Define headers for CSV data
DATA_HEADER = ['SECONDS', 'NANOSECONDS', 'DIAG', 'Ndx', 'CO2D', 'H2OD', 'Temp', 'Pressure', 'CO2MF', 'CO2MFd',
               'H2OMF', 'H2OMFd', 'DewPt', 'CO2Abs', 'H2OAbs', 'H2OAW', 'H2OAWO', 'CO2AW', 'CO2AWO', 'AvgSS', 'CO2SS',
               'H2OSS', 'DeltaDD', 'DetectorCoolerV', 'ChopperCoolerV', 'VIN', 'DiagVal', 'CHK']


class DataLogger:
    """CSV Data Logger."""
    def __init__(self, plugin, prefix="LI7500DS_data", rotate_minutes=DEFAULT_CSV_ROTATE_MINUTES):
        self.plugin = plugin
        self.prefix = prefix
        self.current_file = None
        self.csv_writer = None
        self.start_time = None
        self.end_time = None
        self.upload_thread = None
        self.is_uploading = False
        self.flush_interval_seconds = FLUSH_INTERVAL_SECONDS
        self.last_flush_time = time.time()
        self.rotate_minutes = rotate_minutes

    def _make_filename(self):
        """Generates filename with prefix and time range."""
        start_time_str = self.start_time.strftime("%Y%m%d_%H%M")
        end_time_str = self.end_time.strftime("%H%M")
        return f"{self.prefix}_{start_time_str}_{end_time_str}.csv"

    def _open_file(self):
        """Closes current, opens new CSV file, writer."""
        if self.current_file:
            self.current_file.close()
            self._upload_async(self.current_file_path)

        self.start_time = datetime.datetime.now()
        self.end_time = self.start_time + datetime.timedelta(minutes=self.rotate_minutes)
        filename = self._make_filename()
        file_dir = "data_csv"
        Path(file_dir).mkdir(exist_ok=True)
        self.current_file_path = os.path.join(file_dir, filename)
        self.current_file = open(self.current_file_path, 'w', newline='')
        self.csv_writer = csv.writer(self.current_file)
        logging.info(f"Opened CSV file: {self.current_file_path}")

    def rotate_file(self):
        """Rotates CSV file based on time intervals."""
        self._open_file()

    def save_df_csv(self, df):
        """Saves DataFrame to CSV, includes header."""
        if not self.current_file:
            self._open_file()

        if df is not None and not df.empty:
            try:
                if self.csv_writer:
                    if self.current_file.tell() == 0:
                        self.csv_writer.writerow(df.columns)
                    for _, row in df.iterrows():
                        self.csv_writer.writerow(row.tolist())

                    if time.time() - self.last_flush_time >= self.flush_interval_seconds:
                        self.current_file.flush()
                        os.fsync(self.current_file.fileno())
                        self.last_flush_time = time.time()
            except Exception as e:
                logging.error(f"CSV Write Error: {e}")

    def _upload_async(self, file_path):
        """Upload file in a separate thread."""
        if not self.is_uploading:
            self.is_uploading = True
            self.upload_thread = threading.Thread(target=self._upload, args=(file_path,))
            self.upload_thread.start()
        else:
            logging.warning(f"Upload already in progress, skipping: {file_path}")

    def _upload(self, file_path):
        """Upload CSV file using plugin.upload_file."""
        try:
            logging.info(f"Uploading: {file_path}")
            self.plugin.upload_file(file_path)
            logging.info(f"Uploaded: {file_path}")
        except Exception as e:
            logging.error(f"File upload failed for {file_path}: {e}")
        finally:
            self.is_uploading = False


# Global DataFrame
df_data = pd.DataFrame(columns=DATA_HEADER)

def ingest_data(args, plugin, oneshot=False):
    """Data ingestion thread."""
    global df_data
    tcp_socket = None
    try:
        tcp_socket = connect_device(args)
        logging.info("Data ingestion started.")
        current_data_header = DATA_HEADER

        while True:
            try:
                line = tcp_socket.recv(4096).decode('utf-8').strip()
                logging.info(f"Ingested line: '{line}'") # DEBUG LOGGING
                if not line:
                    continue

                parts = line.split()
                logging.info(f"Parts after split: {parts}") # DEBUG LOGGING
                if not parts:
                    continue

                dtype = parts[0]
                logging.info(f"Data type: {dtype}") # DEBUG LOGGING

                if dtype == 'DATAH':
                    header_row = parts[1:]
                    if len(header_row) == len(DATA_HEADER):
                        current_data_header = header_row
                        df_data.columns = current_data_header
                        logging.info("DATAH Header updated.")
                    else:
                        logging.warning(f"DATAH Header format issue, using default. Line: {line}")

                elif dtype == 'DATA':
                    values = parts[1:]
                    if len(values) == len(current_data_header):
                        try:
                            numeric_vals = [float(v) if v.replace('.', '', 1).isdigit() else v for v in values]
                            row_data = dict(zip(current_data_header, numeric_vals))
                            df_data = pd.concat([df_data, pd.DataFrame([row_data])], ignore_index=True)
                            logging.info(f"DataFrame after DATA ingestion:\n{df_data.to_string()}") # DEBUG LOGGING
                        except ValueError as ve:
                            logging.warning(f"Data convert error, skipping row. Err: {ve}, Line: {line}")
                    else:
                        logging.warning(f"DATA format issue, skipping. Line: {line}")

            except Exception as e:
                logging.error(f"Ingestion error: {e}")
                time.sleep(5)
            if oneshot:
                break

    finally:
        if tcp_socket:
            tcp_socket.close()
            logging.info("Data ingestion stopped, socket closed.")



def export_data(plugin, args, oneshot=False): # ADDED oneshot=False
    """Data export thread."""
    global df_data
    csv_logger = DataLogger(plugin, rotate_minutes=args.csv_rotate_minutes)

    if oneshot:
        csv_logger.rotate_file()
        csv_logger.save_df_csv(df_data.copy())
        csv_logger._upload_async(csv_logger.current_file_path)
        df_data = pd.DataFrame(columns=DATA_HEADER)
        return
        csv_logger.save_df_csv(df_data.copy())
        csv_logger._upload_async(csv_logger.current_file_path)
        df_data = pd.DataFrame(columns=DATA_HEADER)
        return

    while True:
        start_time = datetime.datetime.now()

        df_data_copy = df_data.copy()
        csv_logger.save_df_csv(df_data_copy)
        csv_logger._upload_async(csv_logger.current_file_path)

        df_data = pd.DataFrame(columns=DATA_HEADER) # Clear df_data, keep header

        end_time = datetime.datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        sleep_duration = max(0, int(args.csv_rotate_minutes) * 60 - int(elapsed_time))
        if not oneshot:
            time.sleep(sleep_duration)
        else:
            break # break for testing


@timeout_decorator.timeout(TIMEOUT_SECONDS, use_signals=True)
def connect_device(args):
    try:
        sock = socket.create_connection((args.ip, args.port), timeout=TIMEOUT_SECONDS)
        return sock
    except socket.error as e:
        raise Exception("Connect failed") from e


def run(args):
    """Main run function."""
    with Plugin() as plugin:
        logging.info("Starting Licor SmartFlux plugin - simplest workflow.")

        ingest_thread = threading.Thread(target=ingest_data, args=(args, plugin), daemon=True)
        ingest_thread.start()

        export_thread = threading.Thread(target=export_data, args=(plugin, args), daemon=True)
        export_thread.start()
        while True:
            time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Licor SmartFlux Data Logger - Simplest Workflow")
    parser.add_argument('--ip', type=str, required=True, help='SmartFlux IP address')
    parser.add_argument('--port', type=int, default=7200, help='TCP port (default: 7200)')
    parser.add_argument('--timeout', type=int, default=300, help='Connection timeout in seconds (default: 300)')
    parser.add_argument('--csv_rotate_minutes', type=int, default=DEFAULT_CSV_ROTATE_MINUTES, help=f'CSV rotation interval in minutes (default: {DEFAULT_CSV_ROTATE_MINUTES})')
    args = parser.parse_args()

    try:
        run(args)
    except KeyboardInterrupt:
        logging.info("Plugin stopped by user.")
    except Exception as e:
        logging.error(f"Plugin error: {e}")