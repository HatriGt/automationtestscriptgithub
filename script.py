import mysql.connector
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import os
import time
import logging
import socket
from datetime import datetime
import sys

# Enable debug logging with more detail
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# Specifically enable paramiko logging
logging.getLogger('paramiko').setLevel(logging.DEBUG)

def is_port_in_use(port, host='127.0.0.1'):
    """Check if a port is already in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0

def wait_for_port(port, host='127.0.0.1', timeout=30):
    """Wait until a port starts accepting TCP connections"""
    start_time = time.time()
    while True:
        if time.time() - start_time >= timeout:
            return False
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.1)

def find_free_port(start_port=3307):
    """Find a free port starting from the given port number"""
    port = start_port
    while is_port_in_use(port):
        port += 1
        if port > start_port + 100:  # Don't search indefinitely
            raise RuntimeError("Could not find a free port")
    return port

def execute_query():
    # Get configurations
    config = {
        'ssh_host': os.environ.get('SSH_HOST'),
        'ssh_username': os.environ.get('SSH_USERNAME'),
        'mysql_host': os.environ.get('MYSQL_HOST'),
        'mysql_user': os.environ.get('MYSQL_USER'),
        'mysql_password': os.environ.get('MYSQL_PASSWORD'),
        'mysql_database': os.environ.get('MYSQL_DATABASE')
    }

    # Validate configurations
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    tunnel = None
    connection = None
    
    try:
        print(f"Starting execution at {datetime.now()}")

        # Find an available local port
        local_port = find_free_port()
        print(f"Using local port: {local_port}")

        # Create and start SSH tunnel
        tunnel = SSHTunnelForwarder(
            (config['ssh_host'], 22),
            ssh_username=config['ssh_username'],
            ssh_pkey=os.path.expanduser('~/.ssh/id_rsa'),
            remote_bind_address=(config['mysql_host'], 3306),
            local_bind_address=('127.0.0.1', local_port),
            threaded=True  # Run in background thread
        )

        print("Starting tunnel...")
        tunnel.daemon_forward_servers = True
        tunnel.start()

        if not tunnel.is_active:
            raise RuntimeError("Failed to establish SSH tunnel")

        print("Waiting for tunnel to be ready...")
        if not wait_for_port(local_port):
            raise TimeoutError("Tunnel port not responding")

        print("Establishing database connection...")
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=local_port,
            user=config['mysql_user'],
            password=config['mysql_password'],
            database=config['mysql_database'],
            connection_timeout=30
        )

        print("Executing query...")
        query = "SELECT * FROM countries"
        df = pd.read_sql_query(query, connection)

        output_file = f'query_results_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

    finally:
        print("Cleaning up connections...")
        if connection and connection.is_connected():
            print("Closing database connection...")
            connection.close()

        if tunnel and tunnel.is_active:
            print("Closing SSH tunnel...")
            tunnel.stop()
            tunnel.close()
            
        print("Cleanup complete")

if __name__ == "__main__":
    try:
        execute_query()
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Script failed: {str(e)}")
        sys.exit(1)