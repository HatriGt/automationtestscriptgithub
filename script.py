import mysql.connector
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import os
import time
import logging
import socket
from datetime import datetime

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration from environment variables
DB_CONFIG = {
    'ssh_host': os.environ.get('SSH_HOST'),
    'ssh_username': os.environ.get('SSH_USERNAME'),
    'mysql_host': os.environ.get('MYSQL_HOST'),
    'mysql_user': os.environ.get('MYSQL_USER'),
    'mysql_password': os.environ.get('MYSQL_PASSWORD'),
    'mysql_database': os.environ.get('MYSQL_DATABASE')
}

def wait_for_port(port, host='127.0.0.1', timeout=20):
    """Wait until a port starts accepting TCP connections."""
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            if time.time() - start_time >= timeout:
                return False
            time.sleep(1)

def execute_query():
    # Validate environment variables
    required_vars = ['SSH_HOST', 'SSH_USERNAME', 'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    try:
        print(f"Starting query execution at {datetime.now()}")
        print(f"Using SSH host: {DB_CONFIG['ssh_host']}")
        print(f"Using SSH username: {DB_CONFIG['ssh_username']}")
        print(f"Using MySQL host: {DB_CONFIG['mysql_host']}")
        
        ssh_key_path = os.path.expanduser('~/.ssh/id_rsa')
        print(f"Using SSH key from: {ssh_key_path}")
        
        if not os.path.exists(ssh_key_path):
            raise Exception(f"SSH key not found at {ssh_key_path}")

        print("Creating SSH tunnel...")
        with SSHTunnelForwarder(
            (DB_CONFIG['ssh_host'], 22),
            ssh_username=DB_CONFIG['ssh_username'],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(DB_CONFIG['mysql_host'], 3306),
            local_bind_address=('127.0.0.1', 3307)
        ) as server:
            
            print(f"SSH tunnel established on local port {server.local_bind_port}")
            
            if not wait_for_port(3307):
                raise Exception("Timeout waiting for tunnel port")
            
            print("Connecting to database...")
            try:
                connection = mysql.connector.connect(
                    host='127.0.0.1',
                    port=3307,
                    user=DB_CONFIG['mysql_user'],
                    password=DB_CONFIG['mysql_password'],
                    database=DB_CONFIG['mysql_database'],
                    connection_timeout=30
                )
                
                print("Database connection established")
                
                query = """
                SELECT 
                   * FROM countries
                """
                
                print("\nExecuting main query...")
                df = pd.read_sql_query(query, connection)
                
                output_file = f'query_results_{datetime.now().strftime("%Y%m%d")}.csv'
                df.to_csv(output_file, index=False)
                print(f"Results saved to {output_file}")
                
                connection.close()
                print("\nDatabase connection closed")
                
            except mysql.connector.Error as err:
                print(f"Database Error: {err}")
                raise
                    
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    execute_query()