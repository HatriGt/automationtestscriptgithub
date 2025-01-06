import mysql.connector
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import os
import time
import logging
from datetime import datetime

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration from environment variables
DB_CONFIG = {
    'ssh_host': os.environ.get('SSH_HOST'),
    'ssh_username': os.environ.get('SSH_USER'),
    'mysql_host': os.environ.get('DB_HOST'),
    'mysql_user': os.environ.get('DB_USER'),
    'mysql_password': os.environ.get('DB_PASSWORD'),
    'mysql_database': os.environ.get('DB_NAME')
}

def execute_query():
    try:
        print(f"Starting query execution at {datetime.now()}")
        print(f"Using SSH host: {DB_CONFIG['ssh_host']}")
        print(f"Using SSH username: {DB_CONFIG['ssh_username']}")
        
        # Add logging for SSH key
        ssh_key_path = os.path.expanduser('~/.ssh/id_rsa')
        print(f"Using SSH key from: {ssh_key_path}")
        if not os.path.exists(ssh_key_path):
            raise Exception(f"SSH key not found at {ssh_key_path}")
        
        with SSHTunnelForwarder(
            (DB_CONFIG['ssh_host'], 22),
            ssh_username=DB_CONFIG['ssh_username'],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(DB_CONFIG['mysql_host'], 3306),
            local_bind_address=('127.0.0.1', 3307),
            set_keepalive=60
        ) as tunnel:
            print(f"SSH tunnel established on local port {tunnel.local_bind_port}")
            
            time.sleep(3)  # Wait for tunnel to be ready
            
            print("Connecting to database...")
            try:
                connection = mysql.connector.connect(
                    host='127.0.0.1',
                    port=3307,
                    user=DB_CONFIG['mysql_user'],
                    password=DB_CONFIG['mysql_password'],
                    database=DB_CONFIG['mysql_database']
                )
                
                print("Database connection established")
                
                # Execute your queries here
                query = """
                SELECT 
                   * FROM countries
                """
                
                print("\nExecuting main query...")
                df = pd.read_sql_query(query, connection)
                
                # Save results to CSV
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
