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

# Print all environment variables (excluding sensitive values)
print("Environment variables available:")
for key in os.environ:
    if any(sensitive in key.lower() for sensitive in ['password', 'key', 'secret']):
        print(f"{key}: [MASKED]")
    else:
        print(f"{key}: {os.environ[key]}")

# Database configuration from environment variables
DB_CONFIG = {
    'ssh_host': os.environ.get('SSH_HOST'),
    'ssh_username': os.environ.get('SSH_USERNAME'),
    'mysql_host': os.environ.get('MYSQL_HOST'),
    'mysql_user': os.environ.get('MYSQL_USER'),
    'mysql_password': os.environ.get('MYSQL_PASSWORD'),
    'mysql_database': os.environ.get('MYSQL_DATABASE')
}

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
            
        print(f"SSH key permissions: {oct(os.stat(ssh_key_path).st_mode)[-3:]}")
        
        with SSHTunnelForwarder(
            (DB_CONFIG['ssh_host'], 22),
            ssh_username=DB_CONFIG['ssh_username'],
            ssh_pkey=ssh_key_path,
            remote_bind_address=(DB_CONFIG['mysql_host'], 3306),
            local_bind_address=('127.0.0.1', 3307),
            set_keepalive=60
        ) as tunnel:
            print(f"SSH tunnel established on local port {tunnel.local_bind_port}")
            
            time.sleep(3)
            
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