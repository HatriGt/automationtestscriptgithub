import mysql.connector
import paramiko
import pandas as pd
import os
import time
import logging
import sys
from datetime import datetime
import socket
import select

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseTunnel:
    def __init__(self, ssh_host, ssh_user, ssh_key_path, remote_host, remote_port=3306, local_port=3307):
        self.ssh_client = None
        self.transport = None
        self.forward_channel = None
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_key_path = ssh_key_path
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.local_port = local_port
        
    def __enter__(self):
        try:
            # Initialize SSH client
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect to SSH server
            logger.info(f"Connecting to SSH server {self.ssh_host}")
            self.ssh_client.connect(
                self.ssh_host,
                username=self.ssh_user,
                key_filename=self.ssh_key_path,
                timeout=10,
                allow_agent=False,
                look_for_keys=False
            )
            
            # Get transport
            self.transport = self.ssh_client.get_transport()
            self.transport.set_keepalive(5)
            
            # Create a direct-tcpip channel
            logger.info(f"Creating direct TCP/IP channel to {self.remote_host}:{self.remote_port}")
            dest_addr = (self.remote_host, self.remote_port)
            local_addr = ('127.0.0.1', self.local_port)
            self.forward_channel = self.transport.open_channel(
                "direct-tcpip", 
                dest_addr,
                local_addr
            )
            
            if self.forward_channel is None:
                raise Exception("Failed to open forward channel")
            
            logger.info("Port forwarding established")
            return self
            
        except Exception as e:
            logger.error(f"Failed to establish tunnel: {str(e)}")
            self.close()
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        if self.forward_channel and not self.forward_channel.closed:
            self.forward_channel.close()
        if self.transport and self.transport.is_active():
            self.transport.close()
        if self.ssh_client:
            self.ssh_client.close()

def execute_query():
    try:
        # Configuration
        config = {
            'ssh_host': os.environ['SSH_HOST'],
            'ssh_user': os.environ['SSH_USERNAME'],
            'mysql_host': os.environ['MYSQL_HOST'],
            'mysql_user': os.environ['MYSQL_USER'],
            'mysql_pass': os.environ['MYSQL_PASSWORD'],
            'mysql_db': os.environ['MYSQL_DATABASE']
        }
        
        ssh_key_path = os.path.expanduser('~/.ssh/id_rsa')
        logger.info("Starting database connection...")
        
        with DatabaseTunnel(
            ssh_host=config['ssh_host'],
            ssh_user=config['ssh_user'],
            ssh_key_path=ssh_key_path,
            remote_host=config['mysql_host']
        ) as tunnel:
            
            logger.info("Tunnel established, connecting to database...")
            # Short delay to ensure channel is ready
            time.sleep(2)
            
            # Connect to MySQL through the tunnel
            connection = mysql.connector.connect(
                host='127.0.0.1',
                port=3307,
                user=config['mysql_user'],
                password=config['mysql_pass'],
                database=config['mysql_db'],
                connection_timeout=10
            )
            
            logger.info("Connected to database, executing query...")
            query = "SELECT * FROM countries"
            df = pd.read_sql_query(query, connection)
            
            output_file = f'query_results_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_file, index=False)
            logger.info(f"Results saved to {output_file}")
            
            connection.close()
            logger.info("Database connection closed")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        logger.info("Cleanup complete")

if __name__ == "__main__":
    execute_query()