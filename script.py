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
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection(host, user):
    """Test direct SSH connection"""
    cmd = f'nc -zv {host} 3306'
    try:
        result = subprocess.run(['ssh', f'{user}@{host}', cmd], 
                              capture_output=True, 
                              text=True)
        logger.info(f"Connection test result: {result.stdout}")
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False

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
        
        # Test connection first
        logger.info("Testing connection to database server...")
        if not test_connection(config['ssh_host'], config['ssh_user']):
            raise Exception("Failed to establish connection to database server")

        # Use SSH command to create the tunnel
        tunnel_cmd = (f"ssh -N -L 3307:{config['mysql_host']}:3306 "
                     f"{config['ssh_user']}@{config['ssh_host']} & "
                     "echo $!")
        
        logger.info("Starting SSH tunnel...")
        tunnel_process = subprocess.Popen(tunnel_cmd, 
                                        shell=True, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE)
        
        # Wait for tunnel to be established
        time.sleep(5)
        
        logger.info("Connecting to database through tunnel...")
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3307,
            user=config['mysql_user'],
            password=config['mysql_pass'],
            database=config['mysql_db'],
            connection_timeout=30
        )
        
        logger.info("Executing query...")
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
        # Cleanup
        try:
            subprocess.run(['pkill', '-f', 
                          f'ssh -N -L 3307:{config["mysql_host"]}:3306'])
        except:
            pass
        logger.info("Cleanup complete")

if __name__ == "__main__":
    execute_query()