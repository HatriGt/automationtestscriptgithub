import mysql.connector
import pandas as pd
import os
import time
import logging
import sys
from datetime import datetime
import subprocess

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def setup_tunnel():
    """Set up SSH tunnel using system SSH command"""
    ssh_host = os.environ['SSH_HOST']
    ssh_user = os.environ['SSH_USERNAME']
    mysql_host = os.environ['MYSQL_HOST']
    
    # Build SSH command
    cmd = f"ssh -v -N -L 3307:{mysql_host}:3306 {ssh_user}@{ssh_host}"
    
    logger.info(f"Starting SSH tunnel with command: {cmd}")
    
    # Start tunnel in background
    process = subprocess.Popen(
        cmd.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait a bit for tunnel to establish
    time.sleep(5)
    return process

def execute_query():
    tunnel_process = None
    connection = None
    
    try:
        logger.info("Starting database connection process...")
        
        # Start SSH tunnel
        tunnel_process = setup_tunnel()
        logger.info("SSH tunnel process started")
        
        # Check if tunnel process is still running
        if tunnel_process.poll() is not None:
            stdout, stderr = tunnel_process.communicate()
            logger.error(f"Tunnel process failed. stdout: {stdout}, stderr: {stderr}")
            raise Exception("Failed to establish SSH tunnel")
            
        logger.info("Attempting database connection...")
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3307,
            user=os.environ['MYSQL_USER'],
            password=os.environ['MYSQL_PASSWORD'],
            database=os.environ['MYSQL_DATABASE'],
            connection_timeout=10
        )
        
        logger.info("Successfully connected to database")
        
        # Execute query
        logger.info("Executing query...")
        query = "SELECT * FROM countries"
        df = pd.read_sql_query(query, connection)
        
        # Save results
        output_file = f'query_results_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise
        
    finally:
        logger.info("Cleaning up...")
        
        # Close database connection
        if connection:
            try:
                connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
        
        # Kill SSH tunnel
        if tunnel_process:
            try:
                tunnel_process.terminate()
                tunnel_process.wait(timeout=5)
                logger.info("SSH tunnel terminated")
            except Exception as e:
                logger.error(f"Error terminating SSH tunnel: {str(e)}")
                # Force kill if needed
                try:
                    tunnel_process.kill()
                    logger.info("SSH tunnel force killed")
                except:
                    pass
        
        logger.info("Cleanup complete")

if __name__ == "__main__":
    execute_query()