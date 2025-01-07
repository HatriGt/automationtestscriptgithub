import pandas as pd
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import openpyxl
from typing import Dict, List
import re
from sshtunnel import SSHTunnelForwarder
import paramiko
import logging
import os
import sys
from datetime import datetime
import traceback
import time
import openpyxl.styles
from dotenv import load_dotenv

# Set up logging with timestamp in filename
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'weekly_report_{current_time}.log'

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)
log_filepath = os.path.join('logs', log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath),
        logging.StreamHandler(sys.stdout)
    ]
)

# Set MySQL connector logging
mysql_logger = logging.getLogger('mysql.connector')
mysql_logger.setLevel(logging.DEBUG)
mysql_logger.addHandler(logging.FileHandler('mysql_debug.log'))

logger = logging.getLogger(__name__)

load_dotenv()  # Load environment variables from .env file

class ReportGenerator:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.regions = {
            'Dubai': ['adarsh', 'sagun', 'simrahashraf', 'raghav', 'nihad', 'waseem'],
            'Abu Dhabi': ['clarita', 'adhil', 'sana', 'lamis'],
            'Sharjah': ['taiba', 'sahbhan', 'akef', 'aaftab'],
            'KAM': ['musir', 'sparsh', 'sourabh', 'tariq']
        }
        self.ssh_tunnel = None
        self._init_ssh_tunnel()

    def _init_ssh_tunnel(self):
        """Initialize SSH tunnel with retries"""
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                logger.info(f"Attempting to create SSH tunnel (attempt {retry_count + 1}/{max_retries})")
                self.create_ssh_tunnel()
                if self.ssh_tunnel and self.ssh_tunnel.is_active:
                    logger.info("SSH tunnel successfully created and active")
                    break
                logger.warning("SSH tunnel created but not active")
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    logger.error("Failed to create SSH tunnel after maximum retries")
                    raise
                logger.warning(f"SSH tunnel creation failed, retrying... Error: {str(e)}")
                time.sleep(2)

    def create_ssh_tunnel(self):
        if self.ssh_tunnel is None or not self.ssh_tunnel.is_active:
            try:
                logger.info("Creating SSH tunnel...")
                self.ssh_tunnel = SSHTunnelForwarder(
                    (self.db_config['ssh_host'], 22),
                    ssh_username=self.db_config['ssh_username'],
                    ssh_pkey=self.db_config['ssh_pkey_path'],
                    remote_bind_address=(self.db_config['mysql_host'], 3306)
                )
                self.ssh_tunnel.start()
                logger.info(f"SSH tunnel created successfully on local port {self.ssh_tunnel.local_bind_port}")
            except Exception as e:
                logger.error(f"Error creating SSH tunnel: {str(e)}")
                logger.error(traceback.format_exc())
                raise

        return self.ssh_tunnel

    def get_db_connection(self) -> mysql.connector.connection.MySQLConnection:
        try:
            if not self.ssh_tunnel or not self.ssh_tunnel.is_active:
                logger.error("SSH tunnel is not active")
                self._init_ssh_tunnel()
            
            logger.info("Attempting to establish database connection...")
            logger.info(f"Connection parameters: host=127.0.0.1, port={self.ssh_tunnel.local_bind_port}")
            
            # Use only supported connection parameters
            conn = mysql.connector.connect(
                host='127.0.0.1',
                port=self.ssh_tunnel.local_bind_port,
                user=self.db_config['mysql_user'],
                password=self.db_config['mysql_password'],
                database=self.db_config['mysql_database'],
                connect_timeout=30,  # Only keep this timeout
                use_pure=True,      # Use pure Python implementation
                buffered=True       # Use buffered cursors
            )
            
            # Test the connection with timeout
            try:
                cursor = conn.cursor(buffered=True)
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                if result != (1,):
                    raise Exception("Connection test failed")
                logger.info("Database connection established and tested successfully")
                return conn
            except Exception as e:
                logger.error(f"Connection test failed: {str(e)}")
                if conn:
                    conn.close()
                raise
            
        except mysql.connector.Error as e:
            logger.error(f"MySQL connection error: {str(e)}")
            logger.error(f"Error code: {e.errno if hasattr(e, 'errno') else 'N/A'}")
            logger.error(f"SQLSTATE: {e.sqlstate if hasattr(e, 'sqlstate') else 'N/A'}")
            logger.error(f"Error message: {e.msg if hasattr(e, 'msg') else str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in database connection: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def validate_excel_template(self, template_path: str) -> None:
        """Validate Excel template existence and structure"""
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Excel template not found: {template_path}")
        
        try:
            workbook = openpyxl.load_workbook(template_path)
            logger.info(f"Successfully opened Excel template: {template_path}")
            
            # Only need to verify the main sheet exists
            if 'Sheet1' not in workbook.sheetnames:
                raise ValueError("Template must contain 'Sheet1'")
            
            logger.info("Excel template validated successfully")
            workbook.close()
        except Exception as e:
            logger.error(f"Error validating Excel template: {str(e)}")
            raise

    def modify_query_for_region(self, query: str, region: str) -> str:
        try:
            agents = self.regions[region]
            agents_str = "','".join(agents)
            logger.info(f"Modifying query for region {region} with agents: {agents_str}")
            
            pattern = r"ram\.agent_name\s+in\s*\([^)]+\)"
            new_filter = f"ram.agent_name in ('{agents_str}')"
            
            # Log the original query
            logger.info("Original query:")
            logger.info(query)
            
            modified_query = re.sub(pattern, new_filter, query)
            
            # Log the modified query
            logger.info("Modified query:")
            logger.info(modified_query)
            
            # Verify the modification was successful
            if modified_query == query:
                logger.warning("Query modification might have failed - no changes detected")
            
            return modified_query
        except Exception as e:
            logger.error(f"Error modifying query for {region}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def execute_query(self, query: str, region: str) -> pd.DataFrame:
        conn = None
        attempts = 0
        max_attempts = 3
        
        # Extract only the SQL part of the query
        sql_query = self._extract_sql_from_query(query)
        
        while attempts < max_attempts:
            try:
                logger.info(f"Processing region: {region}")
                conn = self.get_db_connection()
                modified_query = self.modify_query_for_region(sql_query, region)
                logger.info(f"Executing modified query for {region}...")
                
                # Print the actual modified query for debugging
                logger.info("Modified Query:")
                logger.info(modified_query)
                
                # Execute with cursor first to test query
                cursor = conn.cursor(buffered=True)
                logger.info("Executing query with cursor first...")
                cursor.execute(modified_query)
                logger.info("Query executed successfully with cursor")
                
                # Fetch results with pandas
                df = pd.read_sql_query(modified_query, conn)
                
                logger.info(f"Query execution successful for {region} - Retrieved {len(df)} rows")
                if df.empty:
                    logger.warning(f"Query returned empty DataFrame for {region}")
                else:
                    logger.info(f"DataFrame columns: {df.columns.tolist()}")
                    logger.info(f"First row sample: {df.iloc[0].to_dict()}")
                return df
                
            except mysql.connector.Error as e:
                attempts += 1
                logger.error(f"MySQL Error for {region} (attempt {attempts}/{max_attempts}): {str(e)}")
                logger.error(f"Error details: {e.__class__.__name__}")
                if attempts == max_attempts:
                    raise
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error executing query for {region}: {str(e)}")
                logger.error(f"Error type: {type(e).__name__}")
                logger.error(traceback.format_exc())
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                        logger.debug(f"Database connection closed for {region}")
                    except Exception as e:
                        logger.warning(f"Error closing database connection for {region}: {str(e)}")

    def _extract_sql_from_query(self, query: str) -> str:
        """Extract the SQL part from the query text by finding the first SELECT statement."""
        try:
            # Find the position of the first SELECT statement
            select_pos = query.upper().find('SELECT')
            if select_pos == -1:
                raise ValueError("No SELECT statement found in query")
            
            # Return everything from SELECT onwards
            sql_query = query[select_pos:]
            logger.info("Successfully extracted SQL query")
            return sql_query
        except Exception as e:
            logger.error(f"Error extracting SQL from query: {str(e)}")
            logger.error(f"Original query text: {query[:200]}...")  # Log first 200 chars
            raise

    def process_region(self, region: str, queries: Dict[str, str]) -> tuple:
        results = {}
        try:
            logger.info(f"Starting to process region: {region}")
            logger.info(f"Number of queries to process: {len(queries)}")
            
            for query_name, query in queries.items():
                logger.info(f"Processing query '{query_name}' for region '{region}'...")
                max_retries = 3
                retry_count = 0
                
                while retry_count < max_retries:
                    try:
                        results[query_name] = self.execute_query(query, region)
                        logger.info(f"Successfully completed query '{query_name}' for region '{region}'")
                        break
                    except Exception as e:
                        retry_count += 1
                        logger.error(f"Attempt {retry_count} failed for query '{query_name}' in region '{region}': {str(e)}")
                        if retry_count == max_retries:
                            raise
                        time.sleep(5)  # Wait before retry
                
            logger.info(f"Completed processing all queries for region: {region}")
            return region, results
        except Exception as e:
            logger.error(f"Error processing region {region}: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(traceback.format_exc())
            raise

    def update_excel_template(self, template_path: str, results: Dict[str, Dict[str, pd.DataFrame]]):
        try:
            logger.info(f"Opening Excel template: {template_path}")
            workbook = openpyxl.load_workbook(template_path)
            worksheet = workbook['Sheet1']  # Use the single sheet
            
            # Process results for each region
            for region, region_results in results.items():
                logger.info(f"\nProcessing region: {region}")
                self._process_query_results(worksheet, region_results, region)

            # Save the updated workbook
            output_dir = 'output'
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = os.path.join(output_dir, f'WoW_report_{timestamp}.xlsx')
            
            try:
                workbook.save(output_path)
                logger.info(f"Excel file successfully saved to {output_path}")
            except Exception as e:
                logger.error(f"Error saving workbook: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"Error updating Excel template: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _process_query_results(self, worksheet, region_results, region: str):
        try:
            logger.info(f"Processing results for region: {region}")
            
            # Define column mappings for each region
            column_mappings = {
                'Dubai': {'data': 'C', 'prev': 'D', 'growth': 'E'},
                'Abu Dhabi': {'data': 'F', 'prev': 'G', 'growth': 'H'},
                'Sharjah': {'data': 'I', 'prev': 'J', 'growth': 'K'},
                'KAM': {'data': 'L', 'prev': 'M', 'growth': 'N'}
            }
            
            cols = column_mappings[region]

            # Define segment mappings with their row ranges
            segment_mappings = {
                'Orders': {'start': 4, 'end': 7, 'params': [
                    'Total Orders',
                    'Successful Orders',
                    'Rejected Orders',
                    'Rejected Orders %'
                ]},
                'Sales': {'start': 8, 'end': 9, 'params': [
                    'Total Sales',
                    'Net Sales'
                ]},
                'Revenue': {'start': 10, 'end': 11, 'params': [
                    'Commissions',
                    'Payment Gateway'
                ]},
                'Customers': {'start': 12, 'end': 17, 'params': [
                    'Order Frequency',
                    'Smiles Subscription Orders',
                    'New Customer Count',
                    'New Customer Order Count',
                    'Repeat Customer Count',
                    'Repeat Customer Order Count'
                ]},
                'Discounts': {'start': 18, 'end': 22, 'params': [
                    'No Discount Orders',
                    'Restaurant Sponsored Orders',
                    'Smiles Sponsored Orders',
                    'Co-fund orders',
                    'Flat Discount'
                ]}
            }

            # Only update headers and segments for the first region
            if region == 'Dubai':
                # Update region headers
                self.update_region_headers(worksheet)
                
                # Update segment headers and parameters
                self.update_segment_headers(worksheet, segment_mappings)

            # Rest of your existing code...
            
        except Exception as e:
            logger.error(f"Error in _process_query_results for region {region}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def update_region_headers(self, worksheet):
        """Update the region headers and their formatting"""
        # Region headers (Row 1)
        region_ranges = {
            'Dubai': 'C1:E1',
            'Abu Dhabi': 'F1:H1',
            'Sharjah': 'I1:K1',
            'KAM': 'L1:N1'
        }
        
        # Unmerge all cells in header rows first
        for row in [1, 2]:
            for merge_range in worksheet.merged_cells.ranges.copy():
                if merge_range.min_row == row:
                    worksheet.unmerge_cells(str(merge_range))

        # Set region headers and merge cells
        for reg, cell_range in region_ranges.items():
            start_col, end_col = cell_range.split(':')[0][0], cell_range.split(':')[1][0]
            worksheet.merge_cells(cell_range)
            worksheet[f'{start_col}1'] = reg
            
            # Week headers (Row 2)
            worksheet[f'{start_col}2'] = 'Week'
            worksheet[f'{chr(ord(start_col) + 1)}2'] = 'Week'
            worksheet[f'{end_col}2'] = 'Growth/Degrowth %'
            
            # Previous headers (Row 3)
            worksheet[f'{start_col}3'] = '(Previous)'
            worksheet[f'{chr(ord(start_col) + 1)}3'] = '(Previous to Previous)'
            
            # Merge Week cells vertically in row 2-3
            worksheet.merge_cells(f'{start_col}2:{start_col}3')
            worksheet.merge_cells(f'{chr(ord(start_col) + 1)}2:{chr(ord(start_col) + 1)}3')
            worksheet.merge_cells(f'{end_col}2:{end_col}3')

        # Apply formatting to headers
        for row in [1, 2, 3]:
            for col in ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N']:
                cell = worksheet[f'{col}{row}']
                cell.alignment = openpyxl.styles.Alignment(horizontal='center', 
                                                         vertical='center',
                                                         wrap_text=True)
                cell.font = openpyxl.styles.Font(bold=True)
                cell.border = openpyxl.styles.Border(
                    left=openpyxl.styles.Side(style='thin'),
                    right=openpyxl.styles.Side(style='thin'),
                    top=openpyxl.styles.Side(style='thin'),
                    bottom=openpyxl.styles.Side(style='thin')
                )

    def update_segment_headers(self, worksheet, segment_mappings):
        """Update segment headers and their parameters"""
        # Clear existing segment headers and parameters
        for row in range(4, 23):
            worksheet[f'A{row}'].value = None
            worksheet[f'B{row}'].value = None

        # Add segment headers and parameters
        for segment, details in segment_mappings.items():
            start_row = details['start']
            
            # Write segment header in column A
            segment_cell = worksheet[f'A{start_row}']
            segment_cell.value = segment
            segment_cell.font = openpyxl.styles.Font(bold=True)
            
            # Merge segment header cells if segment spans multiple rows
            if details['end'] > start_row:
                worksheet.merge_cells(f'A{start_row}:A{details["end"]}')
            
            # Write parameters in column B
            for i, param in enumerate(details['params']):
                param_cell = worksheet[f'B{start_row + i}']
                param_cell.value = param
                
                # Apply borders to parameter cells
                param_cell.border = openpyxl.styles.Border(
                    left=openpyxl.styles.Side(style='thin'),
                    right=openpyxl.styles.Side(style='thin'),
                    top=openpyxl.styles.Side(style='thin'),
                    bottom=openpyxl.styles.Side(style='thin')
                )

    def parse_queries(self, content: str) -> Dict[str, str]:
        """Parse queries from the content string."""
        # Split on any sequence of dashes that's 10 or more characters long
        query_blocks = re.split(r'-{10,}', content)
        
        # Clean up each block and remove empty ones
        query_blocks = [block.strip() for block in query_blocks if block.strip()]
        
        queries = {}
        query_mapping = {
            'Query 1': 'orders_metrics',
            'Query 2': 'sales_metrics',
            'Query 3': 'commission_metrics',
            'Query 4': 'order_frequency',
            'Query 5': 'subscription_orders',
            'Query 6': 'new_customers',
            'Query 7': 'repeat_customers',
            'Query 8': 'repeat_orders',
            'Query 9': 'discount_orders'
        }
        
        # Map each query block to its corresponding key
        for block in query_blocks:
            for query_id, query_key in query_mapping.items():
                if query_id in block:
                    queries[query_key] = block

        # Verify all queries were found
        for query_key in query_mapping.values():
            if query_key not in queries or not queries[query_key].strip():
                raise ValueError(f"Missing or empty query: {query_key}")
        
        return queries

def validate_env_vars():
    required_vars = [
        'SSH_HOST', 'SSH_USERNAME', 'SSH_PKEY_PATH',
        'MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

def main():
    try:
        logger.info("Starting Weekly Report Generation")
        
        # Validate environment variables
        validate_env_vars()
        
        # Check if template exists
        template_path = 'WoW_report_template.xlsx'
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Excel template not found: {template_path}")
        
        # Check template file permissions
        if not os.access(template_path, os.R_OK):
            raise PermissionError(f"No read permission for template file: {template_path}")
        
        # Database configuration with SSH tunnel details
        db_config = {
            'ssh_host': os.getenv('SSH_HOST'),
            'ssh_username': os.getenv('SSH_USERNAME'),
            'ssh_pkey_path': os.getenv('SSH_PKEY_PATH'),
            'mysql_host': os.getenv('MYSQL_HOST'),
            'mysql_user': os.getenv('MYSQL_USER'),
            'mysql_password': os.getenv('MYSQL_PASSWORD'),
            'mysql_database': os.getenv('MYSQL_DATABASE')
        }
        
        # Check if query file exists
        query_file_path = 'Queries for Weekly Report.txt'
        if not os.path.exists(query_file_path):
            raise FileNotFoundError(f"Query file not found: {query_file_path}")
        
        logger.info("Reading query file...")
        with open(query_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # Initialize report generator before parsing queries
        logger.info("Initializing report generator...")
        generator = ReportGenerator(db_config)
        
        # Parse queries using the generator instance
        logger.info("Parsing queries...")
        queries = generator.parse_queries(content)
        
        logger.info(f"Successfully parsed {len(queries)} queries")
        for name in queries.keys():
            logger.info(f"Found query: {name}")
            logger.debug(f"Query content length: {len(queries[name])}")
        
        # Process each region and collect results
        results = {}
        for region in generator.regions:
            try:
                logger.info(f"Starting processing for region: {region}")
                region_result, region_data = generator.process_region(region, queries)
                results[region] = region_data
                logger.info(f"Successfully processed region: {region}")
            except Exception as e:
                logger.error(f"Failed to process region {region}: {str(e)}")
                raise

        # Verify results before updating Excel
        logger.info("\nVerifying results before Excel update:")
        logger.info(f"Number of regions processed: {len(results)}")
        
        if not results:
            raise ValueError("No results to process - results dictionary is empty")
        
        for region, data in results.items():
            logger.info(f"\nRegion: {region}")
            logger.info(f"Number of queries: {len(data)}")
            for query_name, df in data.items():
                logger.info(f"Query: {query_name}")
                logger.info(f"Data shape: {df.shape if isinstance(df, pd.DataFrame) else 'Not a DataFrame'}")
                if isinstance(df, pd.DataFrame):
                    logger.info(f"First few rows: \n{df.head().to_string()}")

        # Update the Excel template with results
        logger.info(f"\nStarting Excel template update with {len(results)} regions")
        generator.update_excel_template(template_path, results)
        logger.info("Excel template update completed successfully")
        
    except Exception as e:
        logger.error("Error during results verification and Excel update")
        logger.error(str(e))
        logger.error(traceback.format_exc())
        raise
    finally:
        logger.info("Script execution completed")

if __name__ == "__main__":
    main()