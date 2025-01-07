# Database Query Automation with GitHub Actions

This repository contains an automated solution for securely querying a MySQL database through SSH tunneling using GitHub Actions. The automation runs daily and on-demand, fetching data and storing results as CSV files.

## Architecture Overview

```
GitHub Actions → SSH Tunnel → MySQL Database
    ↓
CSV Results → GitHub Artifacts
```

## Technical Components

### 1. Database Connection Flow
- **SSH Tunnel**: Creates a secure tunnel from GitHub Actions runner to database server
- **Port Forwarding**: Maps local port 3307 to remote MySQL port 3306
- **Connection Security**: Utilizes SSH keys and environment variables for secure authentication

### 2. Core Components

#### Python Script (`script.py`)
- **SSH Tunnel Setup**:
  - Creates background SSH process
  - Establishes port forwarding
  - Handles tunnel lifecycle management
  
- **Database Operations**:
  - Connects to MySQL via tunneled connection
  - Executes predefined queries
  - Exports results to CSV format
  
- **Error Handling**:
  - Comprehensive logging
  - Connection timeout management
  - Proper resource cleanup

#### GitHub Actions Workflow (`database-query.yml`)
- **Trigger Methods**:
  - Scheduled: Daily at midnight (UTC)
  - Manual: Via workflow_dispatch
  
- **Environment Setup**:
  - Python 3.9 runtime
  - Required packages: mysql-connector-python, pandas
  - SSH configuration and key management
  
- **Security**:
  - Secure handling of credentials via GitHub Secrets
  - Proper SSH key permissions (600)
  - Known hosts verification

### 3. Required Secrets

The following secrets need to be configured in GitHub repository settings:
- `SSH_PRIVATE_KEY`: SSH private key for tunnel authentication
- `SSH_HOST`: SSH server hostname
- `SSH_USERNAME`: SSH username
- `MYSQL_HOST`: MySQL server hostname
- `MYSQL_USER`: Database username
- `MYSQL_PASSWORD`: Database password
- `MYSQL_DATABASE`: Target database name

### 4. Output Artifacts

- CSV files containing query results
- Naming format: `query_results_YYYYMMDD.csv`
- Retention period: 5 days

## Usage

### Manual Execution
1. Go to the "Actions" tab in GitHub
2. Select "Run Database Query" workflow
3. Click "Run workflow"

### Automated Execution
- Runs automatically every day at midnight (UTC)
- Results are available in the workflow run artifacts

## Error Handling

The system includes comprehensive error handling for:
- SSH tunnel connection failures
- Database connection issues
- Query execution errors
- Resource cleanup
- All errors are logged with detailed information for debugging

## Logging

Detailed logging is implemented for:
- SSH tunnel establishment
- Database connections
- Query execution
- Error states
- Resource cleanup

## Best Practices Implemented

1. Secure credential management
2. Proper resource cleanup
3. Comprehensive error handling
4. Detailed logging
5. Automated execution with manual override
6. Artifact retention management