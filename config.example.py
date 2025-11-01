"""
Database and File Server Configuration Template
Copy this file to config.py and fill in your actual credentials
"""

# MySQL Database Configuration
DB_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your-database-host.com',
    'port': 3306,
    'database': 'your_database_name'
}

# File Server Configuration (SFTP)
FILE_SERVER_CONFIG = {
    'host': 'your_sftp_host',
    'port': 22,
    'username': 'your_username',
    'password': 'your_password',
    'upload_path': '/path/to/uploads'
}
