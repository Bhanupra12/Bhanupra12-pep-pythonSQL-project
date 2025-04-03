import sys
import os
import unittest
import sqlite3

# Ensure the src directory is included in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import function from main.py
from src.main import load_and_clean_users

class ProjectTests(unittest.TestCase):

    def setUp(self):
        """Set up database connection before each test"""
        self.conn = sqlite3.connect(":memory:")  # Use in-memory database for testing
        self.cursor = self.conn.cursor()

        # Create users table
        self.cursor.execute('''CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)''')

        # Insert test data
        self.cursor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        self.cursor.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        self.conn.commit()

    def tearDown(self):
        """Close database connection after each test"""
        self.conn.close()
    def test_users_table_has_clean_data(self):
        """Test if users table contains exactly 2 valid records"""
        self.cursor.execute('SELECT * FROM users')
        results = self.cursor.fetchall()
        print(f"[DEBUG] Users table records: {results}")

        self.assertEqual(2, len(results), "[ERROR] Expected 2 valid records in users table.")

if __name__ == '__main__':
    unittest.main()