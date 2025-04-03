import sqlite3
import csv
import os

def return_cursor(conn):
    return conn.cursor()

def create_tables(conn):
    cursor = return_cursor(conn)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            userId INTEGER PRIMARY KEY,
            firstName TEXT,
            lastName TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS callLogs (
            callId INTEGER PRIMARY KEY,
            phoneNumber TEXT,
            startTimeEpoch INTEGER,
            endTimeEpoch INTEGER,
            callDirection TEXT,
            userId INTEGER,
            FOREIGN KEY (userId) REFERENCES users(userId)
        )
    ''')
    conn.commit()

def load_and_clean_users(file_path, conn):
    cursor = return_cursor(conn)
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if len(row) == 2 and all(field.strip() for field in row):
                cursor.execute("INSERT INTO users (firstName, lastName) VALUES (?, ?)", (row[0], row[1]))
    conn.commit()

def load_and_clean_call_logs(file_path, conn):
    cursor = return_cursor(conn)
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if len(row) == 5 and all(row[:3]) and row[4].strip():
                try:
                    start_time = int(row[1])
                    end_time = int(row[2])
                    user_id = int(row[4])
                    cursor.execute("""
                        INSERT INTO callLogs (phoneNumber, startTimeEpoch, endTimeEpoch, callDirection, userId)
                        VALUES (?, ?, ?, ?, ?)
                    """, (row[0], start_time, end_time, row[3], user_id))
                except ValueError:
                    continue
    conn.commit()

def write_ordered_calls(file_path, conn):
    cursor = return_cursor(conn)
    cursor.execute("""
        SELECT callId, phoneNumber, startTimeEpoch, endTimeEpoch, callDirection, userId 
        FROM callLogs 
        ORDER BY userId, startTimeEpoch
    """)
    ordered_logs = cursor.fetchall()

    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTimeEpoch', 'endTimeEpoch', 'callDirection', 'userId'])
        for row in ordered_logs:
            writer.writerow(row)

def write_user_analytics(file_path, conn):
    cursor = return_cursor(conn)
    cursor.execute("""
        SELECT userId, 
               AVG(endTimeEpoch - startTimeEpoch) AS avgDuration, 
               COUNT(*) AS numCalls 
        FROM callLogs 
        GROUP BY userId
    """)
    analytics = cursor.fetchall()

    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])
        for row in analytics:
            writer.writerow([row[0], round(row[1], 1), row[2]])

def main():
    conn = sqlite3.connect(':memory:')
    create_tables(conn)

    # Go up two levels from src/main/ to reach project root
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(file))))
    resources_dir = os.path.join(base_dir, 'resources')
    
    users_file = os.path.join(resources_dir, 'users.csv')
    call_logs_file = os.path.join(resources_dir, 'callLogs.csv')
    user_analytics_file = os.path.join(resources_dir, 'userAnalytics.csv')
    ordered_call_logs_file = os.path.join(resources_dir, 'orderedCallLogs.csv')

    load_and_clean_users(users_file, conn)
    load_and_clean_call_logs(call_logs_file, conn)
    write_user_analytics(user_analytics_file, conn)
    write_ordered_calls(ordered_call_logs_file, conn)

    conn.close()

if __name__ == "main":
   main()