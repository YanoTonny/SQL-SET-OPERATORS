import pandas as pd
import mysql.connector

# Load the CSV file into a DataFrame
file_path = r'C:\Users\Tyano\Desktop\Important docs\netflix_titles.csv'
netflix_data = pd.read_csv(file_path)

# Preprocess the date format in the DataFrame
netflix_data['date_added'] = pd.to_datetime(netflix_data['date_added'], errors='coerce').dt.strftime('%Y-%m-%d')

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='your_cdusername',
    password='your_password',
    database='netflix_db'
)
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS netflix_titles (
    show_id VARCHAR(10),
    type VARCHAR(10),
    title VARCHAR(255),
    director VARCHAR(255),
    cast TEXT,
    country VARCHAR(255),
    date_added DATE,
    release_year INT,
    rating VARCHAR(10),
    duration VARCHAR(50),
    listed_in TEXT,
    description TEXT
);
''')

# Insert data into the table
for index, row in netflix_data.iterrows():
    cursor.execute('''
    INSERT INTO netflix_titles (show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        type=VALUES(type), title=VALUES(title), director=VALUES(director), cast=VALUES(cast),
        country=VALUES(country), date_added=VALUES(date_added), release_year=VALUES(release_year),
        rating=VALUES(rating), duration=VALUES(duration), listed_in=VALUES(listed_in), description=VALUES(description)
    ''', tuple(row))

# Commit and close the connection
conn.commit()
cursor.close()
conn.close()
