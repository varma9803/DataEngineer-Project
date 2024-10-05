import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="newpassword",
    database="walmart"
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM store_data LIMIT 10")

rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()

