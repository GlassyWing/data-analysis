import psycopg2

conn = psycopg2.connect(database="testdb", host="172.18.130.101", user="gpadmin", password="gpadmin", port="5432")

cur = conn.cursor()

cur.execute("SELECT id, name, age  from testtable1")
rows = cur.fetchall()

for row in rows:
   print("ID = ", row[0])
   print("NAME = ", row[1])
   print("AGE = ", row[2])