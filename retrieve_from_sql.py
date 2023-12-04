import csv
import mysql.connector

config = {
    'user': 'root',
    'password': '!$&)1725pZa',
    'host': '127.0.0.1',
    'database': 'yelp_business',
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

# query = "select rid, date, stars, text from review where business_id = '6ajnOk0GcY9xbb5Ocaw8Gw'"

#query = "select b.business_id, r.date, r.stars, b.is_open " \
#        "from business b " \
#        "left join review r on b.business_id = r.business_id " \
#        "where categories like '%Italian%' and categories like '%Restaurant%' " \
#        "and ST_Distance_Sphere( " \
#        "POINT(-75.1652, 39.9526), POINT(longitude,latitude) " \
#        ") <= 4828 -- 3 mile in meters"

# Get 1 year's worth of reviews for Branzino, which has a good mix of negative and positive reviews
#query = "select rid,stars,date,text from review r where business_id = '9_B5sCqKBOKDAmYpByiFFg' and DATEDIFF(" \
#        "'2009-01-01', date) * DATEDIFF('2009-01-01', date) < 365 * 365"

query = "select rid,stars,date,text from review r where business_id = 'MmN1qpG224QxOeq72DpN2g' and DATEDIFF(" \
        "'2013-01-01', date) * DATEDIFF('2013-01-01', date) < 365 * 365"

cursor.execute(query)

# Replace 'your_file.csv' with the path to the CSV file you want to create
with open('AldosItalian.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([i[0] for i in cursor.description])  # write headers
    csvwriter.writerows(cursor)

cursor.close()
cnx.close()
