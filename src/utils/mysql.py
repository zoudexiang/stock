import pymysql


def connect():
    cnx = pymysql.connect(host="127.0.0.1",
                          port=3306,
                          user="root",
                          password="000000",
                          database='stock'
                         )
    return cnx

# cnx = connect()
# # Get a cursor
# cur = cnx.cursor()
#
# # Execute a query
# cur.execute("""
# truncate snapshot
# """)
#
# # Fetch one result
# row = cur.fetchone()
# print("Current date is: {0}".format(row[0]))
#
# # Close connection
# cnx.close()
