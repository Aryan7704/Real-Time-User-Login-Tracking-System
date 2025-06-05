import mysql.connector

def dbconnection(data_list):
    
    connection = mysql.connector.connect(
        host="mysql",
        user="user",
        password="password",
        database='userdb'
    )
    # print(data_list)

    cursor = connection.cursor()
    insert_sql = 'INSERT INTO logins (user_id, login_location, login_date, topic) VALUES (%s, %s, %s, %s)'
    values = [(d["user_id"], d["login_location"], d["date_time"], d['topic_name']) for d in data_list]
    cursor.executemany(insert_sql , values)
    connection.commit()

    # cursor.execute("select * from logins;")
    # record = cursor.fetchall()

    # print(record)

    cursor.close()
    connection.close()


