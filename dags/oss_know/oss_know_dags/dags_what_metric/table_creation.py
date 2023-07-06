import mysql.connector
from datetime import datetime


def create_table(cur):
    table_creation = (
        "CREATE TABLE `titles2` ("
        "  `emp_no` int(11) NOT NULL,"
        "  `title` varchar(50) NOT NULL,"
        "  `from_date` date NOT NULL,"
        "  `to_date` date DEFAULT NULL"
        ") ENGINE=InnoDB")
    cur.execute(table_creation)


with mysql.connector.connect(user='root', password='containerops123', host='192.168.8.60', database='oss_know',
                             port=3306) as cnx:
    with cnx.cursor() as cursor:
        # create_table(cursor)

        # insert_sql = '''
        # insert into titles (emp_no, title, from_date, to_date) values (%s, %s, %s, %s)
        # '''
        #
        # emp_data = [
        #     (1, 'Manager', datetime.now(), datetime.now()),
        #     (2, 'Engineer', datetime.now(), datetime.now()),
        #     (3, 'Operator', datetime.now(), datetime.now()),
        # ]
        # cursor.executemany(insert_sql, emp_data)
        # cnx.commit()

        cursor.execute('select emp_no, title from titles')
        for (emp_no, title) in cursor:
            print(emp_no, title)
