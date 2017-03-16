import MySQLdb


# The table will start with a DATETIME column, since we assumed you use pandas to analyse time-series data
# The sub-key is designed for the situation that you have multiple pandas data for different objects with same patterns
def create_table_command(cols, table_name, sub_key=False):
    sql_command_p1 = "CREATE TABLE `%s` (`DATETIME` DATETIME NOT NULL" % table_name
    sql_command_p2 = str()
    for col_i in cols:
        sql_command_p2 = "%s, `%s` FLOAT NULL" % (sql_command_p2, col_i)
    if sub_key:
        sql_command_p1 = "%s, `%s` VARCHAR(20) NOT NULL" % (sql_command_p1, sub_key)
        sql_command_p3 = ", PRIMARY KEY (`DATETIME`, `%s`));" % sub_key
    else:
        sql_command_p3 = ", PRIMARY KEY (`DATETIME`));"
    sql_command = sql_command_p1 + sql_command_p2 + sql_command_p3
    return sql_command


# Class Database for database connection and tables checking
# Initializing parameter is the database authorization information
class Database(object):

    def __init__(self, login_information):
        self.login_information = login_information
        self.table_list = self.tables_in_the_database()
        self.column_dictionary = self.columns_in_the_database()

    def tables_in_the_database(self):
        info = self.login_information
        my_connect = MySQLdb.connect(info[0], info[1], info[2], info[3], info[4])
        cursor = my_connect.cursor()
        sql_command = "SELECT TABLE_NAME FROM information_schema.tables where TABLE_SCHEMA = '%s'" % info[3]
        cursor.execute(sql_command)
        query_data = cursor.fetchall()
        table_list = list()
        for name_i in query_data:
            table_list.append(name_i[0])
        my_connect.close()
        cursor.close()
        return table_list

    def columns_in_the_database(self):
        info = self.login_information
        my_connect = MySQLdb.connect(info[0], info[1], info[2], info[3], info[4])
        cursor = my_connect.cursor()
        sql_command = "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS where TABLE_SCHEMA = '%s'" % info[3]
        column_dictionary = dict()
        cursor.execute(sql_command)
        query_data = cursor.fetchall()
        for column_i in query_data:
            table_name = column_i[0]
            column_name = column_i[1]
            if table_name not in column_dictionary.keys():
                column_dictionary[table_name] = list()
            column_dictionary[table_name].append(column_name)
        my_connect.close()
        cursor.close()
        return column_dictionary


# Class DatabaseInsert for inserting data into table(s)
class DatabaseInsert(Database):

    def __init__(self, login_information, table_name, sub_key=False):
        Database.__init__(self, login_information)
        self.table_name = table_name
        self.sub_key = sub_key
        self.table_add = list()
        self.columns_add_list = list()

    def create_table(self, cols):
        if len(cols) < 800:
            if self.table_name in self.table_list:
                print "Table %s already existed" % self.table_name
            else:
                info = self.login_information
                my_connect = MySQLdb.connect(info[0], info[1], info[2], info[3], info[4])
                cursor = my_connect.cursor()
                sql_command = create_table_command(cols, self.table_name, self.sub_key)
                cursor.execute(sql_command)
                my_connect.close()
                cursor.close()
                print "Table %s created" % self.table_name
            self.table_add.append(self.table_name)
            self.columns_add_list.append(cols)
        else:
            tables_num = (len(cols) - 1) / 800 + 1
            potential_name = list()
            for i in range(tables_num):
                table_name_i = "%s_%s" % (self.table_name, str(i+1).zfill(2))
                potential_name.append(table_name_i)

            start = 0
            end = 800
            for table_i in potential_name:
                if table_i in self.table_list:
                    print "Table %s already existed" % table_i
                    col_add_i = cols[start:end]
                else:
                    info = self.login_information
                    my_connect = MySQLdb.connect(info[0], info[1], info[2], info[3], info[4])
                    cursor = my_connect.cursor()
                    col_add_i = cols[start:end]
                    sql_command = create_table_command(col_add_i, table_i, sub_key)
                    cursor.execute(sql_command)
                    my_connect.close()
                    cursor.close()
                    print "Table %s created" % table_i
                self.table_add.append(table_i)
                self.columns_add_list.append(col_add_i)
                start += 800
                end += 800

    def insert_data(self, data_input, sub_key_data=False):
        info = self.login_information
        my_connect = MySQLdb.connect(info[0], info[1], info[2], info[3], info[4])
        cursor = my_connect.cursor()
        if self.sub_key:
            for i in range(len(self.table_add)):
                sql_command = "INSERT INTO %s (`DATETIME`, `%s`" % (self.table_add[i], self.sub_key)
                col_i_list = self.columns_add_list[i]
                date_data = list(data_input.index)
                sub_key_column = [sub_key_data] * len(data_input)
                main_data = map(list, zip(*data_input[col_i_list].values))
                data_insert = [date_data] + [sub_key_column] + main_data
                data_insert = map(list, zip(*data_insert))

                for col_i in col_i_list:
                    sql_command = "%s, `%s`" % (sql_command, col_i)
                sql_command += ") VALUES (%s" + ", %s" * (len(col_i_list) + 1) + ")"
                cursor.executemany(sql_command, data_insert)
                my_connect.commit()
        else:
            for i in range(len(self.table_add)):
                sql_command = "INSERT INTO %s (`DATETIME`" % self.table_add[i]
                col_i_list = self.columns_add_list[i]
                date_data = list(data_input.index)
                main_data = map(list, zip(*data_input[col_i_list].values))
                data_insert = [date_data] + main_data
                data_insert = map(list, zip(*data_insert))

                for col_i in col_i_list:
                    sql_command = "%s, `%s`" % (sql_command, col_i)
                sql_command += ") VALUES (%s" + ", %s" * len(col_i_list) + ")"
                cursor.executemany(sql_command, data_insert)
                my_connect.commit()
        my_connect.close()
        cursor.close()

