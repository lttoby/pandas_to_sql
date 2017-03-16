from Database_Insert import *
import pandas
import numpy


# Type your own database authority
login = ["host", "user", "password", "database", "port"]

# Sample data to insert
dates = pandas.date_range('20130101', periods=6)
data_input = pandas.DataFrame(numpy.random.randn(6, 4), index=dates,columns=list('ABCD'))

# Create table and insert data, (with sub_key)
cols = data_input.columns
table_name = "table_test"
sub_key = "Name"
dbm = DatabaseInsert(login, table_name, sub_key)
dbm.create_table(cols)
dbm.insert_data(data_input, "Sunny")

# Create table and insert data, (without sub_key)
cols = data_input.columns
table_name = "table_test"
dbm = DatabaseInsert(login, table_name)
dbm.create_table(cols)
dbm.insert_data(data_input)
