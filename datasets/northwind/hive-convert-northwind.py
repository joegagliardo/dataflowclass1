from pyhive import hive
host='localhost'
port = 10000
user = 'student'
password = 'student'
database = 'northwind'
cn = hive.Connection(host=host, port=port, username = user, password = password, database=database, auth='CUSTOM')
cur  = cn.cursor()

tables = [
'categories'
,'orderdetails'
,'shippers'
,'customers'
,'orders'
,'suppliers'
,'employees'
,'products'
,'territories'
,'employeeterritories'
,'regions'
,'usstates'
]

from os import system as run
print('Starting')
for name, format in {'orc':'STORED AS ORC', 'parquet':'STORED AS PARQUET', 'avro':'STORED AS AVRO', 'json':"ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'"}.items():
   run(f'mkdir /class/datasets/northwind/{name.upper()}')
   for table in tables:
      run(f'mkdir /class/datasets/northwind/{name.upper()}/{table}')
      sql = f"create table {table}_{name} {format} as select * from {table}"
      print(sql)
      cur.execute(sql)
      run(f'hadoop fs -get /user/hive/warehouse/northwind.db/{table}_{name}/000000_0 /class/datasets/northwind/{name.upper()}/{table}/{table}.{name}')




 
