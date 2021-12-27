CREATE DATABASE northwind_json;
use northwind_json;

ADD JAR /usr/local/hive/hcatalog/share/hcatalog/hive-hcatalog-core.jar;

CREATE TABLE categories (
    categoryid smallint,
    categoryname varchar(15),
    description string
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE customers (
    customerid varchar(5),
    companyname varchar(40),
    contactname varchar(30),
    contacttitle varchar(30),
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postalcode varchar(10),
    country varchar(15),
    phone varchar(24),
    fax varchar(24)
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE employees (
    employeeid smallint,
    lastname varchar(20),
    firstname varchar(10),
    title varchar(30),
    titleofcourtesy varchar(25),
    birthdate date,
    hiredate date,
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postalcode varchar(10),
    country varchar(15),
    homephone varchar(24),
    extension varchar(4),
    photo binary,
    notes string,
    reportsto smallint,
    photopath varchar(255)
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE employeeterritories (
    employeeid smallint,
    territoryid varchar(20)
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE orderdetails (
    orderid smallint,
    productid smallint,
    unitprice decimal,
    quantity smallint,
    discount decimal
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE orders (
    orderid smallint,
    customerid varchar(5),
    employeeid smallint,
    orderdate date,
    requireddate date,
    shippeddate date,
    shipvia smallint,
    freight decimal,
    shipname varchar(40),
    shipaddress varchar(60),
    shipcity varchar(15),
    shipregion varchar(15),
    shippostalcode varchar(10),
    shipcountry varchar(15)
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE products (
    productid smallint,
    productname varchar(40),
    supplierid smallint,
    categoryid smallint,
    quantityperunit varchar(20),
    unitprice decimal,
    unitsinstock smallint,
    unitsonorder smallint,
    reorderlevel smallint,
    discontinued int
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE regions (
    regionid smallint,
    regiondescription string
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE shippers (
    shipperid smallint,
    companyname varchar(40),
    phone varchar(24)
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE suppliers (
    supplierid smallint,
    companyname varchar(40),
    contactname varchar(30),
    contacttitle varchar(30),
    address varchar(60),
    city varchar(15),
    region varchar(15),
    postalcode varchar(10),
    country varchar(15),
    phone varchar(24),
    fax varchar(24),
    homepage string
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE territories (
    territoryid varchar(20),
    territorydescription string,
    regionid smallint
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

CREATE TABLE usstates (
    stateid smallint,
    statename varchar(100),
    stateabbr varchar(2),
    stateregion varchar(50)
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/examples/northwind/JSON/categories' overwrite into table categories;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/customers' overwrite into table customers;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/employees' overwrite into table employees;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/employeeterritories' overwrite into table employeeterritories;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/orderdetails' overwrite into table orderdetails;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/orders' overwrite into table orders;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/products' overwrite into table products;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/regions' overwrite into table regions;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/shippers' overwrite into table shippers;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/suppliers' overwrite into table suppliers;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/territories' overwrite into table territories;
LOAD DATA LOCAL INPATH '/examples/northwind/JSON/usstates' overwrite into table usstates;

select * from categories;
select * from customers;
select * from employees;
select * from employeeterritories;
select * from orderdetails;
select * from orders;
select * from products;
select * from regions;
select * from shippers;
select * from suppliers;
select * from territories;
select * from usstates;
