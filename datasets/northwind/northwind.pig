register /usr/local/pig/lib/avro-*.jar;

categories = LOAD 'JSON/categories.json' USING JsonLoader('
    categoryid:int,
    categoryname:chararray,
    description:chararray,
    picture:bytearray');
store categories into 'JSON/categories' using JsonStorage();
store categories into 'CSV/categories' using PigStorage(',');
store categories into 'TSV/categories' using PigStorage('\t');
store categories into 'AVRO/categories' using AvroStorage();

customers = LOAD 'JSON/customers.json' USING JsonLoader('
    customerid:chararray,
    companyname:chararray,
    contactname:chararray,
    contacttitle:chararray,
    address:chararray,
    city:chararray,
    region:chararray,
    postalcode:chararray,
    country:chararray,
    phone:chararray,
    fax:chararray');
store customers into 'JSON/customers' using JsonStorage();
store customers into 'CSV/customers' using PigStorage(',');
store customers into 'TSV/customers' using PigStorage('\t');
store customers into 'AVRO/customers' using AvroStorage();

employees = LOAD 'JSON/employees.json' USING JsonLoader('
    employeeid:int,
    lastname:chararray,
    firstname:chararray,
    title:chararray,
    titleofcourtesy:chararray,
    birthdate:chararray,
    hiredate:chararray,
    address:chararray,
    city:chararray,
    region:chararray,
    postalcode:chararray,
    country:chararray,
    homephone:chararray,
    extension:chararray,
    photo:bytearray,
    notes:chararray,
    reportsto:int,
    photopath:chararray');
store employees into 'JSON/employees' using JsonStorage();
store employees into 'CSV/employees' using PigStorage(',');
store employees into 'TSV/employees' using PigStorage('\t');
store employees into 'AVRO/employees' using AvroStorage();

employeeterritories = LOAD 'JSON/employeeterritories.json' USING JsonLoader('
    employeeid:int,
    territoryid:chararray');
store employeeterritories into 'JSON/employeeterritories' using JsonStorage();
store employeeterritories into 'CSV/employeeterritories' using PigStorage(',');
store employeeterritories into 'TSV/employeeterritories' using PigStorage('\t');
store employeeterritories into 'AVRO/employeeterritories' using AvroStorage();

orderdetails = LOAD 'JSON/orderdetails.json' USING JsonLoader('
    orderid:int,
    productid:int,
    unitprice:double,
    quantity:int,
    discount:double');
store orderdetails into 'JSON/orderdetails' using JsonStorage();
store orderdetails into 'CSV/orderdetails' using PigStorage(',');
store orderdetails into 'TSV/orderdetails' using PigStorage('\t');
store orderdetails into 'AVRO/orderdetails' using AvroStorage();

orders = LOAD 'JSON/orders.json' USING JsonLoader('
    orderid:int,
    customerid:chararray,
    employeeid:int,
    orderdate:chararray,
    requireddate:chararray,
    shippeddate:chararray,
    shipvia:int,
    freight:double,
    shipname:chararray,
    shipaddress:chararray,
    shipcity:chararray,
    shipregion:chararray,
    shippostalcode:chararray,
    shipcountry:chararray');
store orders into 'JSON/orders' using JsonStorage();
store orders into 'CSV/orders' using PigStorage(',');
store orders into 'TSV/orders' using PigStorage('\t');
store orders into 'AVRO/orders' using AvroStorage();

products = LOAD 'JSON/products.json' USING JsonLoader('
    productid:int,
    productname:chararray,
    supplierid:int,
    categoryid:int,
    quantityperunit:chararray,
    unitprice:double,
    unitsinstock:int,
    unitsonorder:int,
    reorderlevel:int,
    discontinued:int');
store products into 'JSON/products' using JsonStorage();
store products into 'CSV/products' using PigStorage(',');
store products into 'TSV/products' using PigStorage('\t');
store products into 'AVRO/products' using AvroStorage();

regions = LOAD 'JSON/regions.json' USING JsonLoader('
    regionid:int,
    regiondescription:chararray');
store regions into 'JSON/regions' using JsonStorage();
store regions into 'CSV/regions' using PigStorage(',');
store regions into 'TSV/regions' using PigStorage('\t');
store regions into 'AVRO/regions' using AvroStorage();

shippers = LOAD 'JSON/shippers.json' USING JsonLoader('
    shipperid:int,
    companyname:chararray,
    phone:chararray');
store shippers into 'JSON/shippers' using JsonStorage();
store shippers into 'CSV/shippers' using PigStorage(',');
store shippers into 'TSV/shippers' using PigStorage('\t');
store shippers into 'AVRO/shippers' using AvroStorage();

suppliers = LOAD 'JSON/suppliers.json' USING JsonLoader('
    supplierid:int,
    companyname:chararray,
    contactname:chararray,
    contacttitle:chararray,
    address:chararray,
    city:chararray,
    region:chararray,
    postalcode:chararray,
    country:chararray,
    phone:chararray,
    fax:chararray,
    homepage:chararray');
store suppliers into 'JSON/suppliers' using JsonStorage();
store suppliers into 'CSV/suppliers' using PigStorage(',');
store suppliers into 'TSV/suppliers' using PigStorage('\t');
store suppliers into 'AVRO/suppliers' using AvroStorage();

territories = LOAD 'JSON/territories.json' USING JsonLoader('
    territoryid:chararray,
    territorydescription:chararray,
    regionid:int');
store territories into 'JSON/territories' using JsonStorage();
store territories into 'CSV/territories' using PigStorage(',');
store territories into 'TSV/territories' using PigStorage('\t');
store territories into 'AVRO/territories' using AvroStorage();

usstates = LOAD 'JSON/usstates.json' USING JsonLoader('
    stateid:int,
    statename:chararray,
    stateabbr:chararray,
    stateregion:chararray');
store usstates into 'JSON/usstates' using JsonStorage();
store usstates into 'CSV/usstates' using PigStorage(',');
store usstates into 'TSV/usstates' using PigStorage('\t');
store usstates into 'AVRO/usstates' using AvroStorage();
    
    
register /usr/local/pig/lib/piggybank.jar;
categories1 = load 'categories.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (doc:chararray);
categories2 = foreach categories1 GENERATE FLATTEN(REGEX_EXTRACT_ALL(doc,'<row>\\s*<categoryid>(.*)</categoryid>\\s*<categoryname>(.*)</categoryname>\\s*<description>(.*)</description>\\s*</row>')) AS (categoryid:int, categoryname:chararray, description:chararray);
dump categories2;


