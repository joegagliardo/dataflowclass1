--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.3
-- Dumped by pg_dump version 9.6.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: unaccent; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS unaccent WITH SCHEMA public;


--
-- Name: EXTENSION unaccent; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION unaccent IS 'text search dictionary that removes accents';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: categories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE categories (
    categoryid smallint NOT NULL,
    categoryname character varying(15) NOT NULL,
    description text,
    picture bytea
);


--
-- Name: customers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE customers (
    customerid bpchar NOT NULL,
    companyname character varying(40) NOT NULL,
    contactname character varying(30),
    contacttitle character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postalcode character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24)
);


--
-- Name: employees; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE employees (
    employeeid smallint NOT NULL,
    lastname character varying(20) NOT NULL,
    firstname character varying(10) NOT NULL,
    title character varying(30),
    titleofcourtesy character varying(25),
    birthdate date,
    hiredate date,
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postalcode character varying(10),
    country character varying(15),
    homephone character varying(24),
    extension character varying(4),
    photo bytea,
    notes text,
    reportsto smallint,
    photopath character varying(255)
);


--
-- Name: employeeterritories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE employeeterritories (
    employeeid smallint NOT NULL,
    territoryid character varying(20) NOT NULL
);


--
-- Name: order_details; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE order_details (
    orderid smallint NOT NULL,
    productid smallint NOT NULL,
    unitprice real NOT NULL,
    quantity smallint NOT NULL,
    discount real NOT NULL
);


--
-- Name: orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE orders (
    orderid smallint NOT NULL,
    customerid bpchar,
    employeeid smallint,
    orderdate date,
    requireddate date,
    shippeddate date,
    shipvia smallint,
    freight real,
    shipname character varying(40),
    shipaddress character varying(60),
    shipcity character varying(15),
    shipregion character varying(15),
    shippostalcode character varying(10),
    shipcountry character varying(15)
);


--
-- Name: products; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE products (
    productid smallint NOT NULL,
    productname character varying(40) NOT NULL,
    supplierid smallint,
    categoryid smallint,
    quantityperunit character varying(20),
    unitprice real,
    unitsinstock smallint,
    unitsonorder smallint,
    reorderlevel smallint,
    discontinued integer NOT NULL
);


--
-- Name: regions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE regions (
    regionid smallint NOT NULL,
    regiondescription bpchar NOT NULL
);


--
-- Name: shippers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE shippers (
    shipperid smallint NOT NULL,
    companyname character varying(40) NOT NULL,
    phone character varying(24)
);


--
-- Name: suppliers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE suppliers (
    supplierid smallint NOT NULL,
    companyname character varying(40) NOT NULL,
    contactname character varying(30),
    contacttitle character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postalcode character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24),
    homepage text
);


--
-- Name: territories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE territories (
    territoryid character varying(20) NOT NULL,
    territorydescription bpchar NOT NULL,
    regionid smallint NOT NULL
);


--
-- Name: usstates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE usstates (
    stateid smallint NOT NULL,
    statename character varying(100),
    stateabbr character varying(2),
    stateregion character varying(50)
);


--
-- Name: categories pk_categories; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY categories
    ADD CONSTRAINT pk_categories PRIMARY KEY (categoryid);


--
-- Name: customers pk_customers; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY customers
    ADD CONSTRAINT pk_customers PRIMARY KEY (customerid);


--
-- Name: employees pk_employees; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY employees
    ADD CONSTRAINT pk_employees PRIMARY KEY (employeeid);


--
-- Name: employeeterritories pk_employeeterritories; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY employeeterritories
    ADD CONSTRAINT pk_employeeterritories PRIMARY KEY (employeeid, territoryid);


--
-- Name: order_details pk_order_details; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY order_details
    ADD CONSTRAINT pk_order_details PRIMARY KEY (orderid, productid);


--
-- Name: orders pk_orders; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (orderid);


--
-- Name: products pk_products; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY products
    ADD CONSTRAINT pk_products PRIMARY KEY (productid);


--
-- Name: regions pk_region; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY regions
    ADD CONSTRAINT pk_region PRIMARY KEY (regionid);


--
-- Name: shippers pk_shippers; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY shippers
    ADD CONSTRAINT pk_shippers PRIMARY KEY (shipperid);


--
-- Name: suppliers pk_suppliers; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY suppliers
    ADD CONSTRAINT pk_suppliers PRIMARY KEY (supplierid);


--
-- Name: territories pk_territories; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY territories
    ADD CONSTRAINT pk_territories PRIMARY KEY (territoryid);


--
-- Name: employees fk_employees_employees; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY employees
    ADD CONSTRAINT fk_employees_employees FOREIGN KEY (reportsto) REFERENCES employees(employeeid);


--
-- Name: employeeterritories fk_employeeterritories_employees; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY employeeterritories
    ADD CONSTRAINT fk_employeeterritories_employees FOREIGN KEY (employeeid) REFERENCES employees(employeeid);


--
-- Name: employeeterritories fk_employeeterritories_territories; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY employeeterritories
    ADD CONSTRAINT fk_employeeterritories_territories FOREIGN KEY (territoryid) REFERENCES territories(territoryid);


--
-- Name: order_details fk_order_details_orders; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (orderid) REFERENCES orders(orderid);


--
-- Name: order_details fk_order_details_products; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_products FOREIGN KEY (productid) REFERENCES products(productid);


--
-- Name: orders fk_orders_customers; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customerid) REFERENCES customers(customerid);


--
-- Name: orders fk_orders_employees; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_employees FOREIGN KEY (employeeid) REFERENCES employees(employeeid);


--
-- Name: orders fk_orders_shippers; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_shippers FOREIGN KEY (shipvia) REFERENCES shippers(shipperid);


--
-- Name: products fk_products_categories; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_categories FOREIGN KEY (categoryid) REFERENCES categories(categoryid);


--
-- Name: products fk_products_suppliers; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_suppliers FOREIGN KEY (supplierid) REFERENCES suppliers(supplierid);


--
-- Name: territories fk_territories_region; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY territories
    ADD CONSTRAINT fk_territories_region FOREIGN KEY (regionid) REFERENCES regions(regionid);


--
-- Name: categories; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE categories TO northwind_user;


--
-- Name: customers; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE customers TO northwind_user;


--
-- Name: employees; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE employees TO northwind_user;


--
-- Name: employeeterritories; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE employeeterritories TO northwind_user;


--
-- Name: order_details; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE order_details TO northwind_user;


--
-- Name: orders; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE orders TO northwind_user;


--
-- Name: products; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE products TO northwind_user;


--
-- Name: regions; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE regions TO northwind_user;


--
-- Name: shippers; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE shippers TO northwind_user;


--
-- Name: suppliers; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE suppliers TO northwind_user;


--
-- Name: territories; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE territories TO northwind_user;


--
-- Name: usstates; Type: ACL; Schema: public; Owner: -
--

GRANT ALL ON TABLE usstates TO northwind_user;


--
-- PostgreSQL database dump complete
--

