# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
df_boston = pd.read_sql("""SELECT firstName, lastName, jobTitle FROM employees JOIN offices ON employees.officeCode = offices.officeCode WHERE offices.city = 'Boston'""", conn)

# STEP 2
df_zero_emp = pd.read_sql("""SELECT offices.officeCode, offices.city FROM offices LEFT JOIN employees ON offices.officeCode = employees.officeCode GROUP BY offices.officeCode, offices.city HAVING COUNT(employees.employeeNumber) = 0""", conn)

# STEP 3
df_employee = pd.read_sql("""SELECT firstName, lastName, city, state FROM employees LEFT JOIN offices ON employees.officeCode = offices.officeCode ORDER BY firstName, lastName""", conn)

# STEP 4
df_contacts = pd.read_sql("""SELECT contactFirstName, contactLastName, phone, salesRepEmployeeNumber FROM customers LEFT JOIN orders ON customers.customerNumber = orders.customerNumber WHERE orders.orderNumber IS NULL ORDER BY contactLastName""", conn)

# STEP 5
df_payment = pd.read_sql("""SELECT contactFirstName, contactLastName, amount, paymentDate FROM customers JOIN payments ON customers.customerNumber = payments.customerNumber ORDER BY CAST(amount AS REAL) DESC""", conn)

# STEP 6
df_credit = pd.read_sql("""SELECT employees.employeeNumber, firstName, lastName, COUNT(customers.customerNumber) as num_customers FROM employees JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber GROUP BY employees.employeeNumber, firstName, lastName HAVING AVG(customers.creditLimit) > 90000 ORDER BY num_customers DESC""", conn)

# STEP 7
df_product_sold = pd.read_sql("""SELECT productName, COUNT(orderdetails.orderNumber) as numorders, SUM(quantityOrdered) as totalunits FROM products JOIN orderdetails ON products.productCode = orderdetails.productCode GROUP BY productName ORDER BY totalunits DESC""", conn)

# STEP 8
df_total_customers = pd.read_sql("""SELECT productName, products.productCode, COUNT(DISTINCT orders.customerNumber) as numpurchasers FROM products JOIN orderdetails ON products.productCode = orderdetails.productCode JOIN orders ON orderdetails.orderNumber = orders.orderNumber GROUP BY productName, products.productCode ORDER BY numpurchasers DESC""", conn)

# STEP 9
df_customers = pd.read_sql("""SELECT offices.officeCode, offices.city, COUNT(customers.customerNumber) as n_customers FROM offices JOIN employees ON offices.officeCode = employees.officeCode JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber GROUP BY offices.officeCode, offices.city""", conn)

# STEP 10
df_under_20 = pd.read_sql("""SELECT employeeNumber, firstName, lastName, city, offices.officeCode FROM employees JOIN offices ON employees.officeCode = offices.officeCode WHERE employeeNumber IN (SELECT DISTINCT employees.employeeNumber FROM employees JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber JOIN orders ON customers.customerNumber = orders.customerNumber JOIN orderdetails ON orders.orderNumber = orderdetails.orderNumber WHERE orderdetails.productCode IN (SELECT orderdetails.productCode FROM orderdetails JOIN orders ON orderdetails.orderNumber = orders.orderNumber GROUP BY orderdetails.productCode HAVING COUNT(DISTINCT orders.customerNumber) < 20))""", conn)

conn.close()