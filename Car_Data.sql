--Create Actual Table--
CREATE TABLE CarData (
    ID NUMBER PRIMARY KEY,
    Car_Name VARCHAR2(50),
    Year NUMBER,
    Selling_Price NUMBER,
    Present_Price NUMBER,
    Kms_Driven NUMBER,
    Fuel_Type VARCHAR2(10),
    Seller_Type VARCHAR2(10),
    Transmission VARCHAR2(10),
    Owner NUMBER
);

CREATE SEQUENCE CarData_seq
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

CREATE OR REPLACE TRIGGER CarData_before_insert
BEFORE INSERT ON CarData
FOR EACH ROW
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT CarData_seq.NEXTVAL
        INTO :NEW.ID
        FROM dual;
    END IF;
END;
/

--Create External Table--
CREATE TABLE CarData_ext (
    Car_Name VARCHAR2(50),
    Year NUMBER,
    Selling_Price NUMBER,
    Present_Price NUMBER,
    Kms_Driven NUMBER,
    Fuel_Type VARCHAR2(10),
    Seller_Type VARCHAR2(10),
    Transmission VARCHAR2(10),
    Owner NUMBER
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY my_dir
    ACCESS PARAMETERS (
        records delimited by newline
        fields terminated by ',' optionally enclosed by '"'
    )
    LOCATION ('cardata.csv')
)
REJECT LIMIT UNLIMITED;

--Create Directory Object--
CREATE OR REPLACE DIRECTORY my_dir AS 'C:\Users\HUAWEI\OneDrive\Desktop\SQL\Assignment1';

--Insert Data into Internal/Actual Table--
INSERT INTO CarData (Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner)
SELECT Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner
FROM CarData_ext;

------------------------------------------------------------------------------------------------------------------------------------------

--Create Car--

--Before Trigger--
CREATE OR REPLACE TRIGGER before_insert_car
BEFORE INSERT ON CarData
FOR EACH ROW
BEGIN
    -- Validate the data
    IF :NEW.Year < 1886 OR :NEW.Year > EXTRACT(YEAR FROM SYSDATE) THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid Year. Please enter a valid year.');
    END IF;
    
    IF :NEW.Selling_Price < 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Selling Price cannot be negative.');
    END IF;
    
    IF :NEW.Present_Price < 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Present Price cannot be negative.');
    END IF;
END;
/

--After Trigger--
CREATE OR REPLACE TRIGGER after_insert_car
AFTER INSERT ON CarData
FOR EACH ROW
BEGIN
    DBMS_OUTPUT.PUT_LINE('New car record inserted: ' || :NEW.Car_Name);
END;
/

CREATE OR REPLACE PROCEDURE create_car(
    p_car_name IN VARCHAR2,
    p_year IN NUMBER,
    p_selling_price IN NUMBER,
    p_present_price IN NUMBER,
    p_kms_driven IN NUMBER,
    p_fuel_type IN VARCHAR2,
    p_seller_type IN VARCHAR2,
    p_transmission IN VARCHAR2,
    p_owner IN NUMBER
) AS
BEGIN
    SAVEPOINT before_insert;
    
    BEGIN
        INSERT INTO CarData (Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner)
        VALUES (p_car_name, p_year, p_selling_price, p_present_price, p_kms_driven, p_fuel_type, p_seller_type, p_transmission, p_owner);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Record created successfully.');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK TO before_insert;
            DBMS_OUTPUT.PUT_LINE('Error creating record: ' || SQLERRM);
    END;
END;
/
SET SERVEROUTPUT ON

-- Prompt user for input--
ACCEPT car_name PROMPT 'Enter Car Name: '
ACCEPT year PROMPT 'Enter Year: '
ACCEPT selling_price PROMPT 'Enter Selling Price: '
ACCEPT present_price PROMPT 'Enter Present Price: '
ACCEPT kms_driven PROMPT 'Enter Kms Driven: '
ACCEPT fuel_type PROMPT 'Enter Fuel Type: '
ACCEPT seller_type PROMPT 'Enter Seller Type: '
ACCEPT transmission PROMPT 'Enter Transmission: '
ACCEPT owner PROMPT 'Enter Owner: '

-- Execute the procedure with the provided values--
BEGIN
    create_car(
        '&car_name',
        &year,
        &selling_price,
        &present_price,
        &kms_driven,
        '&fuel_type',
        '&seller_type',
        '&transmission',
        &owner
    );
END;
/

------------------------------------------------------------------------------------------------------------------------------------------

--Get all cars--
CREATE OR REPLACE PROCEDURE get_all_cars AS
    CURSOR car_cursor IS
        SELECT Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner
        FROM CarData;
        
    v_car_name CarData.Car_Name%TYPE;
    v_year CarData.Year%TYPE;
    v_selling_price CarData.Selling_Price%TYPE;
    v_present_price CarData.Present_Price%TYPE;
    v_kms_driven CarData.Kms_Driven%TYPE;
    v_fuel_type CarData.Fuel_Type%TYPE;
    v_seller_type CarData.Seller_Type%TYPE;
    v_transmission CarData.Transmission%TYPE;
    v_owner CarData.Owner%TYPE;
BEGIN
    OPEN car_cursor;
    
    LOOP
        FETCH car_cursor INTO v_car_name, v_year, v_selling_price, v_present_price, v_kms_driven, v_fuel_type, v_seller_type, v_transmission, v_owner;
        
        EXIT WHEN car_cursor%NOTFOUND;
        
        DBMS_OUTPUT.PUT_LINE('Car Name: ' || v_car_name);
        DBMS_OUTPUT.PUT_LINE('Year: ' || v_year);
        DBMS_OUTPUT.PUT_LINE('Selling Price: ' || v_selling_price);
        DBMS_OUTPUT.PUT_LINE('Present Price: ' || v_present_price);
        DBMS_OUTPUT.PUT_LINE('Kms Driven: ' || v_kms_driven);
        DBMS_OUTPUT.PUT_LINE('Fuel Type: ' || v_fuel_type);
        DBMS_OUTPUT.PUT_LINE('Seller Type: ' || v_seller_type);
        DBMS_OUTPUT.PUT_LINE('Transmission: ' || v_transmission);
        DBMS_OUTPUT.PUT_LINE('Owner: ' || v_owner);
        DBMS_OUTPUT.PUT_LINE('-------------------------------');
    END LOOP;
    
    CLOSE car_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error retrieving data: ' || SQLERRM);
END;
/

------------------------------------------------------------------------------------------------------------------------------------------

--Retrieve Car with Name--
CREATE OR REPLACE PROCEDURE read_car(p_car_name IN VARCHAR2) AS
    v_car_name CarData.Car_Name%TYPE;
    v_year CarData.Year%TYPE;
    v_selling_price CarData.Selling_Price%TYPE;
    v_present_price CarData.Present_Price%TYPE;
    v_kms_driven CarData.Kms_Driven%TYPE;
    v_fuel_type CarData.Fuel_Type%TYPE;
    v_seller_type CarData.Seller_Type%TYPE;
    v_transmission CarData.Transmission%TYPE;
    v_owner CarData.Owner%TYPE;
BEGIN
    SELECT Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner
    INTO v_car_name, v_year, v_selling_price, v_present_price, v_kms_driven, v_fuel_type, v_seller_type, v_transmission, v_owner
    FROM CarData
    WHERE Car_Name = p_car_name;

    DBMS_OUTPUT.PUT_LINE('Car Name: ' || v_car_name);
    DBMS_OUTPUT.PUT_LINE('Year: ' || v_year);
    DBMS_OUTPUT.PUT_LINE('Selling Price: ' || v_selling_price);
    DBMS_OUTPUT.PUT_LINE('Present Price: ' || v_present_price);
    DBMS_OUTPUT.PUT_LINE('Kms Driven: ' || v_kms_driven);
    DBMS_OUTPUT.PUT_LINE('Fuel Type: ' || v_fuel_type);
    DBMS_OUTPUT.PUT_LINE('Seller Type: ' || v_seller_type);
    DBMS_OUTPUT.PUT_LINE('Transmission: ' || v_transmission);
    DBMS_OUTPUT.PUT_LINE('Owner: ' || v_owner);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No record found for car name: ' || p_car_name);
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error reading record: ' || SQLERRM);
END;
/

------------------------------------------------------------------------------------------------------------------------------------------

--Update Car--

--Before Trigger--
CREATE OR REPLACE TRIGGER before_update_car
BEFORE UPDATE ON CarData
FOR EACH ROW
BEGIN
    -- Validate the data
    IF :NEW.Year < 1886 OR :NEW.Year > EXTRACT(YEAR FROM SYSDATE) THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid Year. Please enter a valid year.');
    END IF;
    
    IF :NEW.Selling_Price < 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Selling Price cannot be negative.');
    END IF;
    
    IF :NEW.Present_Price < 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Present Price cannot be negative.');
    END IF;
END;
/

--After Trigger--
CREATE OR REPLACE TRIGGER after_update_car
AFTER UPDATE ON CarData
FOR EACH ROW
BEGIN
    DBMS_OUTPUT.PUT_LINE('Car record updated: ' || :NEW.Car_Name);
END;
/

CREATE OR REPLACE PROCEDURE update_car(
    p_id IN NUMBER,
    p_car_name IN VARCHAR2,
    p_year IN NUMBER,
    p_selling_price IN NUMBER,
    p_present_price IN NUMBER,
    p_kms_driven IN NUMBER,
    p_fuel_type IN VARCHAR2,
    p_seller_type IN VARCHAR2,
    p_transmission IN VARCHAR2,
    p_owner IN NUMBER
) AS
BEGIN
    SAVEPOINT before_update;

    BEGIN
        UPDATE CarData
        SET Car_Name = p_car_name,
            Year = p_year,
            Selling_Price = p_selling_price,
            Present_Price = p_present_price,
            Kms_Driven = p_kms_driven,
            Fuel_Type = p_fuel_type,
            Seller_Type = p_seller_type,
            Transmission = p_transmission,
            Owner = p_owner
        WHERE ID = p_id;

        IF SQL%ROWCOUNT = 0 THEN
            ROLLBACK TO before_update;
            DBMS_OUTPUT.PUT_LINE('No record found for ID: ' || p_id);
        ELSE
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Record updated successfully.');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK TO before_update;
            DBMS_OUTPUT.PUT_LINE('Error updating record: ' || SQLERRM);
    END;
END;
/
SET SERVEROUTPUT ON

-- Prompt user for input
ACCEPT id PROMPT 'Enter ID of the record to update: '
ACCEPT car_name PROMPT 'Enter new Car Name: '
ACCEPT year PROMPT 'Enter new Year: '
ACCEPT selling_price PROMPT 'Enter new Selling Price: '
ACCEPT present_price PROMPT 'Enter new Present Price: '
ACCEPT kms_driven PROMPT 'Enter new Kms Driven: '
ACCEPT fuel_type PROMPT 'Enter new Fuel Type: '
ACCEPT seller_type PROMPT 'Enter new Seller Type: '
ACCEPT transmission PROMPT 'Enter new Transmission: '
ACCEPT owner PROMPT 'Enter new Owner: '

-- Execute the procedure with the provided values
BEGIN
    update_car(
        &id,
        '&car_name',
        &year,
        &selling_price,
        &present_price,
        &kms_driven,
        '&fuel_type',
        '&seller_type',
        '&transmission',
        &owner
    );
END;
/

------------------------------------------------------------------------------------------------------------------------------------------

--Delete Car--

--Before Trigger--
CREATE OR REPLACE TRIGGER before_delete_car
BEFORE DELETE ON CarData
FOR EACH ROW
BEGIN
    -- Validate the data
    IF :OLD.Selling_Price > 100000 THEN
        RAISE_APPLICATION_ERROR(-20004, 'Cannot delete cars with a selling price above 100,000.');
    END IF;
END;
/

--After Trigger--
CREATE OR REPLACE TRIGGER after_delete_car
AFTER DELETE ON CarData
FOR EACH ROW
BEGIN
    DBMS_OUTPUT.PUT_LINE('Car record deleted: ' || :OLD.Car_Name);
END;
/

CREATE OR REPLACE PROCEDURE delete_car(p_id IN NUMBER) AS
BEGIN
    SAVEPOINT before_delete;

    BEGIN
        DELETE FROM CarData
        WHERE ID = p_id;

        IF SQL%ROWCOUNT = 0 THEN
            ROLLBACK TO before_delete;
            DBMS_OUTPUT.PUT_LINE('No record found for ID: ' || p_id);
        ELSE
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Record deleted successfully.');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK TO before_delete;
            DBMS_OUTPUT.PUT_LINE('Error deleting record: ' || SQLERRM);
    END;
END;
/
SET SERVEROUTPUT ON

-- Prompt user for input
ACCEPT id PROMPT 'Enter ID of the record to delete: '

-- Execute the procedure with the provided value
BEGIN
    delete_car(
        &id
    );
END;
/

------------------------------------------------------------------------------------------------------------------------------------------

--WHY USING INDEX?
    --Faster Data Retrieval
        --Search Optimization: Indexes allow the database to locate data without scanning every row in a table
        --Efficient Sorting: Indexes help quickly sort the data on the indexed column(s), making operations like ORDER BY or range queries faster
    --Reduced I/O Operations
        --Less Disk Access: Instead of scanning the entire table, the database engine can use the index to quickly jump to the desired data
    --Enhanced Performance for Joins
        --Join Optimization: Indexes on the columns used in JOIN conditions can significantly improve the performance of join operations by quickly locating matching rows
    --Improved Query Efficiency
        --Selective Queries: Queries that filter based on specific conditions
        --Aggregation and Grouping: Speed up aggregation operations like COUNT, SUM, AVG, and GROUP BY by reducing the number of rows the database needs to scan

-- Index on Year --
--e.g. SELECT * FROM CarData WHERE Year = 2011;
CREATE INDEX idx_car_year ON CarData (Year);

-- Index on Selling_Price --
CREATE INDEX idx_car_selling_price ON CarData (Selling_Price);

-- Index on Fuel_Type --
CREATE INDEX idx_car_fuel_type ON CarData (Fuel_Type);

--Index on Seller_Type --
CREATE INDEX idx_car_seller_type ON CarData (Seller_Type);

-- Composite Index on Fuel_Type and Transmission (for queries involving both columns) --
--e.g. SELECT * FROM CarData WHERE Fuel_Type = 'Petrol' AND Transmission = 'Manual';
CREATE INDEX idx_car_fuel_transmission ON CarData (Fuel_Type, Transmission);

------------------------------------------------------------------------------------------------------------------------------
--Data Analysis--
--Extracting Useful Information--
--- Procedure to get all cars from a specific year---
CREATE OR REPLACE PROCEDURE get_cars_by_year(p_year IN NUMBER) AS
    CURSOR car_cursor IS
        SELECT Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner
        FROM CarData
        WHERE Year = p_year;
        
    v_car_name CarData.Car_Name%TYPE;
    v_year CarData.Year%TYPE;
    v_selling_price CarData.Selling_Price%TYPE;
    v_present_price CarData.Present_Price%TYPE;
    v_kms_driven CarData.Kms_Driven%TYPE;
    v_fuel_type CarData.Fuel_Type%TYPE;
    v_seller_type CarData.Seller_Type%TYPE;
    v_transmission CarData.Transmission%TYPE;
    v_owner CarData.Owner%TYPE;
BEGIN
    OPEN car_cursor;
    
    LOOP
        FETCH car_cursor INTO v_car_name, v_year, v_selling_price, v_present_price, v_kms_driven, v_fuel_type, v_seller_type, v_transmission, v_owner;
        
        EXIT WHEN car_cursor%NOTFOUND;
        
        DBMS_OUTPUT.PUT_LINE('Car Name: ' || v_car_name);
        DBMS_OUTPUT.PUT_LINE('Year: ' || v_year);
        DBMS_OUTPUT.PUT_LINE('Selling Price: ' || v_selling_price);
        DBMS_OUTPUT.PUT_LINE('Present Price: ' || v_present_price);
        DBMS_OUTPUT.PUT_LINE('Kms Driven: ' || v_kms_driven);
        DBMS_OUTPUT.PUT_LINE('Fuel Type: ' || v_fuel_type);
        DBMS_OUTPUT.PUT_LINE('Seller Type: ' || v_seller_type);
        DBMS_OUTPUT.PUT_LINE('Transmission: ' || v_transmission);
        DBMS_OUTPUT.PUT_LINE('Owner: ' || v_owner);
        DBMS_OUTPUT.PUT_LINE('-------------------------------');
    END LOOP;
    
    CLOSE car_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error retrieving data: ' || SQLERRM);
END;
/

--- Execute the procedure to get cars from a specific year ---
BEGIN
    get_cars_by_year(2010);
END;
/
    
-- Performing filtering table task --
--- Procedure to get cars with selling price above a certain amount, sorted by price ---
CREATE OR REPLACE PROCEDURE get_cars_by_price(p_min_price IN NUMBER) AS
    CURSOR car_cursor IS
        SELECT Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner
        FROM CarData
        WHERE Selling_Price > p_min_price
        ORDER BY Selling_Price DESC;
        
    v_car_name CarData.Car_Name%TYPE;
    v_year CarData.Year%TYPE;
    v_selling_price CarData.Selling_Price%TYPE;
    v_present_price CarData.Present_Price%TYPE;
    v_kms_driven CarData.Kms_Driven%TYPE;
    v_fuel_type CarData.Fuel_Type%TYPE;
    v_seller_type CarData.Seller_Type%TYPE;
    v_transmission CarData.Transmission%TYPE;
    v_owner CarData.Owner%TYPE;
BEGIN
    OPEN car_cursor;
    
    LOOP
        FETCH car_cursor INTO v_car_name, v_year, v_selling_price, v_present_price, v_kms_driven, v_fuel_type, v_seller_type, v_transmission, v_owner;
        
        EXIT WHEN car_cursor%NOTFOUND;
        
        DBMS_OUTPUT.PUT_LINE('Car Name: ' || v_car_name);
        DBMS_OUTPUT.PUT_LINE('Year: ' || v_year);
        DBMS_OUTPUT.PUT_LINE('Selling Price: ' || v_selling_price);
        DBMS_OUTPUT.PUT_LINE('Present Price: ' || v_present_price);
        DBMS_OUTPUT.PUT_LINE('Kms Driven: ' || v_kms_driven);
        DBMS_OUTPUT.PUT_LINE('Fuel Type: ' || v_fuel_type);
        DBMS_OUTPUT.PUT_LINE('Seller Type: ' || v_seller_type);
        DBMS_OUTPUT.PUT_LINE('Transmission: ' || v_transmission);
        DBMS_OUTPUT.PUT_LINE('Owner: ' || v_owner);
        DBMS_OUTPUT.PUT_LINE('-------------------------------');
    END LOOP;
    
    CLOSE car_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error retrieving data: ' || SQLERRM);
END;
/

--- Execute the procedure to get cars with selling price above 20 ---
BEGIN
    get_cars_by_price(20);
END;
/

-- Create reusable PL/SQL functions and procedures to  perform calculations on the data --
--- Calculate Average Selling Price by Year ---
CREATE OR REPLACE FUNCTION get_avg_selling_price_by_year(p_year IN NUMBER) RETURN NUMBER IS
    v_avg_price NUMBER;
BEGIN
    SELECT AVG(Selling_Price) INTO v_avg_price
    FROM CarData
    WHERE Year = p_year;

    RETURN v_avg_price;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No records found for the year: ' || p_year);
        RETURN NULL;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error calculating average selling price: ' || SQLERRM);
        RETURN NULL;
END;
/

--- Execute the procedure for get_avg_selling_price_by_year by year 2017 ---
DECLARE
    v_avg_price NUMBER;
BEGIN
    v_avg_price := get_avg_selling_price_by_year(2017);
    IF v_avg_price IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('Average Selling Price for 2017: ' || v_avg_price);
    ELSE
        DBMS_OUTPUT.PUT_LINE('No data found for the year 2017.');
    END IF;
END;
/

-- Generating meaningful report --

CREATE OR REPLACE PROCEDURE generate_comprehensive_report AS
    v_avg_price NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('---- Car Data Report ----');
    DBMS_OUTPUT.PUT_LINE('');

    -- Part 1: Cars from a Specific Year (e.g., 2010)
    DBMS_OUTPUT.PUT_LINE('--- Cars from the Year 2010 ---');
    BEGIN
        get_cars_by_year(2010);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error retrieving cars for the year 2015: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');

    -- Part 2: Cars with Selling Price Above a Certain Amount (e.g., 20)
    DBMS_OUTPUT.PUT_LINE('--- Cars with Selling Price Above 20 ---');
    BEGIN
        get_cars_by_price(20);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error retrieving cars with selling price above 20: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');

    -- Part 3: Average Selling Price by Year
    DBMS_OUTPUT.PUT_LINE('--- Average Selling Price by Year ---');
    FOR year IN 2012..2020 LOOP
        BEGIN
            v_avg_price := get_avg_selling_price_by_year(year);
            IF v_avg_price IS NOT NULL THEN
                DBMS_OUTPUT.PUT_LINE('Year ' || year || ': ' || v_avg_price);
            ELSE
                DBMS_OUTPUT.PUT_LINE('Year ' || year || ': No data found.');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error calculating average selling price for the year ' || year || ': ' || SQLERRM);
        END;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    DBMS_OUTPUT.PUT_LINE('---- End of Report ----');
END;
/

BEGIN
    generate_comprehensive_report;
END;
/

--- Generated report content ---

---- Car Data Report ----

--- Cars from the Year 2010 ---
Car Name: sx4
Year: 2010
Selling Price: 2.65
Present Price: 7.98
Kms Driven: 41442
Fuel Type: Petrol
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: alto k10
Year: 2010
Selling Price: 1.95
Present Price: 3.95
Kms Driven: 44542
Fuel Type: Petrol
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: corolla altis
Year: 2010
Selling Price: 4.75
Present Price: 18.54
Kms Driven: 50000
Fuel Type: Petrol
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: fortuner
Year: 2010
Selling Price: 9.25
Present Price: 20.45
Kms Driven: 59000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: corolla altis
Year: 2010
Selling Price: 5.25
Present Price: 22.83
Kms Driven: 80000
Fuel Type: Petrol
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: land cruiser
Year: 2010
Selling Price: 35
Present Price: 92.6
Kms Driven: 78000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: fortuner
Year: 2010
Selling Price: 9.65
Present Price: 20.45
Kms Driven: 50024
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: Bajaj Pulsar 220 F
Year: 2010
Selling Price: .52
Present Price: .94
Kms Driven: 45000
Fuel Type: Petrol
Seller Type: Individual
Transmission: Manual
Owner: 0
-------------------------------
Car Name: Bajaj Avenger 220 dtsi
Year: 2010
Selling Price: .45
Present Price: .95
Kms Driven: 27000
Fuel Type: Petrol
Seller Type: Individual
Transmission: Manual
Owner: 0
-------------------------------
Car Name: Honda Karizma
Year: 2010
Selling Price: .31
Present Price: 1.05
Kms Driven: 213000
Fuel Type: Petrol
Seller Type: Individual
Transmission: Manual
Owner: 0
-------------------------------
Car Name: TVS Wego
Year: 2010
Selling Price: .25
Present Price: .52
Kms Driven: 22000
Fuel Type: Petrol
Seller Type: Individual
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: Honda CB twister
Year: 2010
Selling Price: .16
Present Price: .51
Kms Driven: 33000
Fuel Type: Petrol
Seller Type: Individual
Transmission: Manual
Owner: 0
-------------------------------
Car Name: i20
Year: 2010
Selling Price: 3.25
Present Price: 6.79
Kms Driven: 58000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Manual
Owner: 1
-------------------------------
Car Name: jazz
Year: 2010
Selling Price: 2.25
Present Price: 7.5
Kms Driven: 61203
Fuel Type: Petrol
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: city
Year: 2010
Selling Price: 3.25
Present Price: 9.9
Kms Driven: 38000
Fuel Type: Petrol
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------

--- Cars with Selling Price Above 20 ---
Car Name: benza
Year: 2008
Selling Price: 89
Present Price: 28
Kms Driven: 20000
Fuel Type: Petrol
Seller Type: Dealer
Transmission: 0
Owner: 0
-------------------------------
Car Name: land cruiser
Year: 2010
Selling Price: 35
Present Price: 92.6
Kms Driven: 78000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Manual
Owner: 0
-------------------------------
Car Name: fortuner
Year: 2017
Selling Price: 33
Present Price: 36.23
Kms Driven: 6000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: fortuner
Year: 2015
Selling Price: 23.5
Present Price: 35.96
Kms Driven: 47000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: fortuner
Year: 2015
Selling Price: 23
Present Price: 30.61
Kms Driven: 40000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: fortuner
Year: 2015
Selling Price: 23
Present Price: 30.61
Kms Driven: 40000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: innova
Year: 2017
Selling Price: 23
Present Price: 25.39
Kms Driven: 15000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------
Car Name: innova
Year: 2016
Selling Price: 20.75
Present Price: 25.39
Kms Driven: 29000
Fuel Type: Diesel
Seller Type: Dealer
Transmission: Automatic
Owner: 0
-------------------------------

--- Average Selling Price by Year ---
Year 2012: 3.8413043478260869565217391304347826087
Year 2013: 3.54090909090909090909090909090909090909
Year 2014: 4.76210526315789473684210526315789473684
Year 2015: 5.92704918032786885245901639344262295082
Year 2016: 5.2132
Year 2017: 6.17852941176470588235294117647058823529
Year 2018: 9.25
Year 2019: No data found.
Year 2020: No data found.

---- End of Report ---
------------------------------------------------------------------------------------------------------------------------------
-------Aggregations:Calculate the Percentage Change in Average Selling Price Year-over-Year--------
DECLARE
    v_avg_price_current   NUMBER;
    v_avg_price_previous  NUMBER;
    v_percentage_change   NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('--- Percentage Change in Average Selling Price Year-over-Year ---');

    FOR year IN 2013..2020 LOOP
        BEGIN
            -- Get average price for the current year
            v_avg_price_current := get_avg_selling_price_by_year(year);

            -- Get average price for the previous year
            v_avg_price_previous := get_avg_selling_price_by_year(year - 1);

            IF v_avg_price_current IS NOT NULL AND v_avg_price_previous IS NOT NULL THEN
                -- Calculate percentage change
                v_percentage_change := ((v_avg_price_current - v_avg_price_previous) / v_avg_price_previous) * 100;

                DBMS_OUTPUT.PUT_LINE('Year ' || year || ' vs ' || (year - 1) || ': ' || ROUND(v_percentage_change, 2) || '%');
            ELSE
                IF v_avg_price_current IS NULL THEN
                    DBMS_OUTPUT.PUT_LINE('Year ' || year || ': No data found.');
                END IF;
                IF v_avg_price_previous IS NULL THEN
                    DBMS_OUTPUT.PUT_LINE('Year ' || (year - 1) || ': No data found.');
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error calculating percentage change for the year ' || year || ': ' || SQLERRM);
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('---- End of Report ----');
END;
/

--report generated--
--- Percentage Change in Average Selling Price Year-over-Year ---
Year 2013 vs 2012: -7.96%
Year 2014 vs 2013: 34.69%
Year 2015 vs 2014: 24.46%
Year 2016 vs 2015: -12.04%
Year 2017 vs 2016: 19.1%
Year 2018 vs 2017: 48.97%
Year 2019: No data found.
Year 2019: No data found.
---- End of Report ----

----sorting : Sorting by Average Selling Price-------
DECLARE
    TYPE avg_price_record IS RECORD (
        year        NUMBER,
        avg_price   NUMBER
    );
    
    TYPE avg_price_table IS TABLE OF avg_price_record INDEX BY PLS_INTEGER;
    
    v_avg_prices    avg_price_table;
    v_temp          avg_price_record;
    v_count         INTEGER := 1;
    
BEGIN
    -- Collect the average selling prices by year
    FOR year IN 2012..2020 LOOP
        BEGIN
            v_avg_prices(v_count).year := year;
            v_avg_prices(v_count).avg_price := get_avg_selling_price_by_year(year);
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error retrieving data for year ' || year || ': ' || SQLERRM);
        END;
    END LOOP;

    -- Sorting the collection by average price (simple bubble sort)
    FOR i IN 1..v_count-2 LOOP
        FOR j IN 1..v_count-2 LOOP
            IF v_avg_prices(j).avg_price > v_avg_prices(j+1).avg_price THEN
                v_temp := v_avg_prices(j);
                v_avg_prices(j) := v_avg_prices(j+1);
                v_avg_prices(j+1) := v_temp;
            END IF;
        END LOOP;
    END LOOP;

    -- Output the sorted list
    DBMS_OUTPUT.PUT_LINE('--- Sorted List of Years by Average Selling Price ---');
    FOR i IN 1..v_count-1 LOOP
        IF v_avg_prices(i).avg_price IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('Year ' || v_avg_prices(i).year || ': ' || v_avg_prices(i).avg_price);
        ELSE
            DBMS_OUTPUT.PUT_LINE('Year ' || v_avg_prices(i).year || ': No data found.');
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('---- End of Sorted List ----');
END;
/

--- Sorted List of Years by Average Selling Price ---
Year 2013: 3.53565217391304347826086956521739130435
Year 2012: 3.8413043478260869565217391304347826087
Year 2014: 4.76210526315789473684210526315789473684
Year 2016: 5.2132
Year 2015: 5.92704918032786885245901639344262295082
Year 2017: 6.20914285714285714285714285714285714286
Year 2018: 9.25
Year 2019: No data found.
Year 2020: 2.2
---- End of Sorted List ---

---Additional Requirements---
----Explore advanced PL/SQL features : cursors + filtering :filter car data by year
SELECT * FROM all_tables WHERE table_name = 'CarData';

CREATE OR REPLACE PROCEDURE process_cars_by_year(p_year IN NUMBER) AS
    CURSOR car_cursor IS
        SELECT Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner
        FROM CarData
        WHERE Year = p_year;

    -- Variables to hold the data fetched by the cursor
    v_car_name CarData.Car_Name%TYPE;
    v_year CarData.Year%TYPE;
    v_selling_price CarData.Selling_Price%TYPE;
    v_present_price CarData.Present_Price%TYPE;
    v_kms_driven CarData.Kms_Driven%TYPE;
    v_fuel_type CarData.Fuel_Type%TYPE;
    v_seller_type CarData.Seller_Type%TYPE;
    v_transmission CarData.Transmission%TYPE;
    v_owner CarData.Owner%TYPE;
BEGIN
    OPEN car_cursor;
    
    LOOP
        FETCH car_cursor INTO v_car_name, v_year, v_selling_price, v_present_price, v_kms_driven, v_fuel_type, v_seller_type, v_transmission, v_owner;
        EXIT WHEN car_cursor%NOTFOUND;
        
        -- Process each row (for demonstration purposes, we'll just print the data)
        DBMS_OUTPUT.PUT_LINE('Car Name: ' || v_car_name);
        DBMS_OUTPUT.PUT_LINE('Year: ' || v_year);
        DBMS_OUTPUT.PUT_LINE('Selling Price: ' || v_selling_price);
        DBMS_OUTPUT.PUT_LINE('Present Price: ' || v_present_price);
        DBMS_OUTPUT.PUT_LINE('Kms Driven: ' || v_kms_driven);
        DBMS_OUTPUT.PUT_LINE('Fuel Type: ' || v_fuel_type);
        DBMS_OUTPUT.PUT_LINE('Seller Type: ' || v_seller_type);
        DBMS_OUTPUT.PUT_LINE('Transmission: ' || v_transmission);
        DBMS_OUTPUT.PUT_LINE('Owner: ' || v_owner);
        DBMS_OUTPUT.PUT_LINE('-------------------------------');
    END LOOP;
    
    CLOSE car_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing cars: ' || SQLERRM);
END;
/

SET SERVEROUTPUT ON;
EXECUTE process_cars_by_year(2014);  


