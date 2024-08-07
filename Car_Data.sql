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
CREATE OR REPLACE DIRECTORY my_dir AS 'M:\Y3S2\AdvanceDB\Assgm_Max\cardatabase';

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
    DBMS_OUTPUT.PUT_LINE('---- Comprehensive Car Data Report ----');
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



