--Create Actual Table--
CREATE TABLE CarData (
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
CREATE OR REPLACE DIRECTORY my_dir AS 'M:\Y3S2\AdvanceDB\Assgm\cardatabase';

--Insert Data into Internal/Actual Table--
INSERT INTO CarData
SELECT * FROM CarData_ext;

--Create Car--
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
    INSERT INTO CarData (Car_Name, Year, Selling_Price, Present_Price, Kms_Driven, Fuel_Type, Seller_Type, Transmission, Owner)
    VALUES (p_car_name, p_year, p_selling_price, p_present_price, p_kms_driven, p_fuel_type, p_seller_type, p_transmission, p_owner);
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error creating record: ' || SQLERRM);
END;
/

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


--Update Car--
CREATE OR REPLACE PROCEDURE update_car(
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
    UPDATE CarData
    SET Year = p_year,
        Selling_Price = p_selling_price,
        Present_Price = p_present_price,
        Kms_Driven = p_kms_driven,
        Fuel_Type = p_fuel_type,
        Seller_Type = p_seller_type,
        Transmission = p_transmission,
        Owner = p_owner
    WHERE Car_Name = p_car_name;

    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No record found for car name: ' || p_car_name);
    ELSE
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Record updated successfully.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error updating record: ' || SQLERRM);
END;
/

--Delete Car--
CREATE OR REPLACE PROCEDURE delete_car(p_car_name IN VARCHAR2) AS
BEGIN
    DELETE FROM CarData
    WHERE Car_Name = p_car_name;

    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No record found for car name: ' || p_car_name);
    ELSE
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Record deleted successfully.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error deleting record: ' || SQLERRM);
END;
/
