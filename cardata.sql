
-- 2
CREATE TABLE Cars (
    CarID NUMBER PRIMARY KEY,
    Make VARCHAR2(50),
    Model VARCHAR2(50),
    Year NUMBER,
    Price NUMBER
);


CREATE OR REPLACE DIRECTORY my_dir AS 'C:\Users\maxvu\OneDrive\Desktop\dbassignment';


CREATE TABLE Cars_External (
    CarID NUMBER,
    Make VARCHAR2(50),
    Model VARCHAR2(50),
    Year NUMBER,
    Price NUMBER
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY my_dir
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
        (CarID, Make, Model, Year, Price)
    )
    LOCATION ('cardata.csv')
)
PARALLEL
REJECT LIMIT UNLIMITED;

DECLARE
    v_count NUMBER := 0;
BEGIN
    -- Insert data from external table to actual table
    BEGIN
        INSERT INTO Cars (CarID, Make, Model, Year, Price)
        SELECT CarID, Make, Model, Year, Price FROM Cars_External;

        v_count := SQL%ROWCOUNT;
        DBMS_OUTPUT.put_line(v_count || ' rows inserted.');
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            DBMS_OUTPUT.put_line('Duplicate value found. Insertion aborted.');
        WHEN OTHERS THEN
            DBMS_OUTPUT.put_line('Other error: ' || SQLERRM);
    END;
END;
/

-- 3
-- Index on Make
CREATE INDEX idx_cars_make ON Cars (Make);

-- Composite Index on Model and Year
CREATE INDEX idx_cars_model_year ON Cars (Model, Year);

--4
--a create with tm
CREATE OR REPLACE PROCEDURE create_car(
    p_CarID IN NUMBER,
    p_Make IN VARCHAR2,
    p_Model IN VARCHAR2,
    p_Year IN NUMBER,
    p_Price IN NUMBER
) IS
    v_savepoint VARCHAR2(30);
BEGIN
    v_savepoint := 'before_insert';
    SAVEPOINT before_insert;
    
    INSERT INTO Cars (CarID, Make, Model, Year, Price)
    VALUES (p_CarID, p_Make, p_Model, p_Year, p_Price);
    
    COMMIT;
    DBMS_OUTPUT.put_line('Car record created successfully.');
EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        ROLLBACK TO before_insert;
        DBMS_OUTPUT.put_line('Error: Duplicate CarID.');
    WHEN OTHERS THEN
        ROLLBACK TO before_insert;
        DBMS_OUTPUT.put_line('Error: ' || SQLERRM);
END;
/


--b retrieve
CREATE OR REPLACE PROCEDURE retrieve_cars(
    p_Make IN VARCHAR2 DEFAULT NULL,
    p_Model IN VARCHAR2 DEFAULT NULL,
    p_Year IN NUMBER DEFAULT NULL
) IS
    CURSOR c_cars IS
        SELECT * FROM Cars
        WHERE (p_Make IS NULL OR Make = p_Make)
        AND (p_Model IS NULL OR Model = p_Model)
        AND (p_Year IS NULL OR Year = p_Year);
BEGIN
    FOR r_car IN c_cars LOOP
        DBMS_OUTPUT.put_line('CarID: ' || r_car.CarID || ', Make: ' || r_car.Make || 
                             ', Model: ' || r_car.Model || ', Year: ' || r_car.Year || 
                             ', Price: ' || r_car.Price);
    END LOOP;
END;
/


--c update
CREATE OR REPLACE PROCEDURE update_car(
    p_CarID IN NUMBER,
    p_Make IN VARCHAR2 DEFAULT NULL,
    p_Model IN VARCHAR2 DEFAULT NULL,
    p_Year IN NUMBER DEFAULT NULL,
    p_Price IN NUMBER DEFAULT NULL
) IS
    v_savepoint VARCHAR2(30);
BEGIN
    v_savepoint := 'before_update';
    SAVEPOINT before_update;
    
    UPDATE Cars
    SET Make = NVL(p_Make, Make),
        Model = NVL(p_Model, Model),
        Year = NVL(p_Year, Year),
        Price = NVL(p_Price, Price)
    WHERE CarID = p_CarID;
    
    IF SQL%ROWCOUNT = 0 THEN
        ROLLBACK TO before_update;
        DBMS_OUTPUT.put_line('Error: CarID not found.');
    ELSE
        COMMIT;
        DBMS_OUTPUT.put_line('Car record updated successfully.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK TO before_update;
        DBMS_OUTPUT.put_line('Error: ' || SQLERRM);
END;
/



--d delete
CREATE OR REPLACE PROCEDURE delete_car(
    p_CarID IN NUMBER
) IS
    v_savepoint VARCHAR2(30);
BEGIN
    v_savepoint := 'before_delete';
    SAVEPOINT before_delete;
    
    DELETE FROM Cars WHERE CarID = p_CarID;
    
    IF SQL%ROWCOUNT = 0 THEN
        ROLLBACK TO before_delete;
        DBMS_OUTPUT.put_line('Error: CarID not found.');
    ELSE
        COMMIT;
        DBMS_OUTPUT.put_line('Car record deleted successfully.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK TO before_delete;
        DBMS_OUTPUT.put_line('Error: ' || SQLERRM);
END;
/

--6a before insert trigger
CREATE OR REPLACE TRIGGER before_insert_cars
BEFORE INSERT ON Cars
FOR EACH ROW
BEGIN
    IF :NEW.Price <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Price must be a positive number.');
    END IF;

    IF :NEW.Year < 1886 OR :NEW.Year > EXTRACT(YEAR FROM SYSDATE) THEN
        RAISE_APPLICATION_ERROR(-20002, 'Year must be between 1886 and the current year.');
    END IF;
END;
/


--6b after insert trigger
CREATE OR REPLACE TRIGGER after_insert_cars
AFTER INSERT ON Cars
FOR EACH ROW
BEGIN
    DBMS_OUTPUT.put_line('New car record inserted: ' || :NEW.CarID || ', ' || :NEW.Make || ', ' || :NEW.Model);
    -- Additional actions can be added here, such as inserting into a log table
END;
/


--6c before update trigger
CREATE OR REPLACE TRIGGER before_update_cars
BEFORE UPDATE ON Cars
FOR EACH ROW
BEGIN
    IF :NEW.Price <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Price must be a positive number.');
    END IF;

    IF :NEW.Year < 1886 OR :NEW.Year > EXTRACT(YEAR FROM SYSDATE) THEN
        RAISE_APPLICATION_ERROR(-20002, 'Year must be between 1886 and the current year.');
    END IF;
END;
/


--6d after update trigger
CREATE OR REPLACE TRIGGER after_update_cars
AFTER UPDATE ON Cars
FOR EACH ROW
BEGIN
    DBMS_OUTPUT.put_line('Car record updated: ' || :NEW.CarID || ', ' || :NEW.Make || ', ' || :NEW.Model);
    -- Additional actions can be added here, such as inserting into a log table
END;
/

--6e before delete trigger
CREATE OR REPLACE TRIGGER before_delete_cars
BEFORE DELETE ON Cars
FOR EACH ROW
BEGIN
    -- Example: Prevent deletion if the car is from the current year
    IF :OLD.Year = EXTRACT(YEAR FROM SYSDATE) THEN
        RAISE_APPLICATION_ERROR(-20003, 'Cannot delete cars from the current year.');
    END IF;
END;
/

--6f after delete trigger
CREATE OR REPLACE TRIGGER after_delete_cars
AFTER DELETE ON Cars
FOR EACH ROW
BEGIN
    DBMS_OUTPUT.put_line('Car record deleted: ' || :OLD.CarID || ', ' || :OLD.Make || ', ' || :OLD.Model);
    -- Additional actions can be added here, such as inserting into a log table
END;
/


--8a enhancement
CREATE TABLE Error_Log (
    ErrorID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ErrorMessage VARCHAR2(4000),
    ErrorDate DATE DEFAULT SYSDATE
);

CREATE OR REPLACE PROCEDURE create_car(
    p_CarID IN NUMBER,
    p_Make IN VARCHAR2,
    p_Model IN VARCHAR2,
    p_Year IN NUMBER,
    p_Price IN NUMBER
) IS
    v_savepoint VARCHAR2(30);
BEGIN
    v_savepoint := 'before_insert';
    SAVEPOINT before_insert;
    
    INSERT INTO Cars (CarID, Make, Model, Year, Price)
    VALUES (p_CarID, p_Make, p_Model, p_Year, p_Price);
    
    COMMIT;
    DBMS_OUTPUT.put_line('Car record created successfully.');
EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        ROLLBACK TO before_insert;
        INSERT INTO Error_Log (ErrorMessage) VALUES ('Duplicate CarID: ' || p_CarID);
        DBMS_OUTPUT.put_line('Error: Duplicate CarID.');
    WHEN OTHERS THEN
        ROLLBACK TO before_insert;
        INSERT INTO Error_Log (ErrorMessage) VALUES (SQLERRM);
        DBMS_OUTPUT.put_line('Error: ' || SQLERRM);
END;
/

--8d cursor 
CREATE OR REPLACE PROCEDURE retrieve_cars_by_make(p_Make IN VARCHAR2) IS
    CURSOR c_cars IS
        SELECT CarID, Make, Model, Year, Price FROM Cars WHERE Make = p_Make;
    
    TYPE car_table IS TABLE OF c_cars%ROWTYPE;
    v_cars car_table;
BEGIN
    OPEN c_cars;
    FETCH c_cars BULK COLLECT INTO v_cars;
    CLOSE c_cars;
    
    FOR i IN 1..v_cars.COUNT LOOP
        DBMS_OUTPUT.put_line('CarID: ' || v_cars(i).CarID || ', Make: ' || v_cars(i).Make || 
                             ', Model: ' || v_cars(i).Model || ', Year: ' || v_cars(i).Year || 
                             ', Price: ' || v_cars(i).Price);
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO Error_Log (ErrorMessage) VALUES (SQLERRM);
        DBMS_OUTPUT.put_line('Error: ' || SQLERRM);
END;
/

--8d dynamic sql
CREATE OR REPLACE PROCEDURE retrieve_cars_dynamic(p_column IN VARCHAR2, p_value IN VARCHAR2) IS
    v_sql VARCHAR2(4000);
    v_carID Cars.CarID%TYPE;
    v_make Cars.Make%TYPE;
    v_model Cars.Model%TYPE;
    v_year Cars.Year%TYPE;
    v_price Cars.Price%TYPE;
BEGIN
    v_sql := 'SELECT CarID, Make, Model, Year, Price FROM Cars WHERE ' || p_column || ' = :1';
    
    EXECUTE IMMEDIATE v_sql INTO v_carID, v_make, v_model, v_year, v_price USING p_value;
    
    DBMS_OUTPUT.put_line('CarID: ' || v_carID || ', Make: ' || v_make || 
                         ', Model: ' || v_model || ', Year: ' || v_year || 
                         ', Price: ' || v_price);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.put_line('No car found with ' || p_column || ' = ' || p_value);
    WHEN OTHERS THEN
        INSERT INTO Error_Log (ErrorMessage) VALUES (SQLERRM);
        DBMS_OUTPUT.put_line('Error: ' || SQLERRM);
END;
/
