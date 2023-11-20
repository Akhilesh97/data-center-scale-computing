-- Dimension Tables
CREATE TABLE dim_outcome_table (
    Outcome_Id INT PRIMARY KEY,
    Outcome_Type VARCHAR(255),
    Outcome_Subtype VARCHAR(255),
    Age_Upon_Outcome INT
);

CREATE TABLE dim_animal_name (
    Animal_Key INT PRIMARY KEY,
    Animal_ID VARCHAR(255),
    Name VARCHAR(255)
);

CREATE TABLE dim_animal_char (
    Animal_Char_Key INT PRIMARY KEY,
    Animal_Type VARCHAR(255),
    Breed VARCHAR(255),
    Color VARCHAR(255)
);

CREATE TABLE dim_repro_table (
    Repro_Key INT PRIMARY KEY,
    Reproductive_Status VARCHAR(255),
    Sex VARCHAR(10)
);

CREATE TABLE dim_date (
    DateTime_Key INT PRIMARY KEY,
    DateTime TIMESTAMP,
    Month INT,
    Year INT,
    Time VARCHAR(20)
);

-- Fact Table
CREATE TABLE fact_table (
    Fact_Id DOUBLE PRIMARY KEY,
    DateTime TIMESTAMP,
    DateTime_Key DOUBLE,
    Outcome_Id INT,
    Animal_Key INT,
    Animal_Char_Key INT,
    Repro_Key INT,
  
    FOREIGN KEY (DateTime_Key) REFERENCES dim_date(DateTime_Key),
    FOREIGN KEY (Outcome_Id) REFERENCES dim_outcome_table(Outcome_Id),
    FOREIGN KEY (Animal_Key) REFERENCES dim_animal_name(Animal_Key),
    FOREIGN KEY (Animal_Char_Key) REFERENCES dim_animal_char(Animal_Char_Key),
    FOREIGN KEY (Repro_Key) REFERENCES dim_repro_table(Repro_Key)   
);

