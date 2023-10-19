-- query 1
SELECT
    dac."Animal_Type",
    COUNT(DISTINCT ft."Animal_Key") AS NumAnimalsWithOutcomes
FROM
    fact_table ft
JOIN
    dim_animal_name dan ON ft."Animal_Key" = dan."Animal_Key"
JOIN
    dim_animal_char dac ON ft."Animal_Char_Key" = dac."Animal_Char_Key"
JOIN
    dim_outcome_table dot ON ft."Outcome_Id" = dot."Outcome_Id"
GROUP BY
    dac."Animal_Type";


-- query 2

SELECT
    COUNT(DISTINCT ft."Animal_Key") AS NumAnimalsWithMultipleOutcomes
FROM
    fact_table ft
GROUP BY
    ft."Animal_Key"
HAVING
    COUNT(ft."Outcome_Id") > 1;

-- query 3

SELECT
    TO_CHAR(dim_date."DateTime", 'Month') AS MonthName,
    COUNT(*) AS NumOutcomes
FROM
    fact_table
JOIN
    dim_date ON fact_table."DateTime_Key" = dim_date."DateTime_Key"
GROUP BY
    MonthName
ORDER BY
    NumOutcomes DESC
LIMIT 5;


-- query 4a

SELECT
    o."Outcome_Type",
    CASE
        WHEN o."Age_upon_Outcome" < 52 THEN 'Kitten'
        WHEN o."Age_upon_Outcome" >= 52 AND o."Age_upon_Outcome" <= 520 THEN 'Adult'
        WHEN o."Age_upon_Outcome" > 520 THEN 'Senior'
        ELSE 'Unknown'
    END AS "Age_Group",
    COUNT(*) AS Total_Count
FROM
    fact_table f
    JOIN dim_outcome_table o ON f."Outcome_Id" = o."Outcome_Id"
    JOIN dim_animal_name a ON f."Animal_Key" = a."Animal_Key"
WHERE
    o."Outcome_Type" = 'Adoption'
GROUP BY
    "Outcome_Type", "Age_Group";

-- query 4b

SELECT
    CASE
        WHEN o."Age_upon_Outcome" < 52 THEN 'Kitten'
        WHEN o."Age_upon_Outcome" >= 52 AND o."Age_upon_Outcome" <= 520 THEN 'Adult'
        WHEN o."Age_upon_Outcome" > 520 THEN 'Senior'
        ELSE 'Unknown'
    END AS "Age_Group",
    COUNT(*) AS Total_Count
FROM
    fact_table f
    JOIN dim_outcome_table o ON f."Outcome_Id" = o."Outcome_Id"
    JOIN dim_animal_name a ON f."Animal_Key" = a."Animal_Key"
WHERE
    o."Outcome_Type" = 'Adoption'
GROUP BY
    "Age_Group";

-- query 5
SELECT
    CAST(d."DateTime" AS DATE) AS Date,    
    SUM(COUNT(*)) OVER (ORDER BY CAST(d."DateTime" AS DATE)) AS Cumulative_Total
FROM
    fact_table f
    JOIN dim_outcome_table o ON f."Outcome_Id" = o."Outcome_Id"
    JOIN dim_date d ON f."DateTime_Key" = d."DateTime_Key"
GROUP BY
    Date
ORDER BY
    Date
