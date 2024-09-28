CREATE TABLE cleaned_data AS
SELECT *
FROM raw_data
WHERE NOT (COALESCE("userID", "Track", artist, genre, "City", CAST(time AS text), "Weekday") IS NULL
   OR "Report_date" IS NULL)
   AND "Report_date" BETWEEN '1/1/2023' AND '31/12/2023';


Select * from raw_data

Select * from cleaned_data