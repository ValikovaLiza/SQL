--hnc
INSERT INTO Artists (artist)
SELECT DISTINCT artist
FROM cleaned_data;

--lflf
INSERT INTO Cities (city)
SELECT DISTINCT cd."City"
FROM cleaned_data cd

--xn
INSERT INTO Genres (genre)
SELECT DISTINCT genre
FROM cleaned_data;

--x
INSERT INTO tracks (track, genreid)
SELECT DISTINCT "Track", g.id
FROM cleaned_data cd
JOIN Genres g ON cd.genre = g.genre;

--gb
INSERT INTO Users (CityID, userID)
SELECT DISTINCT c.ID, cd."userID"
FROM cleaned_data cd
JOIN Cities c ON cd."City" = c.City


--ujn
INSERT INTO Dayes (day)
SELECT DISTINCT TO_DATE("Report_date", 'MM/DD/YYYY') 
FROM cleaned_data


INSERT INTO Listening (userID, artistID, trackID, dayID, timme, report_date)
SELECT 
    u.ID, 
    a.ID AS artistID, 
    t.ID AS trackID, 
    d.ID AS dayID, 
    (FLOOR(cd.time) || ':' || ((cd.time - FLOOR(cd.time)) * 60)::integer || ':00')::time AS timme,
    TO_DATE(cd."Report_date", 'MM/DD/YYYY') AS report_date
FROM 
    cleaned_data cd
JOIN 
    Users u ON cd."userID" = u.userID  
JOIN 
    Artists a ON cd.artist = a.artist
JOIN 
    Tracks t ON cd."Track" = t.track
JOIN 
    Dayes d ON TO_DATE(cd."Report_date", 'MM/DD/YYYY') = d.day
WHERE 
    cd."userID" IS NOT NULL 
    AND cd.artist IS NOT NULL 
    AND cd."Track" IS NOT NULL 
    AND cd."Report_date" IS NOT NULL;



SELECT * FROM Listening LIMIT 10;
