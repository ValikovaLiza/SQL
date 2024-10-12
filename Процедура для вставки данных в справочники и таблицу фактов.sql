DROP PROCEDURE IF EXISTS InsertDataToFacts(text, integer, integer, integer, time, date);

CREATE OR REPLACE PROCEDURE InsertDataToFacts(
    IN p_userID TEXT,  
    IN p_artistID INT, 
    IN p_trackID INT, 
    IN p_dayID INT, 
    IN p_timme TIME, 
    IN p_report_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Вставляем данные в таблицу фактов с текущей датой
    INSERT INTO Listening (userID, artistID, trackID, dayID, timme, report_date, dtstart, dtend)
    SELECT 
        u.ID,  
        a.ID, 
        t.ID, 
        d.ID, 
        p_timme, 
        p_report_date,
        '1900-01-01',  -- Начало исторического периода
        p_report_date  -- Конец исторического периода
    FROM Users u 
    JOIN Artists a ON a.ID = p_artistID AND CURRENT_DATE BETWEEN a.dtstart AND a.dtend
    JOIN Tracks t ON t.ID = p_trackID AND CURRENT_DATE BETWEEN t.dtstart AND t.dtend
    JOIN Dayes d ON d.ID = p_dayID AND CURRENT_DATE BETWEEN d.dtstart AND d.dtend
    WHERE u.userID = p_userID  
      AND CURRENT_DATE BETWEEN u.dtstart AND u.dtend;

    -- Обновляем временной интервал для существующих записей
    UPDATE Listening 
    SET dtstart = '1900-01-01', dtend = p_report_date
    WHERE report_date = p_report_date;

    UPDATE Listening 
    SET dtstart = p_report_date + 1, dtend = '9999-12-31'
    WHERE report_date = p_report_date;

END;
$$;


CALL InsertDataToFacts(
    'EA721BBE',          -- p_userID 
    12,                  -- p_artistID 
    2,                  -- p_trackID 
    3,                  -- p_dayID 
    '12:30:00',        -- p_timme 
    '2024-10-11'       -- p_report_date 
);

SELECT * FROM Listening WHERE report_date = '2024-10-11';
SELECT * FROM tracks



