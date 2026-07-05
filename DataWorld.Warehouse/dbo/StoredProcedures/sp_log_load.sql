-- 3. Log the run
CREATE   PROCEDURE sp_log_load
    @RunId VARCHAR(50),
    @CityName VARCHAR(100),
    @RowsLoaded INT,
    @NewWatermark DATETIME2
AS
BEGIN
    INSERT INTO load_log VALUES (@RunId, @CityName, @RowsLoaded, @NewWatermark, GETDATE());
END;