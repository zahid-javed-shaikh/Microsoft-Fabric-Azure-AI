-- 2. Update watermark
CREATE   PROCEDURE sp_update_watermark
    @CityId INT,
    @NewWatermark DATETIME2
AS
BEGIN
    UPDATE config_cities
    SET last_watermark = @NewWatermark
    WHERE city_id = @CityId;
END;