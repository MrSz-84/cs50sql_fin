-- In this SQL file, write (and comment!) the schema of 
-- your database, including the CREATE TABLE, CREATE INDEX, 
-- CREATE VIEW, etc. statements that compose it

-- Create ENUM type for pl dow names.
CREATE TYPE "pl_dow" AS ENUM('Poniedziałek', 'Wtorek', 'Środa',
    'Czwartek', 'Piątek', 'Sobota', 'Niedziela'
);

-- Table injecting Polish day of week names into the date_time table.
CREATE TABLE IF NOT EXISTS "pl_dow_names" (
    "id" SERIAL,
    "dow_name" "pl_dow" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Create Enum type for pl month names.
CREATE TYPE "pl_month" AS ENUM('Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj', 
    'Czerwiec', 'Lipiec', 'Sierpień', 'Wrzesień', 'Październik', 
    'Listopad', 'Grudzień'
);

-- Table injecting Piolish month names into the date_time table.
CREATE TABLE IF NOT EXISTS "pl_month_names" (
    "id" SERIAL,
    "month_name" "pl_month" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Create date tables for further filtration and aggregation of data.
-- This table is going to be related to by few others to guarantee e.g. valid joining.
CREATE TABLE IF NOT EXISTS "date_time" (
    "id" SERIAL,
    "date" DATE NOT NULL UNIQUE CHECK(LENGTH("date") = 10),
    "day" SMALLINT CHECK("day" BETWEEN 1 AND 31),
    "day_of_week" SMALLINT CHECK("day_of_week" BETWEEN 1 AND 7),
    "week" SMALLINT CHECK("week" BETWEEN 1 AND 53),
    "year" SMALLINT CHECK("year" BETWEEN 1900 AND 9999),
    "month" SMALLINT CHECK("month" BETWEEN 1 AND 12),
    PRIMARY KEY("id"),
    FOREIGN KEY("day_of_week") REFERENCES "pl_dow_names"("id"),
    FOREIGN KEY("month") REFERENCES "pl_month_names"("id")
);

-- Create ad_brands ENUM type for brand table.
CREATE TYPE "ad_brand" AS ENUM('EURO APPLIANCES', 'MEDIA MASTER', 'MEDIA SHOP', 'NEWNET');

-- Create brands table
CREATE TABLE IF NOT EXISTS "brands" (
    "id" SERIAL,
    "brand" "ad_brand" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Create main table with ads emitted through radio estations across country. 
-- This is the table which holds all the data, and to which other tables point.
CREATE TABLE IF NOT EXISTS "ads_desc" (
    "id" SERIAL,
    "date" DATE NOT NULL CHECK(LENGTH("date") = 10),
    "ad_description" VARCHAR(200) NOT NULL,
    "ad_code" INTEGER NOT NULL,
    "brand_id" SMALLINT NOT NULL,
    "medium_id" SMALLINT NOT NULL,
    "ad_time_details_id" SMALLINT NOT NULL,
    "product_type_id" SMALLINT NOT NULL,
    "cost" INT,
    "num_of_emmisions" SMALLINT NOT NULL CHECK("num_ofemissions" > 0),
    "type" VARCHAR(50) NOT NULL DEFAULT 'advertisement',
    PRIMARY KEY("id"),
    FOREIGN KEY("brand_id") REFERENCES "brands"("id"),
    FOREIGN KEY("medium_id") REFERENCES "mediums"("id"),

);

-- Crteates table containing radiostactions, their parent entity (broadcaster),
-- and the reach of each medium.
CREATE TABLE IF NOT EXISTS "mediums" (
    "id" SERIAL,
    "submedium" VARCHAR(50) NOT NULL UNIQUE,
    "broadcaster_id" SMALLINT NOT NULL,
    "reach_id" SMALLINT NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("broadcaster_id") REFERENCES "broadcasters"("id"),
    FOREIGN KEY("reach_id") REFERENCES "reach"("id"),
);

-- Refferences brodcasters name for medium table.
CREATE TABLE IF NOT EXISTS "broadcasters" (
    "id" SERIAL,
    "broadcaster" VARCHAR(50) NOT NULL UNIQUE,
    PRIMARY KEY("id")
);


CREATE TYPE AS "reach_type" ENUM('krajowe', 'miejskie', 'ponadregionalne', 'regionalne')

CREATE TABLE IF NOT EXISTS "reach" (
    "id" SERIAL,
    "reach" "reach_type" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- CREATE TRIGGER "populate_date_time"
-- AFTER INSERT ON "date_time"
-- FOR EACH ROW
-- BEGIN
--     IF NEW."day" IS NULL THEN
--         UPDATE "date_time" SET "day" = EXTRACT(DAY FROM NEW."data")
--         WHERE "id" = NEW."id"
--     END IF
--     IF NEW."day_of_week" IS NULL THEN
--         UPDATE "date_time" SET "day_of_week" = EXTRACT(ISODOW FROM NEW."date")
--         WHERE "id" = NEW."id"
--     END IF
--     IF NEW."week" IS NULL THEN
--         UPDATE "date_time" SET "week" = EXTRACT(WEEK FROM NEW."date")
--         WHERE "id" = NEW."id"
--     END IF
--     IF NEW."year" IS NULL THEN
--         UPDATE "date_time" SET "year" = EXTRACT(YEAR FROM NEW."date")
--         WHERE "id" = NEW."id"
--     END IF
--     IF NEW."month" IS NULL THEN
--         UPDATE "date_time" SET "month" = EXTRACT(MONTH FROM NEW."date")
--         WHERE "id" = NEW."id"
--     END IF
-- END;