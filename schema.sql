-- In this SQL file, write (and comment!) the schema of 
-- your database, including the CREATE TABLE, CREATE INDEX, 
-- CREATE VIEW, etc. statements that compose it

-- TYPES SECTION --

-- Create ENUM type for pl dow names.
CREATE TYPE "pl_dow" AS ENUM('Poniedziałek', 'Wtorek', 'Środa',
    'Czwartek', 'Piątek', 'Sobota', 'Niedziela'
);

-- Create Enum type for pl month names.
CREATE TYPE "pl_month" AS ENUM('Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj', 
    'Czerwiec', 'Lipiec', 'Sierpień', 'Wrzesień', 'Październik', 
    'Listopad', 'Grudzień'
);

-- Create ad_brands ENUM type for brands table.
CREATE TYPE "ad_brand" AS ENUM('EURO APPLIANCES', 'MEDIA MASTER', 
'MEDIA SHOP', 'NEWNET');

-- Creates ENUM type for reach types table.
CREATE TYPE "reach_type" AS ENUM('krajowe', 'miejskie', 
'ponadregionalne', 'regionalne');

-- Creates ENUM type for dayparts table
CREATE TYPE "daypart_type" AS ENUM('do 9', 'od 9 do 16', 'po 16');

-- Creates ENUM type for unified lengths table.
CREATE TYPE "length_type" AS ENUM('15', '20', '30');

-- Creates ENUM type for product types table.
CREATE TYPE "products" AS ENUM('AGD, RTV, ELEKTRONIKA, FOTOGRAFIA, KOMPUTERY',
'GRUPA', 'OGŁOSZENIA O PRACY');


-- TABLES SECTION --

-- Table injecting Polish day of week names into the date_time table.
CREATE TABLE IF NOT EXISTS "pl_dow_names" (
    "id" SERIAL,
    "dow_name" "pl_dow" NOT NULL UNIQUE,
    PRIMARY KEY("id")
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
    "date" DATE NOT NULL UNIQUE,
    "day" SMALLINT CHECK("day" BETWEEN 1 AND 31),
    "day_of_week" SMALLINT CHECK("day_of_week" BETWEEN 1 AND 7),
    "week" SMALLINT CHECK("week" BETWEEN 1 AND 53),
    "year" SMALLINT CHECK("year" BETWEEN 1900 AND 9999),
    "month" SMALLINT CHECK("month" BETWEEN 1 AND 12),
    PRIMARY KEY("id"),
    FOREIGN KEY("day_of_week") REFERENCES "pl_dow_names"("id"),
    FOREIGN KEY("month") REFERENCES "pl_month_names"("id")
);

-- Create brands table
CREATE TABLE IF NOT EXISTS "brands" (
    "id" SERIAL,
    "brand" "ad_brand" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- References brodcasters name for mediums table.
CREATE TABLE IF NOT EXISTS "broadcasters" (
    "id" SERIAL,
    "broadcaster" VARCHAR(50) NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- References reach of given radio station for mediums table.
CREATE TABLE IF NOT EXISTS "ad_reach" (
    "id" SERIAL,
    "reach" "reach_type" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Crteates table containing radiostactions, their parent entity (broadcaster),
-- and the reach of each medium.
CREATE TABLE IF NOT EXISTS "mediums" (
    "id" SERIAL,
    "submedium" VARCHAR(50) NOT NULL UNIQUE,
    "broadcaster_id" SMALLINT NOT NULL,
    "ad_reach_id" SMALLINT NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("broadcaster_id") REFERENCES "broadcasters"("id"),
    FOREIGN KEY("ad_reach_id") REFERENCES "ad_reach"("id")
);

-- Creates table for dayparts references.
CREATE TABLE IF NOT EXISTS "dayparts" (
    "id" SERIAL,
    "daypart" "daypart_type" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates table for unified advertisemens lenght references.
CREATE TABLE IF NOT EXISTS "unified_lenghts" (
    "id" SERIAL,
    "length" "length_type" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "ad_time_details" (
    "id" SERIAL,
    "date" DATE NOT NULL,
    "ad_slot_hour" VARCHAR(11) NOT NULL,
    "gg" SMALLINT NOT NULL,
    "mm" SMALLINT NOT NULL,
    "length_mod" SMALLINT NOT NULL,
    "daypart_id" SMALLINT NOT NULL,
    "unified_length_id" SMALLINT NOT NULL,
    "ad_code" VARCHAR(80) NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("daypart_id") REFERENCES "dayparts"("id"),
    FOREIGN KEY("unified_length_id") REFERENCES "unified_lenghts"("id")
);

-- Creates pprodyct type references for ads_desc table.
CREATE TABLE IF NOT EXISTS "product_types" (
    "id" SERIAL,
    "product_type" "products" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Create main table with ads emitted through radio estations across country. 
-- This is the table which holds all the data, and to which other tables point.
CREATE TABLE IF NOT EXISTS "ads_desc" (
    "id" SERIAL,
    "date" DATE NOT NULL,
    "ad_description" VARCHAR(200) NOT NULL,
    "ad_code" INTEGER NOT NULL,
    "brand_id" SMALLINT NOT NULL,
    "medium_id" SMALLINT NOT NULL,
    "ad_time_details_id" INTEGER NOT NULL UNIQUE,
    "product_type_id" SMALLINT NOT NULL,
    "cost" INTEGER,
    "num_of_emissions" SMALLINT NOT NULL CHECK("num_of_emissions" > 0),
    "type" VARCHAR(50) NOT NULL DEFAULT 'advertisement',
    PRIMARY KEY("id"),
    FOREIGN KEY("brand_id") REFERENCES "brands"("id"),
    FOREIGN KEY("medium_id") REFERENCES "mediums"("id"),
    FOREIGN KEY("ad_time_details_id") REFERENCES "ad_time_details"("id"),
    FOREIGN KEY("product_type_id") REFERENCES "product_types"("id"),
    FOREIGN KEY("date") REFERENCES "date_time"("date")
);


-- PROCEDURES, FUNCTIONS, TRIGGER FUNCTIONS SECTION --

-- Creates function for extracting day from date column. Used by the populate_day trigger.
CREATE OR REPLACE FUNCTION extract_day()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW."day" IS NULL THEN
        UPDATE "date_time" SET "day" = EXTRACT(DAY FROM NEW."date")
        WHERE "id" = NEW."id";
    END IF;
    RETURN NULL;
END;
$$;

-- Creates function for extracting day from date column. Used by the populate_dow trigger.
CREATE OR REPLACE FUNCTION extract_dow()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW."day_of_week" IS NULL THEN
        UPDATE "date_time" SET "day_of_week" = EXTRACT(ISODOW FROM NEW."date")
        WHERE "id" = NEW."id";
    END IF;
    RETURN NULL;
END;
$$;

-- Creates function for extracting day from date column. Used by the populate_week trigger.
CREATE OR REPLACE FUNCTION extract_week()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW."week" IS NULL THEN
        UPDATE "date_time" SET "week" = EXTRACT(WEEK FROM NEW."date")
        WHERE "id" = NEW."id";
    END IF;
    RETURN NULL;
END;
$$;

-- Creates function for extracting day from date column. Used by the populate_year trigger.
CREATE OR REPLACE FUNCTION extract_year()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW."year" IS NULL THEN
        UPDATE "date_time" SET "year" = EXTRACT(YEAR FROM NEW."date")
        WHERE "id" = NEW."id";
    END IF;
    RETURN NULL;
END;
$$;

-- Creates function for extracting day from date column. Used by the populate_month trigger.
CREATE OR REPLACE FUNCTION extract_month()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS
$$
BEGIN
    IF NEW."month" IS NULL THEN
        UPDATE "date_time" SET "month" = EXTRACT(MONTH FROM NEW."date")
        WHERE "id" = NEW."id";
    END IF;
    RETURN NULL;
END;
$$;


-- TRIGGERS SECTION --

-- Populates day column after date insertion.
CREATE TRIGGER "populate_day"
    AFTER INSERT 
    ON "date_time"
    FOR EACH ROW
    EXECUTE FUNCTION extract_day();

-- Populates dow column out after date insertion.
CREATE TRIGGER "populate_dow"
    AFTER INSERT 
    ON "date_time"
    FOR EACH ROW
    EXECUTE FUNCTION extract_dow();

-- Populates week column out after date insertion.
CREATE TRIGGER "populate_week"
    AFTER INSERT 
    ON "date_time"
    FOR EACH ROW
    EXECUTE FUNCTION extract_week();

-- Populates year column out after date insertion.
CREATE TRIGGER "populate_year"
    AFTER INSERT 
    ON "date_time"
    FOR EACH ROW
    EXECUTE FUNCTION extract_year();

-- Populates month column out after date insertion.
CREATE TRIGGER "populate_month"
    AFTER INSERT 
    ON "date_time"
    FOR EACH ROW
    EXECUTE FUNCTION extract_month();


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