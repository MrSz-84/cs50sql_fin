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
CREATE TABLE IF NOT EXISTS "unified_lengths" (
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
    FOREIGN KEY("unified_length_id") REFERENCES "unified_lengths"("id")
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

-- FUNCTIONS SECTION--

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

-- VIEWS SECTION --

-- View for all the relevant data for JOIN statements. 
-- User can go from there and select what they need.
-- Remember to use WHERE statements to filter out data.
-- It's also a good starting point to inporting the data to a data drame.
CREATE VIEW "all_ads_joined" AS
SELECT "date_time"."date" AS "date", "day", "day_of_week" AS "dow", "dow_name", 
"month", "month_name", "year", "ads_desc"."ad_code" AS "ad_code", "brand", 
"submedium", "broadcaster", "reach", "ad_slot_hour", "daypart", "length", 
"product_type", "cost", "type", "num_of_emissions" AS "quan"
FROM "ads_desc"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
JOIN "broadcasters" ON "broadcasters"."id" = "mediums"."broadcaster_id"
JOIN "ad_reach" ON "ad_reach"."id" = "mediums"."ad_reach_id"
JOIN "ad_time_details" ON "ad_time_details"."id" = "ads_desc"."ad_time_details_id"
JOIN "dayparts" ON "dayparts"."id" = "ad_time_details"."daypart_id"
JOIN "unified_lengths" ON "unified_lengths"."id" = "ad_time_details"."unified_length_id"
JOIN "product_types" ON "product_types"."id" = "ads_desc"."product_type_id"
JOIN "pl_dow_names" ON "pl_dow_names"."id" = "date_time"."day_of_week"
JOIN "pl_month_names" ON "pl_month_names"."id" = "date_time"."month";

-- View of number of spost per day per brand, per medium. For filtering use for instance:
-- SELECT * FROM spots_per_day_2023
-- WHERE "submedium" IN ('FROGGY WEATHER Wrocław', 'BET', 'SOME FM', 'OLD 1', 'TALK FM') AND "month" = 10
CREATE VIEW "spots_per_day_2023" AS
SELECT "submedium", "brand", "month",
    SUM(CASE WHEN "day" = 1 THEN "num_of_emissions" ELSE 0 END) AS "1",
    SUM(CASE WHEN "day" = 2 THEN "num_of_emissions" ELSE 0 END) AS "2",
    SUM(CASE WHEN "day" = 3 THEN "num_of_emissions" ELSE 0 END) AS "3",
    SUM(CASE WHEN "day" = 4 THEN "num_of_emissions" ELSE 0 END) AS "4",
    SUM(CASE WHEN "day" = 5 THEN "num_of_emissions" ELSE 0 END) AS "5",
    SUM(CASE WHEN "day" = 6 THEN "num_of_emissions" ELSE 0 END) AS "6",
    SUM(CASE WHEN "day" = 7 THEN "num_of_emissions" ELSE 0 END) AS "7",
    SUM(CASE WHEN "day" = 8 THEN "num_of_emissions" ELSE 0 END) AS "8",
    SUM(CASE WHEN "day" = 9 THEN "num_of_emissions" ELSE 0 END) AS "9",
    SUM(CASE WHEN "day" = 10 THEN "num_of_emissions" ELSE 0 END) AS "10",
    SUM(CASE WHEN "day" = 11 THEN "num_of_emissions" ELSE 0 END) AS "11",
    SUM(CASE WHEN "day" = 12 THEN "num_of_emissions" ELSE 0 END) AS "12",
    SUM(CASE WHEN "day" = 13 THEN "num_of_emissions" ELSE 0 END) AS "13",
    SUM(CASE WHEN "day" = 14 THEN "num_of_emissions" ELSE 0 END) AS "14",
    SUM(CASE WHEN "day" = 15 THEN "num_of_emissions" ELSE 0 END) AS "15",
    SUM(CASE WHEN "day" = 16 THEN "num_of_emissions" ELSE 0 END) AS "16",
    SUM(CASE WHEN "day" = 17 THEN "num_of_emissions" ELSE 0 END) AS "17",
    SUM(CASE WHEN "day" = 18 THEN "num_of_emissions" ELSE 0 END) AS "18",
    SUM(CASE WHEN "day" = 19 THEN "num_of_emissions" ELSE 0 END) AS "19",
    SUM(CASE WHEN "day" = 20 THEN "num_of_emissions" ELSE 0 END) AS "20",
    SUM(CASE WHEN "day" = 21 THEN "num_of_emissions" ELSE 0 END) AS "21",
    SUM(CASE WHEN "day" = 22 THEN "num_of_emissions" ELSE 0 END) AS "22",
    SUM(CASE WHEN "day" = 23 THEN "num_of_emissions" ELSE 0 END) AS "23",
    SUM(CASE WHEN "day" = 24 THEN "num_of_emissions" ELSE 0 END) AS "24",
    SUM(CASE WHEN "day" = 25 THEN "num_of_emissions" ELSE 0 END) AS "25",
    SUM(CASE WHEN "day" = 26 THEN "num_of_emissions" ELSE 0 END) AS "26",
    SUM(CASE WHEN "day" = 27 THEN "num_of_emissions" ELSE 0 END) AS "27",
    SUM(CASE WHEN "day" = 28 THEN "num_of_emissions" ELSE 0 END) AS "28",
    SUM(CASE WHEN "day" = 29 THEN "num_of_emissions" ELSE 0 END) AS "29",
    SUM(CASE WHEN "day" = 30 THEN "num_of_emissions" ELSE 0 END) AS "30",
    SUM(CASE WHEN "day" = 31 THEN "num_of_emissions" ELSE 0 END) AS "31"
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "year" = 2023
GROUP BY "submedium", "brand", "month"
ORDER BY "submedium", "brand", "month";

-- A view for spots per dow for band and submedium returning pivot like table. 
-- Filter by using:
-- SELECT * FROM "spots_per_dow_2023"
-- WHERE "submedium" IN ('FROGGY WEATHER Wrocław', 'BET', 'SOME FM', 'OLD 1', 'TALK FM') AND "month" = 8;
CREATE VIEW "spots_per_dow_2023" AS
SELECT "submedium", "brand", "month",
    SUM(CASE WHEN "day_of_week" = 1 THEN "num_of_emissions" ELSE 0 END) AS mon,
    SUM(CASE WHEN "day_of_week" = 2 THEN "num_of_emissions" ELSE 0 END) AS tue,
    SUM(CASE WHEN "day_of_week" = 3 THEN "num_of_emissions" ELSE 0 END) AS wed,
    SUM(CASE WHEN "day_of_week" = 4 THEN "num_of_emissions" ELSE 0 END) AS thu,
    SUM(CASE WHEN "day_of_week" = 5 THEN "num_of_emissions" ELSE 0 END) AS fri,
    SUM(CASE WHEN "day_of_week" = 6 THEN "num_of_emissions" ELSE 0 END) AS sat,
    SUM(CASE WHEN "day_of_week" = 7 THEN "num_of_emissions" ELSE 0 END) AS sun
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "year" = 2023
GROUP BY "submedium", "brand", "month"
ORDER BY "submedium", "brand", "month";

-- A viev for returning the number of spot emissions by brand, radio station, 
-- and daypart in selectced month, by each brand per radio station and daypart.
-- Filter using for instance:
-- SELECT * FROM "em_daypart_brand_submedium_2023"
-- WHERE "month" = 8 AND "submedium" IN ('BET', 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław', 'TALK FM')
CREATE VIEW "em_daypart_brand_submedium_2023" AS
SELECT "month", "brand", "submedium", "daypart", SUM("num_of_emissions") AS "quantity"
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
JOIN "ad_time_details" ON "ad_time_details"."id" = "ads_desc"."ad_time_details_id"
JOIN "dayparts" ON "dayparts"."id" = "ad_time_details"."daypart_id"
WHERE "year" = 2023
GROUP BY "brand", "submedium", "daypart", "month"
ORDER BY "brand", "submedium", "daypart";

-- A viev for returning the rc costs of spot emissions by brand, radio station, 
-- and daypart in selectced month, by each brand per radio station and daypart.
-- Filter using for instance:
-- SELECT * FROM "em_daypart_brand_submedium_2023"
-- WHERE "month" = 8 AND "submedium" IN ('BET', 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław', 'TALK FM')
CREATE VIEW "rc_daypart_brand_submedium_2023" AS
SELECT "month", "submedium", "brand", "daypart", SUM("cost") AS "rc_cost"
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
JOIN "ad_time_details" ON "ad_time_details"."id" = "ads_desc"."ad_time_details_id"
JOIN "dayparts" ON "dayparts"."id" = "ad_time_details"."daypart_id"
WHERE "year" = 2023
GROUP BY "brand", "submedium", "daypart", "month"
ORDER BY "brand", "submedium", "daypart";

-- Returns the sum of rc costs of all spots 
-- emitted in selectced month, by each brand per radio station.
-- SELECT * FROM "rc_brand_submedium_2023"
-- WHERE "month" = 10 AND "submedium" IN ('BET', 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław', 'TALK FM');
CREATE VIEW "rc_brand_submedium_2023" AS
SELECT "month", "brand", "submedium", SUM("cost") AS "rc_cost" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "year" = 2023
GROUP BY "brand", "submedium", "month"
ORDER BY "rc_cost" DESC, "brand", "submedium";

-- Returns the sum of all spots 
-- emitted in selectced month, by each brand per radio station.
-- SELECT * FROM "em_brand_submedium_2023"
-- WHERE "month" = 10 AND "submedium" IN ('BET', 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław', 'TALK FM');
CREATE VIEW "em_brand_submedium_2023" AS
SELECT "month", "brand", "submedium", SUM("num_of_emissions") AS "quantity" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "year" = 2023
GROUP BY "brand", "submedium", "month"
ORDER BY "quantity" DESC, "brand", "submedium";

-- -- INDEX SECTION -- 
-- CREATE INDEX "brand" ON "brands" ("brand");
-- CREATE INDEX "month" ON "date_time" ("month")
-- CREATE INDEX "year" ON "date_time" ("year")
-- CREATE INDEX "submedium" ON "mediums" ("submedium");




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