
    -- CREATE TABLE t(x INTEGER PRIMARY KEY ASC, y, z);
    -- CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));
    -- CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x DESC)); 


-- TABLES SECTION --

-- Table injecting Polish day of week names into the date_time table.
CREATE TABLE IF NOT EXISTS "dni_tyg" (
    "id" INTEGER,
    "dzien_tyg" TEXT NOT NULL UNIQUE CHECK(
        "dzien_tyg" IN (
            'Poniedziałek', 'Wtorek', 'Środa', 
            'Czwartek', 'Piątek', 'Sobota', 'Niedziela'
        )
    ),
    PRIMARY KEY("id")
) STRICT;

-- Table injecting Polish month names into the date_time table.
CREATE TABLE IF NOT EXISTS "miesiace" (
    "id" INTEGER,
    "miesiac" TEXT NOT NULL UNIQUE CHECK(
        "miesiac" IN (
            'Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj',
            'Czerwiec', 'Lipiec', 'Sierpień', 'Wrzesień',
            'Październik', 'Listopad', 'Grudzień'
        )
    ),
    PRIMARY KEY("id")
) STRICT;

-- Create date tables for further filtration and aggregation of data.
-- This table is going to be related to by few others to guarantee e.g. valid joining.
CREATE TABLE IF NOT EXISTS "data_czas" (
    "id" INTEGER,
    "data" TEXT NOT NULL UNIQUE,
    "dzien" INTEGER CHECK("dzien" BETWEEN 1 AND 31),
    "dzien_tyg_nr" INTEGER CHECK("dzien_tyg_nr" BETWEEN 1 AND 7),
    "tydzien" INTEGER CHECK("tydzien" BETWEEN 1 AND 53),
    "miesiac_nr" INTEGER CHECK("miesiac_nr" BETWEEN 1 AND 12),
    "rok" INTEGER CHECK("rok" BETWEEN 1900 AND 9999),
    PRIMARY KEY("id"),
    FOREIGN KEY("dzien_tyg_nr") REFERENCES "dni_tyg"("id"),
    FOREIGN KEY("miesiac_nr") REFERENCES "miesiace"("id")
) STRICT;

-- Create brands table
CREATE TABLE IF NOT EXISTS "brandy" (
    "id" INTEGER,
    "brand" TEXT NOT NULL UNIQUE CHECK(
        "brand" IN (
            'EURO RTV AGD',
            'MEDIA EXPERT',
            'MEDIA MARKT',
            'MYCENTER',
            'NEONET AGD RTV'
        )
    ),
    PRIMARY KEY("id")
) STRICT;

-- References brodcasters name for mediums table.
CREATE TABLE IF NOT EXISTS "nadawcy" (
    "id" INTEGER,
    "nadawca" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
) STRICT;

-- References reach of given radio station for mediums table.
CREATE TABLE IF NOT EXISTS "zasiegi" (
    "id" SERIAL,
    "zasieg" TEXT NOT NULL UNIQUE CHECK(
        "zasieg" IN (
            'krajowe',
            'miejskie',
            'ponadregionalne',
            'regionalne'
        )
    ),
    PRIMARY KEY("id")
) STRICT;

-- Crteates table containing radiostactions, their parent entity (broadcaster),
-- and the reach of each medium.
CREATE TABLE IF NOT EXISTS "submedia" (
    "id" INTEGER,
    "submedium" TEXT NOT NULL UNIQUE,
    "nadawca_id" INTEGER NOT NULL,
    "zasieg_id" INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("nadawca_id") REFERENCES "nadawcy"("id"),
    FOREIGN KEY("zasieg_id") REFERENCES "zasiegi"("id")
) STRICT;

-- Creates table for dayparts references.
CREATE TABLE IF NOT EXISTS "dayparty" (
    "id" INTEGER,
    "daypart" TEXT NOT NULL UNIQUE CHECK(
        "daypart" IN (
            'do 9',
            'od 9 do 16',
            'po 16',
        )
    ),
    PRIMARY KEY("id")
) STRICT;

-- Creates table for unified advertisemens lenght references.
CREATE TABLE IF NOT EXISTS "dl_ujednolicone" (
    "id" INTEGER,
    "dl_ujednolicona" INTEGER NOT NULL UNIQUE CHECK(
        "dl_ujednolicona" IN (
            10,
            15,
            20,
            30,
            45,
            60
        )
    ),
    PRIMARY KEY("id")
) STRICT;

CREATE TABLE IF NOT EXISTS "czasy_reklam" (
    "id" INTEGER,
    "data" TEXT NOT NULL,
    "godz_bloku_rek" TEXT NOT NULL,
    "gg" INTEGER NOT NULL,
    "mm" INTEGER NOT NULL,
    "dlugosc" INTEGER NOT NULL,
    "daypart_id" INTEGER NOT NULL,
    "dl_ujednolicona_id" INTEGER NOT NULL,
    "kod_rek" TEXT NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("daypart_id") REFERENCES "dayparty"("id"),
    FOREIGN KEY("dl_ujednolicona_id") REFERENCES "dl_ujednolicone"("id")
) STRICT;

-- Creates pprodyct type references for ads_desc table.
CREATE TABLE IF NOT EXISTS "typy_produktu" (
    "id" INTEGER,
    "typ_produktu" TEXT NOT NULL UNIQUE CHECK(
        "typ_produktu" IN (
            'AGD, RTV, ELEKTRONIKA, FOTOGRAFIA, KOMPUTERY',
            'GRUPA',
            'OGŁOSZENIA O PRACY',
            'TARGI'
        )
    ),
    PRIMARY KEY("id")
) STRICT;

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