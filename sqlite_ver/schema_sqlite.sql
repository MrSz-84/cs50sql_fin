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
);

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
);

-- Create date tables for further filtration and aggregation of data.
-- This table is going to be related to by few others to guarantee e.g. valid joining.
CREATE TABLE IF NOT EXISTS "data_czas" (
    "id" INTEGER,
    "data" TEXT NOT NULL UNIQUE,
    "dzien" INTEGER CHECK("dzien" BETWEEN 1 AND 31),
    "dzien_tyg_nr" INTEGER CHECK("dzien_tyg_nr" BETWEEN 0 AND 7),
    "tydzien" INTEGER CHECK("tydzien" BETWEEN 0 AND 53),
    "miesiac_nr" INTEGER CHECK("miesiac_nr" BETWEEN 1 AND 12),
    "rok" INTEGER CHECK("rok" BETWEEN 1900 AND 9999),
    PRIMARY KEY("id"),
    FOREIGN KEY("dzien_tyg_nr") REFERENCES "dni_tyg"("id"),
    FOREIGN KEY("miesiac_nr") REFERENCES "miesiace"("id")
);

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
);

-- References brodcasters name for mediums table.
CREATE TABLE IF NOT EXISTS "nadawcy" (
    "id" INTEGER,
    "nadawca" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- References reach of given radio station for mediums table.
CREATE TABLE IF NOT EXISTS "zasiegi" (
    "id" INTEGER,
    "zasieg" TEXT NOT NULL UNIQUE CHECK(
        "zasieg" IN (
            'krajowe',
            'miejskie',
            'ponadregionalne',
            'regionalne'
        )
    ),
    PRIMARY KEY("id")
);

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
);

-- Creates table for dayparts references.
CREATE TABLE IF NOT EXISTS "dayparty" (
    "id" INTEGER,
    "daypart" TEXT NOT NULL UNIQUE CHECK(
        "daypart" IN (
            'do 9',
            'od 9 do 16',
            'po 16'
        )
    ),
    PRIMARY KEY("id")
);

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
);

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
);

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
);

-- Create main table with ads emitted through radio estations across country. 
-- This is the table which holds all the data, and to which other tables point.
CREATE TABLE IF NOT EXISTS "spoty" (
    "id" INTEGER,
    "data" TEXT NOT NULL,
    "opis_rek" TEXT NOT NULL,
    "kod_reklamy" INTEGER NOT NULL,
    "brand_id" INTEGER NOT NULL,
    "submedium_id" INTEGER NOT NULL,
    "czas_reklamy_id" INTEGER NOT NULL UNIQUE,
    "typ_produktu_id" INTEGER NOT NULL,
    "koszt" INTEGER,
    "l_emisji" INTEGER NOT NULL CHECK("l_emisji" > 0),
    "typ" TEXT NOT NULL DEFAULT 'reklama',
    PRIMARY KEY("id"),
    FOREIGN KEY("brand_id") REFERENCES "brands"("id"),
    FOREIGN KEY("submedium_id") REFERENCES "submedia"("id"),
    FOREIGN KEY("czas_reklamy_id") REFERENCES "czasy_reklam"("id"),
    FOREIGN KEY("typ_produktu_id") REFERENCES "typy_produktu"("id"),
    FOREIGN KEY("data") REFERENCES "data_czas"("data")
);


-- PROCEDURES, FUNCTIONS, TRIGGER FUNCTIONS SECTION --

-- FUNCTIONS SECTION--

-- TRIGGERS SECTION --
-- Populates day column after date insertion.
CREATE TRIGGER IF NOT EXISTS "populate_dzien"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas" 
    SET "dzien" = CASE 
        WHEN NEW."dzien" IS NULL THEN CAST(strftime('%d', date(NEW."data")) AS INTEGER)
        ELSE NEW."dzien" 
    END 
    WHERE "id" = NEW."id";
END;

-- Populates dow column after date insertion.
CREATE TRIGGER IF NOT EXISTS "populate_dzien_tyg"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "dzien_tyg_nr" = CASE
        WHEN NEW."dzien_tyg_nr" IS NULL THEN 
            CASE 
                WHEN strftime('%w', date(NEW."data")) = '0' THEN 7
                ELSE CAST(strftime('%w', date(NEW."data")) AS INTEGER)
            END
        ELSE NEW."dzien_tyg_nr"
    END
    WHERE "id" = NEW."id";
END;

-- Populates week column after date insertion.
CREATE TRIGGER IF NOT EXISTS "populate_tydzien"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "tydzien" = CASE
        WHEN NEW."tydzien" IS NULL THEN CAST(strftime('%W', date(NEW."data")) AS INTEGER)
        ELSE NEW."tydzien"
    END
    WHERE "id" = NEW."id";
END;

-- Populates month column after date insertion.
CREATE TRIGGER IF NOT EXISTS "populate_miesiac"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "miesiac_nr" = CASE
        WHEN NEW."miesiac_nr" IS NULL THEN CAST(strftime('%m', date(NEW."data")) AS INTEGER)
        ELSE NEW."miesiac_nr"
    END
    WHERE "id" = NEW."id";
END;

-- Populates rok column after date insertion.
CREATE TRIGGER IF NOT EXISTS "populate_rok"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "rok" = CASE
        WHEN NEW."rok" IS NULL THEN CAST(strftime('%Y', date(NEW."data")) AS INTEGER)
        ELSE NEW."rok"
    END
    WHERE "id" = NEW."id";
END;


-- VIEWS SECTION --

-- View for all the relevant data for JOIN statements. 
-- User can go from there and select what they need.
-- Remember to use WHERE statements to filter out data.
-- It's also a good starting point to inporting the data to a data drame.
CREATE VIEW IF NOT EXISTS "reklamy_all" AS
SELECT "data_czas"."data" AS "data", "dzien", "dzien_tyg_nr" AS 'd_tyg_nr', "dzien_tyg", 
"tydzien", "miesiac_nr" AS "m_nr", "miesiac", "rok", "spoty"."kod_reklamy" AS "kod_reklamy", "brand", 
"submedium", "nadawca", "zasieg", "godz_bloku_rek" AS "godz_bloku", "daypart", 
"dl_ujednolicona" AS "dl_spotu", "typ_produktu", "koszt", "typ", "l_emisji" AS "ilosc"
FROM "spoty"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "nadawcy" ON "nadawcy"."id" = "submedia"."nadawca_id"
JOIN "zasiegi" ON "zasiegi"."id" = "submedia"."zasieg_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
JOIN "dl_ujednolicone" ON "dl_ujednolicone"."id" = "czasy_reklam"."dl_ujednolicona_id"
JOIN "typy_produktu" ON "typy_produktu"."id" = "spoty"."typ_produktu_id"
JOIN "dni_tyg" ON "dni_tyg"."id" = "data_czas"."dzien_tyg_nr"
JOIN "miesiace" ON "miesiace"."id" = "data_czas"."miesiac_nr";

-- View of number of spost per day per brand, per medium. For filtering use for instance:
-- SELECT * FROM "spoty_dziennie_2017"
-- WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "miesiac_nr" = 8;
CREATE VIEW IF NOT EXISTS "spoty_dziennie_2017" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2017
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2018" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2018
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2019" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2019
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2020" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2020
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2021" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2021
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2022" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2022
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2023" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2023
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dziennie_2024" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien" = 1 THEN "brand" ELSE 0 END) AS "1",
    COUNT(CASE WHEN "dzien" = 2 THEN "brand" ELSE 0 END) AS "2",
    COUNT(CASE WHEN "dzien" = 3 THEN "brand" ELSE 0 END) AS "3",
    COUNT(CASE WHEN "dzien" = 4 THEN "brand" ELSE 0 END) AS "4",
    COUNT(CASE WHEN "dzien" = 5 THEN "brand" ELSE 0 END) AS "5",
    COUNT(CASE WHEN "dzien" = 6 THEN "brand" ELSE 0 END) AS "6",
    COUNT(CASE WHEN "dzien" = 7 THEN "brand" ELSE 0 END) AS "7",
    COUNT(CASE WHEN "dzien" = 8 THEN "brand" ELSE 0 END) AS "8",
    COUNT(CASE WHEN "dzien" = 9 THEN "brand" ELSE 0 END) AS "9",
    COUNT(CASE WHEN "dzien" = 10 THEN "brand" ELSE 0 END) AS "10",
    COUNT(CASE WHEN "dzien" = 11 THEN "brand" ELSE 0 END) AS "11",
    COUNT(CASE WHEN "dzien" = 12 THEN "brand" ELSE 0 END) AS "12",
    COUNT(CASE WHEN "dzien" = 13 THEN "brand" ELSE 0 END) AS "13",
    COUNT(CASE WHEN "dzien" = 14 THEN "brand" ELSE 0 END) AS "14",
    COUNT(CASE WHEN "dzien" = 15 THEN "brand" ELSE 0 END) AS "15",
    COUNT(CASE WHEN "dzien" = 16 THEN "brand" ELSE 0 END) AS "16",
    COUNT(CASE WHEN "dzien" = 17 THEN "brand" ELSE 0 END) AS "17",
    COUNT(CASE WHEN "dzien" = 18 THEN "brand" ELSE 0 END) AS "18",
    COUNT(CASE WHEN "dzien" = 19 THEN "brand" ELSE 0 END) AS "19",
    COUNT(CASE WHEN "dzien" = 20 THEN "brand" ELSE 0 END) AS "20",
    COUNT(CASE WHEN "dzien" = 21 THEN "brand" ELSE 0 END) AS "21",
    COUNT(CASE WHEN "dzien" = 22 THEN "brand" ELSE 0 END) AS "22",
    COUNT(CASE WHEN "dzien" = 23 THEN "brand" ELSE 0 END) AS "23",
    COUNT(CASE WHEN "dzien" = 24 THEN "brand" ELSE 0 END) AS "24",
    COUNT(CASE WHEN "dzien" = 25 THEN "brand" ELSE 0 END) AS "25",
    COUNT(CASE WHEN "dzien" = 26 THEN "brand" ELSE 0 END) AS "26",
    COUNT(CASE WHEN "dzien" = 27 THEN "brand" ELSE 0 END) AS "27",
    COUNT(CASE WHEN "dzien" = 28 THEN "brand" ELSE 0 END) AS "28",
    COUNT(CASE WHEN "dzien" = 29 THEN "brand" ELSE 0 END) AS "29",
    COUNT(CASE WHEN "dzien" = 30 THEN "brand" ELSE 0 END) AS "30",
    COUNT(CASE WHEN "dzien" = 31 THEN "brand" ELSE 0 END) AS "31"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2024
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";


-- A view for spots per dow for band and submedium returning pivot like table. 
-- Filter by using:
-- SELECT * FROM "spoty_dzien_tyg_2017"
-- WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "miesiac_nr" = 8;
CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2017" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2017
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2018" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2018
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2019" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2019
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2020" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2020
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2021" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2021
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2022" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2022
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2023" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2023
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

CREATE VIEW IF NOT EXISTS "spoty_dzien_tyg_2024" AS
SELECT "submedium", "brand", "miesiac_nr",
    COUNT(CASE WHEN "dzien_tyg_nr" = 1 THEN "brand" ELSE 0 END) AS pon,
    COUNT(CASE WHEN "dzien_tyg_nr" = 2 THEN "brand" ELSE 0 END) AS wt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 3 THEN "brand" ELSE 0 END) AS sr,
    COUNT(CASE WHEN "dzien_tyg_nr" = 4 THEN "brand" ELSE 0 END) AS czw,
    COUNT(CASE WHEN "dzien_tyg_nr" = 5 THEN "brand" ELSE 0 END) AS pt,
    COUNT(CASE WHEN "dzien_tyg_nr" = 6 THEN "brand" ELSE 0 END) AS sob,
    COUNT(CASE WHEN "dzien_tyg_nr" = 7 THEN "brand" ELSE 0 END) AS nd
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2024
GROUP BY "submedium", "brand", "miesiac_nr"
ORDER BY "submedium", "brand", "miesiac_nr";

-- A view for returning the number of spot emissions by brand, radio station, 
-- and daypart in selectced month, by each brand per radio station and daypart.
-- Filter using for instance:
-- A view for spots per dow for band and submedium returning pivot like table. 
-- Filter by using:
-- SELECT * FROM "em_daypart_brand_submedium_2017"
-- WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "miesiac_nr" = 8;
CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2017" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2017
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2018" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2018
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2019" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2019
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2020" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2020
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2021" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2021
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2022" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2022
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2023" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2023
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "em_daypart_brand_submedium_2024" AS
SELECT "miesiac_nr", "brand", "submedium", "daypart", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2024
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";


-- A viev for returning the rc costs of spot emissions by brand, radio station, 
-- and daypart in selectced month, by each brand per radio station and daypart.
-- Filter using for instance:
-- SELECT * FROM "rc_daypart_brand_submedium_2017"
-- WHERE "miesiac_nr" = 8 AND "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM')
CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2017" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2017
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2018" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2018
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2019" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2019
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2020" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2020
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2021" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2021
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2022" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2022
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2023" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2023
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";

CREATE VIEW IF NOT EXISTS "rc_daypart_brand_submedium_2024" AS
SELECT "miesiac_nr", "submedium", "brand", "daypart", SUM("koszt") AS "koszt_rc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "czasy_reklam" ON "czasy_reklam"."id" = "spoty"."czas_reklamy_id"
JOIN "dayparty" ON "dayparty"."id" = "czasy_reklam"."daypart_id"
WHERE "rok" = 2024
GROUP BY "brand", "submedium", "daypart", "miesiac_nr"
ORDER BY "brand", "submedium", "daypart";


-- Returns the sum of rc costs of all spots 
-- emitted in selectced month, by each brand per radio station.
-- SELECT * FROM "rc_brand_submedium_2017"
-- WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "miesiac_nr" = 8;
CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2017" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2017
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2018" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2018
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2019" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2019
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2020" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2020
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2021" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2021
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2022" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2022
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2023" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2023
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "rc_brand_submedium_2024" AS
SELECT "miesiac_nr", "brand", "submedium", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2024
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "koszt_rc" DESC, "brand", "submedium";

-- Returns the sum of all spots 
-- emitted in selectced month, by each brand per radio station.
-- SELECT * FROM "em_brand_submedium_2023"
-- WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "miesiac_nr" = 8;
CREATE VIEW IF NOT EXISTS "em_brand_submedium_2017" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2017
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2018" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2018
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2019" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2019
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2020" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2020
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2021" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2021
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2022" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2022
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2023" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2023
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

CREATE VIEW IF NOT EXISTS "em_brand_submedium_2024" AS
SELECT "miesiac_nr", "brand", "submedium", COUNT("submedium") AS "ilosc"
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
WHERE "rok" = 2024
GROUP BY "brand", "submedium", "miesiac_nr"
ORDER BY "ilosc" DESC, "brand", "submedium";

-- INDEX SECTION -- 
-- CREATE INDEX "dla_ilosci_na_brand" ON "spoty" ("data", "brand_id", "submedium_id");
-- CREATE INDEX "dla_rc_na_brand" ON "spoty" ("data", "brand_id", "submedium_id", "koszt");
-- CREATE INDEX "dla_rc_daypart_na_brand" ON "spoty" ("data", "brand_id", "submedium_id", "czas_reklamy_id", "koszt");
-- CREATE INDEX "dla_ilosci_daypart_na_brand" ON "spoty" ("data", "brand_id", "submedium_id", "czas_reklamy_id");

-- CREATE INDEX "testowy" ON "spoty" ("data", "brand_id", "submedium_id");
