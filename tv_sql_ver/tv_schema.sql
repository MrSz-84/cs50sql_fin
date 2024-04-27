-- DATABASE CREATION SECTION --
-- Creates miesiace table, containing Polish month names.
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

-- Creates dni_tyg table, containing Polish names for dows.
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

-- Creates data_czas table, containing date related data. Time data are absent at this point.
CREATE TABLE IF NOT EXISTS "data_czas"(
    "id" INTEGER,
    "data" TEXT NOT NULL UNIQUE,
    "dzien" INTEGER CHECK("dzien" BETWEEN 1 AND 31),
    "dzien_tyg_nr" INTEGER CHECK("dzien_tyg_nr" BETWEEN 1 AND 7),
    "tydzien" INTEGER CHECK("tydzien" BETWEEN 0 AND 53),
    "miesiac_nr" INTEGER CHECK("miesiac_nr" BETWEEN 1 AND 12),
    "rok" INTEGER CHECK("rok" BETWEEN 1900 AND 9999),
    PRIMARY KEY("id"),
    FOREIGN KEY("dzien_tyg_nr") REFERENCES "dni_tyg"("id"),
    FOREIGN KEY("miesiac_nr") REFERENCES "miesiace"("id")
);

-- Creates kody_rek table, containing ad codes and brief description of ad contents.
CREATE TABLE IF NOT EXISTS "kody_rek" (
    "id" INTEGER,
    "kod_rek" INTEGER UNIQUE,
    "opis" TEXT NOT NULL,
    PRIMARY KEY("id")
);

-- Creates producers table, containing producer names.
CREATE TABLE IF NOT EXISTS "producers" (
    "id" INTEGER,
    "producer" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates syndicates table, contaning syndicate names.
CREATE TABLE IF NOT EXISTS "syndicates" (
    "id" INTEGER,
    "syndicate" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates brandy table, contaning names of the brands instructing advertisements,
-- producers, and syndicates ids.
CREATE TABLE IF NOT EXISTS "brandy" (
    "id" INTEGER,
    "brand" TEXT NOT NULL UNIQUE,
    "producer_id" INTEGER NOT NULL,
    "syndicate_id" INTEGER NOT NULL,
    PRIMARY KEY('id'),
    FOREIGN KEY("producer_id") REFERENCES "producers"("id"),
    FOREIGN KEY("syndicate_id") REFERENCES "syndicates"("id")
);

-- Creates kanaly_gr table, contaning channel group names.
CREATE TABLE IF NOT EXISTS "kanaly_gr" (
    "id" INTEGER,
    "kanal_gr" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates kanaly table, contaning channel name channel group id.
CREATE TABLE IF NOT EXISTS "kanaly" (
    "id" INTEGER,
    "kanal" TEXT NOT NULL UNIQUE,
    "kanal_gr_id" INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("kanal_gr_id") REFERENCES "kanaly_gr"("id")
);

-- Creates dayparty table, containing dayparts at which ads were emitted.
CREATE TABLE IF NOT EXISTS "dayparty" (
    "id" INTEGER,
    "daypart" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates pib_real_rels table, containing relative real positions in break.
CREATE TABLE IF NOT EXISTS "pib_real_rels" (
    "id" INTEGER,
    "pib_real_rel" TEXT UNIQUE NOT NULL,
    PRIMARY KEY("id")
);

-- Creates dlugosci table, containing the lengths of emitted ads.
CREATE TABLE IF NOT EXISTS "dlugosci" (
    "id" INTEGER,
    "dlugosc" INTEGER NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates klasy_spotow table, containing type of emitted ad.
CREATE TABLE IF NOT EXISTS "klasy_spotu" (
    "id" INTEGER,
    "klasa_spotu" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates kody_blokow table, containing block codes of an emitted ad.
CREATE TABLE IF NOT EXISTS "kody_bloku" (
    "id" INTEGER,
    "kod_bloku" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Creates prog_campaigns table, containing information in which tv programme given ad was emitted.
CREATE TABLE IF NOT EXISTS "prog_kampanie" (
    "id" INTEGER,
    "prog_kampania" TEXT UNIQUE NOT NULL,
    PRIMARY KEY("id")
);

-- Creates programy_przed table, containing information before which tv programme given ad was emitted.
CREATE TABLE IF NOT EXISTS "programy_przed" (
    "id" INTEGER,
    "program_przed" TEXT UNIQUE NOT NULL,
    PRIMARY KEY("id")
);

-- Creates programy_po table, containing information after which tv programme given ad was emitted.
CREATE TABLE IF NOT EXISTS "programy_po" (
    "id" INTEGER,
    "program_po" TEXT UNIQUE NOT NULL,
    PRIMARY KEY("id")
);

-- Creates main table spoty, containing unique spot emissions for every analysed brand.
CREATE TABLE IF NOT EXISTS "spoty" (
    "id" INTEGER,
    "data" TEXT NOT NULL,
    "czas" TEXT NOT NULL,
    "pib_pos" INTEGER NOT NULL,
    "pib_count" INTEGER NOT NULL,
    "pib_real_rel_id" INTEGER NOT NULL,
    "klasa_spotu_id" INTEGER NOT NULL,
    "kod_bloku_id" INTEGER NOT NULL,
    "daypart_id" INTEGER NOT NULL,
    "grp" REAL NOT NULL,
    "kanal_id" INTEGER NOT NULL,
    "brand_id" INTEGER NOT NULL,
    "dlugosc_id" INTEGER NOT NULL,
    "kod_rek_id" INTEGER NOT NULL,
    "prog_kampania_id" INTEGER NOT NULL,
    "program_przed_id" INTEGER NOT NULL,
    "program_po_id" INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("pib_real_rel_id") REFERENCES "pib_real_rels"("id"),
    FOREIGN KEY("klasa_spotu_id") REFERENCES "klasay_spotu"("id"),
    FOREIGN KEY("kod_bloku_id") REFERENCES "kody_bloku"("id"),
    FOREIGN KEY("kanal_id") REFERENCES "kanaly"("id"),
    FOREIGN KEY("brand_id") REFERENCES "brandy"("id"),
    FOREIGN KEY("kod_rek_id") REFERENCES "kody_rek"("id"),
    FOREIGN KEY("prog_kampania_id") REFERENCES "prog_kampanie"("id"),
    FOREIGN KEY("program_przed_id") REFERENCES "programy_przed"("id"),
    FOREIGN KEY("program_po_id") REFERENCES "programy_po"("id"),
    FOREIGN KEY("dlugosc_id") REFERENCES "dlugosci"("id")
);


-- VIEWS SECTION --
-- View for instead of usage workaround, in order to populate kanaly table.
CREATE VIEW IF NOT EXISTS "podziel_kanaly_view" AS 
SELECT * FROM "kanaly";

-- View for instead of usage workaround, in order to populate brandy table.
CREATE VIEW IF NOT EXISTS "podziel_brandy_view" AS 
SELECT * FROM "brandy";


-- TRIGGERS SECTION --
-- Populates brandy table from concatenated data inputed into podziel_brandy_view channel column 
CREATE TRIGGER IF NOT EXISTS "podziel_brandy_trig"
INSTEAD OF INSERT ON "podziel_brandy_view"
FOR EACH ROW
BEGIN
    INSERT OR IGNORE INTO "producers"("producer") VALUES(
        CAST(substring(NEW."brand", instr(NEW."brand" ,'@|@') + 3, 
        instr(NEW."brand", '#|#') - (instr(NEW."brand" ,'@|@') + 3)
        ) AS TEXT)
    );
    INSERT OR IGNORE INTO "syndicates"("syndicate") VALUES(
        CAST(substring(NEW."brand", instr(NEW."brand", '#|#') + 3) AS TEXT)
    );
    INSERT INTO "brandy"("brand", "producer_id", "syndicate_id")
    SELECT
        CAST(substring(NEW."brand", 1, instr(NEW."brand", '@|@') - 1) AS TEXT),
    (SELECT "id" FROM "producers" 
        WHERE "producer" = (
            CAST(substring(NEW."brand", instr(NEW."brand" ,'@|@') + 3, 
                instr(NEW."brand", '#|#') - (instr(NEW."brand" ,'@|@') + 3)
            ) AS TEXT)
        )
    ),
    (SELECT "id" FROM "syndicates" 
        WHERE "syndicate" = (
            CAST(substring(NEW."brand", instr(NEW."brand", '#|#') + 3) AS TEXT)
        )
    );
END;

-- Populates kanaly table from concatenated data inputed into podziel_kanaly_view channel column 
CREATE TRIGGER IF NOT EXISTS "podziel_kanaly_trig"
INSTEAD OF INSERT ON "podziel_kanaly_view"
FOR EACH ROW
BEGIN
    INSERT OR IGNORE INTO "kanaly_gr"("kanal_gr") VALUES(
        CAST(substring(NEW."kanal", instr(NEW."kanal", '@|@') + 3) AS TEXT)
    );
    INSERT INTO "kanaly"("kanal", "kanal_gr_id")
    SELECT
        CAST(substring(NEW."kanal", 1, instr(NEW."kanal", '@|@') - 1) AS TEXT),
        "id" 
        FROM "kanaly_gr"
        WHERE "kanal_gr" = (
            CAST(substring(NEW."kanal", instr(NEW."kanal", '@|@') + 3) AS TEXT)
        );
END;

-- Populates kody_rek table from concatenated data inputed into opis column
CREATE TRIGGER IF NOT EXISTS "podziel_opis"
AFTER INSERT ON "kody_rek"
FOR EACH ROW
BEGIN
    UPDATE "kody_rek"
    SET "kod_rek" = CAST(substring(NEW."opis", 1, instr(NEW."opis", '@|@') - 1) AS INTEGER)
    WHERE "id" = NEW."id";
    UPDATE "kody_rek"
    SET "opis" = CAST(substring(NEW."opis", instr(NEW."opis", '@|@') + 3) AS TEXT)
    WHERE "id" = NEW."id";
END;

-- Adds entry at dzien column, after population of data row.
CREATE TRIGGER IF NOT EXISTS "dodaj_dzien"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "dzien" =  CASE
        WHEN NEW."dzien" IS NULL THEN CAST(strftime('%d', NEW."data") AS INTEGER)
        ELSE NEW."dzien"
    END
    WHERE "id" = NEW."id";
END;

-- Adds entry at dzien_tyg_nr column, after population of data row.
CREATE TRIGGER IF NOT EXISTS "dodaj_dzien_tyg"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "dzien_tyg_nr" = CASE 
        WHEN NEW."dzien_tyg_nr" IS NULL THEN
            CASE 
                WHEN CAST(strftime('%w', NEW."data") AS INTEGER) = 0 THEN 7
                ELSE CAST(strftime('%w', NEW."data") AS INTEGER)
            END
        ELSE NEW."dzien_tyg_nr"
    END
    WHERE "id" = NEW."id";
END;

-- Adds entry at tydzien column, after population of data row.
CREATE TRIGGER IF NOT EXISTS "dodaj_tydzien"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "tydzien" =  CASE
        WHEN NEW."tydzien" IS NULL THEN CAST(strftime('%W', NEW."data") AS INTEGER)
        ELSE NEW."tydzien"
    END
    WHERE "id" = NEW."id";
END;

-- Adds entry at miesiac_nr column, after population of data row.
CREATE TRIGGER IF NOT EXISTS "dodaj_miesiac"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas" 
    SET "miesiac_nr" = CASE
        WHEN NEW."miesiac_nr" IS NULL THEN CAST(strftime('%m', NEW."data") AS INTEGER)
        ELSE NEW."miesiac_nr"
    END
    WHERE "id" = NEW."id";
END;

-- Adds entry at rok column, after population of data row.
CREATE TRIGGER IF NOT EXISTS "dodaj_rok"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas" 
    SET "rok" = CASE
        WHEN NEW."rok" IS NULL THEN CAST(strftime('%Y', NEW."data") AS INTEGER)
        ELSE NEW."rok"
    END
    WHERE "id" = NEW."id";
END;


-- POPULATE SOME TABLES ON CREATION --
-- Add month names
INSERT INTO "miesiace" ("miesiac") VALUES
('Styczeń'), ('Luty'), ('Marzec'), ('Kwiecień'), ('Maj'),
('Czerwiec'), ('Lipiec'), ('Sierpień'), ('Wrzesień'),
('Październik'), ('Listopad'), ('Grudzień');


-- Add day of week 
INSERT INTO "dni_tyg" ("dzien_tyg") VALUES
('Poniedziałek'), ('Wtorek'), ('Środa'), 
('Czwartek'), ('Piątek'), ('Sobota'), ('Niedziela');





