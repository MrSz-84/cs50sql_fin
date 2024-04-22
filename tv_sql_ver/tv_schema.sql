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
        dzien_tyg IN (
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
    "kod_rek" INTEGER NOT NULL UNIQUE,
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
    "brand" INTEGER NOT NULL UNIQUE,
    "producer_id" INTEGER NOT NULL,
    "syndicate_id" INTEGER NOT NULL,
    PRIMARY KEY('id'),
    FOREIGN KEY("producer_id") REFERENCES "producers"("id"),
    FOREIGN KEY("syndicate_id") REFERENCES "syndicates"("id")
);

-- Creates channel_gr table, contaning channel group names.
CREATE TABLE IF NOT EXISTS "channel_gr" (
    "id" INTEGER,
    "channel_gr" TEXT NOT NULL UNIQUE
);

-- Creates channels table, contaning channel name channel group id.
CREATE TABLE IF NOT EXISTS "channels" (
    "id" INTEGER,
    "channel" TEXT NOT NULL UNIQUE,
    "channel_gr_id" INTEGER NOT NULL,
    PRIMARY KEY("id"),
    FOREIGN KEY("channel_gr_id") REFERENCES "channels"("id")
);

-- Creates dayparts, containing dayparts at which ads were emitted.
CREATE TABLE IF NOT EXISTS "dayparts" (
    "id" INTEGER,
    "daypart" TEXT NOT NULL UNIQUE
);


-- TRIGGERS SECTION

-- Adds entry at dzien table, after population of data row.
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

-- Adds entry at dzien_tyg_nr table, after population of data row.
CREATE TRIGGER IF NOT EXISTS "dodaj_dzien_tyg"
AFTER INSERT ON "data_czas"
FOR EACH ROW
BEGIN
    UPDATE "data_czas"
    SET "dzien_tyg_nr" = CASE 
        WHEN NEW."dzien_tyg_nr" IS NULL THEN
            CASE 
                WHEN CAST(strftime('%u', NEW."data") AS INTEGER) = 0 THEN 7
                ELSE CAST(strftime('%u', NEW."data") AS INTEGER)
            END
        ELSE NEW."dzien_tyg_nr"
    END
    WHERE "id" = NEW."id";
END;

-- Adds entry at tydzien table, after population of data row.
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

-- Adds entry at rok table, after population of data row.
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
