
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

CREATE TABLE IF NOT EXISTS "kody_rek" (
    "id" INTEGER,
    "kod_rek" INTEGER NOT NULL UNIQUE,
    "opis" TEXT NOT NULL,
    PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "producers" (
    "id" INTEGER,
    "producer" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "syndicates" (
    "id" INTEGER,
    "syndicate" TEXT NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

CREATE TABLE IF NOT EXISTS "brandy" (
    "id" INTEGER,
    "brand" INTEGER NOT NULL UNIQUE,
    "producer_id" INTEGER NOT NULL,
    "syndicate_id" INTEGER NOT NULL,
    PRIMARY KEY('id'),
    FOREIGN KEY("producer_id") REFERENCES "producers"("id"),
    FOREIGN KEY("syndicate_id") REFERENCES "syndicates"("id")
);



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
