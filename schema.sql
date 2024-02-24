-- In this SQL file, write (and comment!) the schema of 
-- your database, including the CREATE TABLE, CREATE INDEX, 
-- CREATE VIEW, etc. statements that compose it

-- Create ENUM type for pl dow names.
CREATE TYPE "pl_dow" AS ENUM('Poniedziałek', 'Wtorek', 'Środa',
    'Czwartek', 'Piątek', 'Sobota', 'Niedziela'
);

-- Table injecting Polish day of week names into the date_time table.
CREATE TABLE IF NOT EXISTS "pl_dow_name" (
    "id" SERIAL UNIQUE,
    "dow_name" VARCHAR(12) NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Create Enum type for pl month names.
CREATE TYPE "pl_month" AS ENUM('Styczeń', 'Luty', 'Marzec', 'Kwiecień', 'Maj', 
    'Czerwiec', 'Lipiec', 'Sierpień', 'Wrzesień', 'Październik', 
    'Listopad', 'Grudzień'
);

-- Table injecting Piolish month names into the date_time table.
CREATE TABLE IF NOT EXISTS "pl_month_name" (
    "id" SERIAL UNIQUE,
    "month_name" "pl_month" NOT NULL UNIQUE,
    PRIMARY KEY("id")
);

-- Create date tables for further filtration and aggregation of data.
-- This table is going to be related to by few others to guarantee e.g. valid joining.
CREATE TABLE IF NOT EXISTS "date_time" (
    "id" SERIAL UNIQUE,
    "date" DATE NOT NULL UNIQUE,
    "day" SMALLINT CHECK("day" BETWEEN 1 AND 31),
    "day_of_week" SMALLINT CHECK("day_of_week" BETWEEN 1 AND 7),
    "week" SMALLINT CHECK("week" BETWEEN 1 AND 53),
    "year" SMALLINT CHECK("year" BETWEEN 1900 AND 9999),
    "month" SMALLINT CHECK("month" BETWEEN 1 AND 12),
    PRIMARY KEY("id"),
    FOREIGN KEY("day_of_week") REFERENCES "pl_dow_name"("id"),
    FOREIGN KEY("month") REFERENCES "pl_month_name"("id")
);

CREATE TABLE IF NOT EXISTS "ads_desc" (

)


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