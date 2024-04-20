-- Returns the sum of ratecard costs of all spots 
-- emitted in selectced month, by each brand.
SELECT "brand", SUM("koszt") AS "koszt_rc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
WHERE "miesiac_nr" = 8
GROUP BY "brand"
ORDER BY "koszt_rc" DESC;

-- Returns the sum of all spots 
-- emitted in selectced month, by each brand.
SELECT "brand", COUNT("brand") AS "ilosc" 
FROM "spoty"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
WHERE "miesiac_nr" = 8
GROUP BY "brand"
ORDER BY "ilosc" DESC;

-- Returns the sum all spots 
-- emitted in selectced month, by each brand per radio station.
SELECT * FROM "em_brand_submedium_2023"
WHERE "miesiac_nr" = 10 AND "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM');

-- Returns the sum of rc costs of all spots 
-- emitted in selectced month, by each brand per radio station.
SELECT * FROM "rc_brand_submedium_2023"
WHERE "miesiac_nr" = 10 AND "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM');

-- Return the number of spot emissions by brand, radio station and daypart
-- in selectced month, by each brand per radio station and daypart.
SELECT * FROM "em_daypart_brand_submedium_2023"
WHERE "miesiac_nr" = 10 AND "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM');

-- Return the ratecard cost of spot emissions by brand, radio station and daypart
-- in selectced month, by each brand per radio station and daypart.
SELECT * FROM "em_daypart_brand_submedium_2023"
WHERE "miesiac_nr" = 10 AND "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM');

-- Return a pivot like table for number of spost per radio station 
-- per brand for each dow.
SELECT * FROM "spots_per_day_2023"
WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "month" = 9;

-- Return a pivot like table for number of spost per radio station 
-- per brand for each dow.
SELECT * FROM "spoty_dziennie_2017"
WHERE "submedium" IN ('ESKA Wrocław', 'ZET', 'RMF FM', 'PR 1', 'TOK FM') AND "miesiac_nr" = 8;

-- Returns the complete data set for selected period of time, 
-- for frurther processing in Pandas.
SELECT "data_czas"."data" AS "data", "dzien", "dzien_tyg_nr" AS 'd_tyg_nr', "dzien_tyg", 
"tydzien", "miesiac_nr" AS "m_nr", "miesiac", "rok", "kod_rek" AS "kod_reklamy", "brand", 
"submedium", "nadawca", "zasieg", "godz_blok_rek" AS "godz_bloku", "daypart", 
"dl_ujednolicona" AS "dl_spotu", "typ_produktu", "koszt", "typ_rek", "l_emisji" AS "ilosc"
FROM "spoty"
JOIN "data_czas" ON "data_czas"."data" = "spoty"."data"
JOIN "brandy" ON "brandy"."id" = "spoty"."brand_id"
JOIN "submedia" ON "submedia"."id" = "spoty"."submedium_id"
JOIN "nadawcy" ON "nadawcy"."id" = "submedia"."nadawca_id"
JOIN "zasiegi" ON "zasiegi"."id" = "submedia"."zasieg_id"
JOIN "dayparty" ON "dayparty"."id" = "spoty"."daypart_id"
JOIN "dl_ujednolicone" ON "dl_ujednolicone"."id" = "spoty"."dl_ujednolicona_id"
JOIN "typy_produktu" ON "typy_produktu"."id" = "spoty"."typ_produktu_id"
JOIN "dni_tyg" ON "dni_tyg"."id" = "data_czas"."dzien_tyg_nr"
JOIN "miesiace" ON "miesiace"."id" = "data_czas"."miesiac_nr"
JOIN "bloki_rek" ON "bloki_rek"."id" = "spoty"."blok_rek_id"
JOIN "kody_rek" ON "kody_rek"."id" = "spoty"."kod_rek_id"
JOIN "typy_rek" ON "typy_rek"."id" = "spoty"."typ_rek_id"
WHERE "miesiac_nr" BETWEEN 8 AND 8 AND "rok" BETWEEN 2023 AND 2023;

-- Insert entry for dni_tyg table
INSERT INTO "dni_ty" ("dzien_tyg") 
VALUES ('Poniedziałek');

-- Insert entry for miesiace table
INSERT INTO "miesiace" ("Miesiac") 
VALUES ('Styczeń');

-- Insert entry for data_czas table. Date format is ISO 8601.
INSERT INTO "data_czas" ("data") 
VALUES ('2023-08-01');

-- Insert entry for brandy table.
INSERT INTO "brandy" ("brand") 
VALUES ('MEDIA MARKT');

-- Insert entry for dl_ujednolicone table.
INSERT INTO "dl_ujednolicone" ("dl_ujednolicona") 
VALUES (30);

-- Insert entry for dayparty table. 
-- "do 9" means in Polish language "up to 9 AM".
INSERT INTO "dayparty" ("daypart") 
VALUES ('do 9');

-- Insert entry for typy_produktu table.
-- "OGŁOSZENIA O PRACY" means in Polish language "Job annoucement" 
-- or "Job advertisement".
INSERT INTO "typy_produktu" ("typ_produktu") 
VALUES ('OGŁOSZENIA O PRACY');

-- Insert entry for nadawca table.
INSERT INTO "nadawcy" ("nadawca") 
VALUES ('BAR RADIO');

-- Insert entry for zasiegi table.
-- "krajowe" means in Polish language "nationwide" 
INSERT INTO "zasiegi" ("zasieg") 
VALUES ('krajowe');

-- Insert entry for kody_rek table.
INSERT INTO "kody_rek" ("kod_rek", "opis_spotu") 
VALUES (22197532, 'Some text goses in here');

-- Insert entry for blogi_rek table.

INSERT INTO "bloki_rek" ("godz_blok_rek") 
VALUES ('15:30-15:59');

-- Insert entry for typy_rek table.
INSERT INTO "typy_rek" ("typ_rek") 
VALUES ('reklama');

-- Insert entry for submedia table.
INSERT INTO "submedia" ("submedium", "nadawca_id", "zasieg_id") 
VALUES ('ESKA Katowice', 8, 2);

-- Insert entry for spoty table. This is the main table.
INSERT INTO "spoty" (
    "data",
    "gg",
    "mm",
    'ss',
    "koszt",
    "dlugosc" ,
    "kod_rek_id",
    "daypart_id",
    "dl_ujednolicona_id",
    "blok_rek_id",
    "brand_id",
    "submedium_id",
    "typ_produktu_id",
    "l_emisji",
    "typ_rek_id",
    ) 
VALUES (
    '2023-08-01',
    12,
    59,
    25,
    310,
    14,
    2411,
    2,
    2,
    14,
    2,
    56,
    1,
    2'
    );




