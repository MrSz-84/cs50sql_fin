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
JOIN "miesiace" ON "miesiace"."id" = "data_czas"."miesiac_nr"
WHERE "miesiac_nr" BETWEEN 8 AND 10 AND "rok" BETWEEN 2023 AND 2023;

-- Insert entry for Polish dow names table
INSERT INTO "dni_ty" ("dzien_tyg") 
VALUES ('Poniedziałek');

-- Insert entry for Polish month names table
INSERT INTO "miesiace" ("Miesiac") 
VALUES ('Styczeń');

-- Insert entry for date_time table. Date format is ISO 8601.
INSERT INTO "data_czas" ("data") 
VALUES ('2023-08-01');

-- Insert entry for brands table.
INSERT INTO "brandy" ("brand") 
VALUES ('MEDIA MARKT');

-- Insert entry for unified_lengths table.
INSERT INTO "dl_ujednolicone" ("dl_ujednolicona") 
VALUES (30);

-- Insert entry for dayparts table. 
-- "do 9" means in Polish language "up to 9 AM".
INSERT INTO "dayparty" ("daypart") 
VALUES ('do 9');

-- Insert entry for product_types table.
-- "OGŁOSZENIA O PRACY" means in Polish language "Job annoucement" 
-- or "Job advertisement".
INSERT INTO "typy_produktu" ("typ_produktu") 
VALUES ('OGŁOSZENIA O PRACY');

-- Insert entry for broadcaster table.
INSERT INTO "nadawcy" ("nadawca") 
VALUES ('BAR RADIO');

-- Insert entry for ad_reach table.
-- "krajowe" means in Polish language "nationwide" 
INSERT INTO "zasiegi" ("zasieg") 
VALUES ('krajowe');

-- Insert entry for mediums table.
INSERT INTO "submedia" ("submedium", "nadawca_id", "zasieg_id") 
VALUES ('ESKA Katowice', 8, 2);

-- Insert entry for ad_time_details table.
INSERT INTO "czasy_reklam" (
    "data", 
    "godz_bloku_rek", 
    "gg",
    "mm",
    "dlugosc",
    "daypart_id",
    "dl_ujednolicona_id",
    "kod_rek"
    ) 
VALUES (
    '2023-08-01', 
    '8:00-8:29', 
    8,
    20,
    29,
    1,
    3,
    '2023-08-01 - 22194483 - 1'
    );

-- Insert entry for ads_desc table. This is the main table.
INSERT INTO "spoty" (
    "data", 
    "opis_rek", 
    "kod_reklamy",
    "brand_id",
    "submedium_id",
    "czas_reklamy_id",
    "typ_produktu_id",
    "koszt",
    "l_emisji",
    "typ"
    ) 
VALUES (
    '2023-08-01', 
    'PATRZ BARBARA NO ALE..MEGA OKAZJE..SF SAMSUNG GALAXY M33 4XAP 5G..999ZŁ', 
    22194483,
    2,
    78,
    1,
    2,
    310,
    1,
    'reklama'
    );




