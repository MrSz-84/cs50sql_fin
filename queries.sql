-- Returns the sum of ratecard costs of all spots 
-- emitted in selectced month, by each brand.
SELECT "brand", SUM("cost") AS "rc_cost" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
WHERE "month" = 8
GROUP BY "brand"
ORDER BY "rc_cost" DESC;

-- Returns the sum of all spots 
-- emitted in selectced month, by each brand.
SELECT "brand", SUM("num_of_emissions") AS "quantity" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
WHERE "month" = 8
GROUP BY "brand"
ORDER BY "quantity" DESC;

-- Returns the sum of ratecard costs of all spots 
-- emitted in selectced month, by each brand per radio station.
SELECT "brand", "submedium", SUM("cost") AS "rc_cost" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "month" = 8 AND "submedium" IN ('BET' , 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław')
GROUP BY "brand", "submedium"
ORDER BY "rc_cost" DESC;

-- Returns the sum all spots 
-- emitted in selectced month, by each brand per radio station.
SELECT "brand", "submedium", SUM("num_of_emissions") AS "quantity" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "month" = 8 AND "submedium" IN ('BET' , 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław')
GROUP BY "brand", "submedium"
ORDER BY "quantity" DESC;


-- Returns the sum of all spots emitted and their percentage
-- in selectced month, by each brand per radio station and daypart.
SELECT "brand", "submedium", SUM("num_of_emissions") AS "spots quantity" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "month" = 8 AND "submedium" IN ('BET' , 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław')
GROUP BY "brand", "submedium"
ORDER BY "brand", "submedium";

-- Returns the sum of ratecard costs of all spots emitted and their percentage
-- in selectced month, by each brand per radio station and daypart.
SELECT "brand", "submedium", SUM("cost") AS "rc_cost" 
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
WHERE "month" = 8 AND "submedium" IN ('BET' , 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław')
GROUP BY "brand", "submedium"
ORDER BY "brand", "submedium";


-- Return the number of spot emissions by brand, radio station and daypart
SELECT "brand", "submedium", "daypart", SUM("num_of_emissions") AS "quantity"
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
JOIN "ad_time_details" ON "ad_time_details"."id" = "ads_desc"."ad_time_details_id"
JOIN "dayparts" ON "dayparts"."id" = "ad_time_details"."daypart_id"
WHERE "month" = 8 AND "submedium" IN ('BET' , 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław')
GROUP BY "brand", "submedium", "daypart"
ORDER BY "brand", "submedium";

-- Return the ratecard cost of spot emissions by brand, radio station and daypart
SELECT "brand", "submedium", "daypart", SUM("cost") AS "rc_cost"
FROM "ads_desc"
JOIN "brands" ON "brands"."id" = "ads_desc"."brand_id"
JOIN "date_time" ON "date_time"."date" = "ads_desc"."date"
JOIN "mediums" ON "mediums"."id" = "ads_desc"."medium_id"
JOIN "ad_time_details" ON "ad_time_details"."id" = "ads_desc"."ad_time_details_id"
JOIN "dayparts" ON "dayparts"."id" = "ad_time_details"."daypart_id"
WHERE "month" = 8 AND "submedium" IN ('BET' , 'SOME FM', 'OLD 1', 'FROGGY WEATHER Wrocław')
GROUP BY "brand", "submedium", "daypart"
ORDER BY "brand", "submedium";


-- Return a pivot like table for number of spost per radio station 
-- per brand for each dow.
SELECT "submedium", "brand",
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
WHERE "submedium" IN ('FROGGY WEATHER Wrocław', 'BET', 'SOME FM') AND "month" = 8
GROUP BY "submedium", "brand";

-- Return a pivot like table for number of spost per radio station 
-- per brand for each dom.
SELECT "submedium", "brand" AS "brand | august",
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
WHERE "submedium" IN ('FROGGY WEATHER Wrocław', 'BET', 'SOME FM') AND "month" = 8
GROUP BY "submedium", "brand";

-- Returns the complete data set for selected period of time, 
-- for frurther processing in Pandas.
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
JOIN "pl_month_names" ON "pl_month_names"."id" = "date_time"."month"
WHERE "month" BETWEEN 8 AND 10 AND "year" BETWEEN 2023 AND 2023;

-- Insert entry for Polish dow names table
INSERT INTO "pl_dow_names" ("dow_name") 
VALUES ('Poniedziałek');

-- Insert entry for Polish month names table
INSERT INTO "pl_month_name" ("month_name") 
VALUES ('Styczeń');

-- Insert entry for date_time table. Date format is ISO 8601.
INSERT INTO "date_time" ("date") 
VALUES ('2023-08-01');

-- Insert entry for brands table.
INSERT INTO "brands" ("brand") 
VALUES ('MEDIA SHOP');

-- Insert entry for unified_lengths table.
INSERT INTO "unified_lengths" ("length") 
VALUES (30);

-- Insert entry for dayparts table. 
-- "do 9" means in Polish language "up to 9 AM".
INSERT INTO "dayparts" ("daypart") 
VALUES ('do 9');

-- Insert entry for product_types table.
-- "OGŁOSZENIA O PRACY" means in Polish language "Job annoucement" 
-- or "Job advertisement".
INSERT INTO "product_types" ("product_type") 
VALUES ('OGŁOSZENIA O PRACY');

-- Insert entry for broadcaster table.
INSERT INTO "broadcasters" ("broadcaster") 
VALUES ('BAR RADIO');

-- Insert entry for ad_reach table.
-- "krajowe" means in Polish language "nationwide" 
INSERT INTO "ad_reach" ("reach") 
VALUES ('krajowe');

-- Insert entry for mediums table.
INSERT INTO "mediums" ("submedium", "broadcaster_id", "ad_reach_id") 
VALUES ('FROGGY WEATHER Katowice', 8, 2);

-- Insert entry for ad_time_details table.
INSERT INTO "ad_time_details" (
    "date", 
    "ad_slot_hour", 
    "gg",
    "mm",
    "length_mod",
    "daypart_id",
    "unified_length_id",
    "ad_code"
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
INSERT INTO "ad_time_details" (
    "date", 
    "ad_description", 
    "ad_code",
    "brand_id",
    "medium_id",
    "ad_time_details_id",
    "product_type_id",
    "cost",
    "num_of_emissions",
    "type"
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




