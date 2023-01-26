CREATE VIEW "powers" (pk VARCHAR PRIMARY KEY, "personal"."hero" VARCHAR, "personal"."power" VARCHAR, "professional"."name" VARCHAR);

SELECT a."professional"."name" as "Name1", b."professional"."name" as "Name2", b."personal"."power" as "Power" FROM "powers" AS a INNER JOIN "powers" AS b ON a."personal"."power" = b."personal"."power" WHERE a."hero" = 'yes' AND b."hero" = 'yes';
