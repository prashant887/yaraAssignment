
truncate table  yara.user_location_stage;


COPY yara.user_location_stage FROM '/var/lib/postgresql/data/source_data/user_location.csv' DELIMITER ',' CSV HEADER;

truncate table  yara.world_cities_stage;



COPY yara.world_cities_stage FROM '/var/lib/postgresql/data/source_data/world_cities.csv' DELIMITER ',' CSV HEADER;


