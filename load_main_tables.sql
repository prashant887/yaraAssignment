truncate table yara.user_location;

insert into yara.user_location(address,latitude,longitude,tag,location_id,user_id)
select address,latitude::float8,longitude::float8,tag,location_id::int,user_id::int from yara.user_location_stage WHERE latitude ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$' and
longitude ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$';

truncate table yara.world_cities;

insert into yara.world_cities(city,city_ascii,lat,lng,iso2,iso3,admin_name,capital,population,id)
select city,city_ascii,lat::float8,lng::float8,iso2,iso3,admin_name,capital,population::int,id::int from yara.world_cities_stage 
WHERE lat ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$' and
lng ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$';

