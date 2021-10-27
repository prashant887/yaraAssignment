with cities(city_point,city)  as (
  select point(lng::float,lat::float),city from yara.world_cities_stage 
WHERE lat ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$' and
lng ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$'), usr_loc(user_point,user_id) as
(select point(longitude::float8,latitude::float8) ,user_id
FROM yara.user_location_stage WHERE latitude ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$' and
longitude ~ '^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$'
),dist(city_point,user_point,city,user_id,distance) as (
select a.city_point,b.user_point,a.city,b.user_id,b.user_point <@> a.city_point from cities a,usr_loc b),
final_tbl  (city_point,user_point,city,user_id,distance,rnk) as (
select *,row_number() over(partition by user_id order by distance ) rnk from dist)
select user_id,city,distance,rnk from final_tbl where rnk=1 order by user_id;
