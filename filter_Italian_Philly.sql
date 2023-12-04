select b.business_id, b.name, r.date, r.stars, b.is_open, b.review_count as rc
from business b  
left join review r on b.business_id = r.business_id 
where categories like '%Italian%' and categories like '%Restaurant%' 
and ST_Distance_Sphere(  
POINT(-75.1652, 39.9526), POINT(longitude,latitude) 
) <= 4828 -- 3 mile in meters
order by rc desc