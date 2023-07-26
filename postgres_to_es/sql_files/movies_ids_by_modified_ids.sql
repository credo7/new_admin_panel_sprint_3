SELECT fw.id
FROM content.film_work fw
LEFT JOIN content.{0}_film_work tfw ON tfw.film_work_id = fw.id
WHERE tfw.{0}_id IN {1}
ORDER BY fw.modified
LIMIT {2};
