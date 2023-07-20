SELECT
    fw.id,
    fw.rating,
    array_agg(DISTINCT g.name) AS genres,
    fw.title,
    fw.description,
    fw.modified,
    -- fw.type,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'person_role', pfw.role,
            'person_id', p.id,
            'person_name', p.full_name
        )) FILTER (WHERE p.id IS NOT NULL),
        '[]'
    ) AS persons
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > {0}
GROUP BY fw.id
ORDER BY fw.modified
LIMIT {1};
