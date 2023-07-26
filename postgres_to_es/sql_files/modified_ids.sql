SELECT id, modified
FROM content.{0}
WHERE modified > {1}
ORDER BY modified
LIMIT {2};
