
-- Вывод дубликатов по нескольким полям с дополнительным выводом id
SELECT
    id, size, width,
    inch_size, sch, unit_measure_id
FROM public.dimension_type
WHERE (size, width, inch_size, sch) IN (
        SELECT size, width, inch_size, sch
        FROM public.dimension_type
        GROUP BY
            size, width,
            inch_size, sch
        HAVING
            COUNT(*) > 1
    )
ORDER BY
    size, width,
    inch_size, sch