-- Обновление полей таблицы связанными данными из другой таблицы 
UPDATE control_operation
SET
    operation_object_id = operation_object_set.operation_object_id
FROM operation_object_set
WHERE
    control_operation.operation_object_id IS NULL
    AND control_operation.operation_object_set_id = operation_object_set.id;


-- Обновление полей с преедварительной проверкой наличия данных
DO $$
BEGIN
    -- Проверяем наличие данных в таблицах
    IF EXISTS (SELECT 1 FROM control_operation) AND EXISTS (SELECT 1 FROM operation_object_set) THEN
        -- Выполняем обновление, если данные присутствуют
        UPDATE control_operation AS co
        SET
            operation_object_id = os.operation_object_id
        FROM operation_object_set AS os
        WHERE
            co.operation_object_id IS NULL
            AND co.operation_object_set_id = os.id;
    ELSE
        RAISE NOTICE 'Обновление не выполнено: в одной из таблиц отсутствуют данные.';
    END IF;
END $$;