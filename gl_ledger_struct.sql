--- provide parameter for gl.name

SELECT     fseg.segment_num                  AS segment_number,
fseg.application_column_name,
    fseg.segment_name                 AS segment_name,
    fseg.application_column_name      AS segment_prompt,
    fvs.flex_value_set_id             AS value_set_id,
    fvs.flex_value_set_name           AS value_set_name,
fseg.display_size,
    fvs.description                   AS value_set_description,
    fs.concatenated_segment_delimiter AS segment_separator
FROM
    gl_ledgers gl
INNER JOIN fnd_id_flex_structures fs
    ON fs.application_id = 101
       AND fs.id_flex_code = 'GL#'
       AND fs.id_flex_num = gl.chart_of_accounts_id
INNER JOIN fnd_id_flex_segments fseg
    ON fseg.application_id = fs.application_id
       AND fseg.id_flex_code = fs.id_flex_code
       AND fseg.id_flex_num = fs.id_flex_num
LEFT JOIN fnd_flex_value_sets fvs
    ON fvs.flex_value_set_id = fseg.flex_value_set_id
WHERE
    gl.name = 'place you ledger name here'
ORDER BY
    fseg.segment_num

-------------------------------------------------------------------------
-- same with result in json format

SELECT
    JSON_ARRAYAGG(
        JSON_OBJECT(
'chart_of_accounts_id' value gl.chart_of_accounts_id,
            'SEGMENT_NUMBER' VALUE fseg.segment_num,
            'APPLICATION_COLUMN_NAME' VALUE fseg.application_column_name,
            'SEGMENT_NAME' VALUE fseg.segment_name,
            'SEGMENT_PROMPT' VALUE fseg.application_column_name,
            'VALUE_SET_ID' VALUE fvs.flex_value_set_id,
            'VALUE_SET_NAME' VALUE fvs.flex_value_set_name,
            'DISPLAY_SIZE' VALUE fseg.display_size,
            'VALUE_SET_DESCRIPTION' VALUE fvs.description,
            'SEGMENT_SEPARATOR' VALUE fs.concatenated_segment_delimiter
        )
    ) AS ledgers_list
FROM
    gl_ledgers gl
INNER JOIN fnd_id_flex_structures fs
    ON fs.application_id = 101
    AND fs.id_flex_code = 'GL#'
    AND fs.id_flex_num = gl.chart_of_accounts_id
INNER JOIN fnd_id_flex_segments fseg
    ON fseg.application_id = fs.application_id
    AND fseg.id_flex_code = fs.id_flex_code
    AND fseg.id_flex_num = fs.id_flex_num
LEFT JOIN fnd_flex_value_sets fvs
    ON fvs.flex_value_set_id = fseg.flex_value_set_id
WHERE
    gl.name = 'place you ledger name here'
ORDER BY
    fseg.segment_num

