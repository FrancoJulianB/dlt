select
  user_id,
  json_field,
  UNNEST(json_field.array) AS array_item,
  json_field.object.key AS object_key,
  json_field.object.value AS object_value
from {{ ref('your_source_table') }}
