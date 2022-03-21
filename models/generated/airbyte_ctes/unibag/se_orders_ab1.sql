{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_unibag",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('unibag', '_airbyte_raw_se_orders') }}
select
    {{ json_extract_scalar('_airbyte_data', ['to'], ['to']) }} as {{ adapter.quote('to') }},
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['cod'], ['cod']) }} as cod,
    {{ json_extract_scalar('_airbyte_data', ['code'], ['code']) }} as code,
    {{ json_extract_scalar('_airbyte_data', ['from'], ['from']) }} as {{ adapter.quote('from') }},
    {{ json_extract_scalar('_airbyte_data', ['note'], ['note']) }} as note,
    {{ json_extract_scalar('_airbyte_data', ['value'], ['value']) }} as {{ adapter.quote('value') }},
    {{ json_extract_scalar('_airbyte_data', ['client'], ['client']) }} as client,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['volume'], ['volume']) }} as volume,
    {{ json_extract_scalar('_airbyte_data', ['weight'], ['weight']) }} as weight,
    {{ json_extract_scalar('_airbyte_data', ['courier'], ['courier']) }} as courier,
    {{ json_extract_scalar('_airbyte_data', ['distance'], ['distance']) }} as distance,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdat,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedat,
    {{ json_extract_scalar('_airbyte_data', ['updatedBy'], ['updatedBy']) }} as updatedby,
    {{ json_extract_scalar('_airbyte_data', ['itemVolume'], ['itemVolume']) }} as itemvolume,
    {{ json_extract_scalar('_airbyte_data', ['searchString'], ['searchString']) }} as searchstring,
    {{ json_extract_array('_airbyte_data', ['extraServices'], ['extraServices']) }} as extraservices,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('unibag', '_airbyte_raw_se_orders') }} as table_alias
-- se_orders
where 1 = 1

