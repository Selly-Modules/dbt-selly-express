{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_unibag",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('se_orders_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('to'),
        '_id',
        'cod',
        'code',
        adapter.quote('from'),
        'note',
        adapter.quote('value'),
        'client',
        'status',
        'volume',
        'weight',
        'courier',
        'distance',
        'createdat',
        'updatedat',
        'updatedby',
        'itemvolume',
        'searchstring',
        array_to_string('extraservices'),
    ]) }} as _airbyte_se_orders_hashid,
    tmp.*
from {{ ref('se_orders_ab2') }} tmp
-- se_orders
where 1 = 1

