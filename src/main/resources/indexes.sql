
create index if not exists ms_stat_entity_idx on stat (entity_fk, stat_class, stat_granularity, ts);

create index if not exists ms_stat_global_idx on stat (stat_class, stat_granularity, ts);

-- lookups by entity_id (Stat.listOne / Stat.listVarious: statclass, entity_id, stat_granularity, ts range)
create index if not exists ms_stat_entity_id_idx on stat (statclass, entity_id, stat_granularity, ts);
