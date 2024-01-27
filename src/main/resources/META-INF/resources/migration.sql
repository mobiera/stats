alter table stat add column statclass character varying (32);
alter table stat alter column stat_class drop not null;

update stat set statclass='SMPP_ACCOUNT' where stat_class='0';
update stat set statclass='CAMPAIGN' where stat_class='1';
update stat set statclass='CAMPAIGN_PARAMS' where stat_class='2';
update stat set statclass='ENDPOINT' where stat_class='3';
update stat set statclass='ACTION_TRIGGER' where stat_class='4';
update stat set statclass='CAMPAIGN_SCHEDULE' where stat_class='5';
update stat set statclass='STK_ACTION' where stat_class='6';
update stat set statclass='STK_ACTION_DATA' where stat_class='7';
update stat set statclass='STK_ACTION_ITEM' where stat_class='8';


alter table stat alter column statclass set not null;

create index statentity_idx on stat (entity_id, statclass, stat_granularity, ts);
create index statglobal_idx on stat (statclass, stat_granularity, ts);
