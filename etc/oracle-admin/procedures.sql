/*
Some procedures that dario found on production DB, which may be useful in the future
*/

CREATE OR REPLACE PROCEDURE CMS_ANALYSIS_REQMGR.BUILD_LOCAL_INDEX(CURRTAB VARCHAR2) AS
BEGIN

declare
dt date;
sql_query VARCHAR2(32767);
partition_key VARCHAR2(30);
index_name VARCHAR2(30);
begin
    dbms_output.put_line('===========================================================================');
    select (select column_name from USER_PART_KEY_COLUMNS where name = CURRTAB)
	   into partition_key from dual;
    index_name := partition_key||'_LOCAL_IDX';
    sql_query := 'create index '||index_name||' on ' ||CURRTAB||' ( '||partition_key||' ) LOCAL  ( ';

    for x in (select partition_name from user_tab_partitions where table_name = CURRTAB order by partition_name)
    loop
    	sql_query := sql_query ||' PARTITION '||x.partition_name||',';
    end loop;

    sql_query := substr(sql_query, 0, length(sql_query)-1) || ' )';
    dbms_output.put_line('partition_key: '||partition_key);
    dbms_output.put_line('index_name: '||index_name);
    dbms_output.put_line('sql_query: '||sql_query);
    execute immediate sql_query;
    dbms_output.put_line('===========================================================================');
end;

END BUILD_LOCAL_INDEX;
/