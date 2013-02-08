declare
  v_owner varchar2(100) := upper([SCHEMA_GOES_HERE]);
  v_table_name varchar2(100) := upper([TABLE_GOES_HERE]);
  
  type col_cursor is ref cursor;
  v_col_cursor col_cursor;
  v_col SYS.ALL_TAB_COLS%ROWTYPE;
  
 
  type statements is varray(100) of clob;
  v_statments statements := statements();
  
  v_tmp clob;
  v_stmt clob;
  
begin

  DBMS_LOB.CREATETEMPORARY(v_stmt, TRUE);
  DBMS_LOB.APPEND(v_stmt,'
begin
if  has_object('''||v_owner||''','''||v_table_name||''') = 0 then
execute immediate ''create table '||v_owner||'.'||v_table_name||' (ROWDATE DATE)'';
end if;
if  has_object('''||v_owner||''','''||v_table_name||''') = 1 then
');
  open v_col_cursor for select * from all_tab_cols where upper(owner) = v_owner and upper(table_name) = v_table_name and table_name != 'ROWDATE' order by column_id;
   
  loop fetch v_col_cursor into v_col;
  exit when v_col_cursor%NOTFOUND;
  
-- dbms_output.put_line('Owner: '||v_col.owner||' Table: '||v_col.table_name||' Column: '||v_col.column_name);
-- dbms_output.put_line( v_col.data_length||','||v_col.data_precision||','||v_col.data_scale);
    DBMS_LOB.CREATETEMPORARY(v_tmp, TRUE);
    DBMS_LOB.APPEND(v_tmp,'
begin
-- Add Columns if they exist
if  objexists('''||v_col.owner||''','''||v_col.table_name||''','''||v_col.column_name||''') = 0 then
execute immediate '''||'alter table '||v_col.owner||'.'||v_col.table_name||' add ('||v_col.column_name||' '||v_col.data_type||
      case
        when v_col.data_type = 'NUMBER' then
          '('||
          case
            when v_col.data_precision is null then v_col.data_length||''
            when (v_col.data_precision > 0 and v_col.data_scale = 0) then (v_col.data_length||','||v_col.data_precision)||''
            else (v_col.data_length||','||v_col.data_precision||','||v_col.data_scale)||''
          end
          ||')'
        when v_col.data_type = 'DATE' then
          ''
        when v_col.data_type = 'VARCHAR2' or v_col.data_type = 'CHAR' then
          '('||v_col.data_length||')'
       end
      ||
      case v_col.nullable
        when 'Y' then ''
        when 'N' then ' not null'
      end
      ||
      case when v_col.data_default is not null then
        case
          when v_col.data_type = 'NUMBER' or v_col.data_Type = 'DATE' then ' default '||replace(trim(v_col.data_default),chr(10),'')
          when (v_col.data_type = 'CHAR' or v_col.data_type = 'VARCHAR2') then ' default '''''||replace(replace(trim(v_col.data_default),chr(10),''),'''','')||''''''
          
        end
      end
      ||')'';
end if;
--Add the Constraints if the columns exist
if  objexists('''||v_col.owner||''','''||v_col.table_name||''','''||v_col.column_name||''') = 1 then
null; -- Call the constraint method
end if;
end;
');
    DBMS_LOB.APPEND(v_stmt,v_tmp);
  end loop;
  DBMS_LOB.APPEND(v_stmt,'
end if;
end; ');
  dbms_output.put_line(v_stmt);
end;
/