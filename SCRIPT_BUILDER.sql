create or replace
package            script_builder as
  gv_script clob;
  gv_schema varchar2(50);
  gv_do_print number := 1;
  
  procedure init(iv_schema varchar2);
  procedure build_table(iv_table_name varchar2);
  procedure build_constraints(iv_table_name varchar2);
  function build_constraint(iv_table_name varchar2,
                            iv_column_name varchar2) return clob;
  
  
  procedure print(msg varchar2);
  procedure printClob(msg clob);
  
  
  /*Instalation Functions*/
  procedure print_instalation_functions;
end;
/
create or replace
PACKAGE BODY SCRIPT_BUILDER AS

  procedure init(iv_schema varchar2) as
    cursor cr_tables is 
    select table_name from all_tables where owner = iv_schema;
  begin
    gv_schema := iv_schema;
    print('Schema: '||gv_schema);
    
    dbms_lob.createtemporary(gv_script,true);
    print_instalation_functions;
    
    for tbl in cr_tables loop      
      print('Table: '||tbl.table_name);
      build_table(tbl.table_name);
    end loop;
    for tbl in cr_tables loop
      build_constraints(tbl.table_name);
    end loop;
    print('Finished');
    print('');
    print('');
    print('');    
    printClob(gv_script);
  END init;

  procedure build_table(iv_table_name varchar2) as
    v_owner varchar2(50) := gv_schema;
    v_table_name varchar2(100) := iv_table_name;
    
    type col_cursor is ref cursor;
    v_col_cursor col_cursor;
    v_col SYS.ALL_TAB_COLS%ROWTYPE;
    
   
    type statements is varray(100) of clob;
    v_statments statements := statements();
    
    v_tmp clob;
    v_stmt clob;
    
  begin
    --Create the clob that holds the script build for this table
    DBMS_LOB.CREATETEMPORARY(v_stmt, TRUE);
    dbms_lob.append(v_stmt,'
    --Begin '||v_owner||'.'||v_table_name||'
    begin
      if  has_object('''||v_owner||''','''||v_table_name||''') = 0 then
      execute immediate ''create table '||v_owner||'.'||v_table_name||' (ROWDATE DATE)'';
      end if;
      if  has_object('''||v_owner||''','''||v_table_name||''') = 1 then
    ');
    --Get the columns
    open v_col_cursor for select * from all_tab_cols where upper(owner) = v_owner and upper(table_name) = v_table_name and table_name != 'ROWDATE' order by column_id;
    loop fetch v_col_cursor into v_col;
    exit when v_col_cursor%NOTFOUND;
  
-- dbms_output.put_line('Owner: '||v_col.owner||' Table: '||v_col.table_name||' Column: '||v_col.column_name);
-- dbms_output.put_line( v_col.data_length||','||v_col.data_precision||','||v_col.data_scale);
    DBMS_LOB.CREATETEMPORARY(v_tmp, TRUE);
    DBMS_LOB.APPEND(v_tmp,'
      begin
        -- Add Column if it doesn''t exist
        if  objexists('''||v_col.owner||''','''||v_col.table_name||''','''||v_col.column_name||''') = 0 then
          execute immediate '''||'alter table '||v_col.owner||'.'||v_col.table_name||' add ('||v_col.column_name||' '||v_col.data_type||
                case
                  when v_col.data_type = 'NUMBER' then
                    '('||
                    case
                      when v_col.data_precision is null then v_col.data_length||''
                      when (v_col.data_precision > 0 and v_col.data_scale = 0) then (v_col.data_length||','||v_col.data_precision)||''
                      else (v_col.data_length||','||v_col.data_precision||'')||''--||','||v_col.data_scale)||''
                    end
                    ||')'
                  when v_col.data_type = 'DATE' then
                    ''
                  when v_col.data_type = 'VARCHAR2' or v_col.data_type = 'CHAR' then
                    '('||v_col.data_length||')'
                 end
                 ||
                case when v_col.data_default is not null then
                  case
                    when v_col.data_type = 'NUMBER' or v_col.data_Type = 'DATE' then ' default '||replace(trim(v_col.data_default),chr(10),'')
                    when (v_col.data_type = 'CHAR' or v_col.data_type = 'VARCHAR2') then ' default '''''||replace(replace(trim(v_col.data_default),chr(10),''),'''','')||''''''
                    
                  end
                end
                ||
                case v_col.nullable
                  when 'Y' then ''
                  when 'N' then ' not null'
                end                
                ||')'';
        end if;
      end;
      ');
              --Add the Constraints if the columns exist
        /*if  objexists('''||v_col.owner||''','''||v_col.table_name||''','''||v_col.column_name||''') = 1 then 
          ');
        dbms_lob.append(v_tmp, build_constraints(v_col.table_name,v_col.column_name));
        dbms_lob.append(v_tmp,'
        
        end if;*/
      DBMS_LOB.APPEND(v_stmt,v_tmp);
      end loop;
      DBMS_LOB.APPEND(v_stmt,'
    end if;
    end; ');       
    --Final bit add it to the global schema
    dbms_lob.append(v_stmt,'
    --End '||v_owner||'.'||v_table_name||'
    /    
    ');
    DBMS_LOB.append(gv_script,v_stmt);
  END build_table;

  procedure build_constraints(iv_table_name varchar2) as
  cursor v_columns is select table_name,column_name from all_tab_cols where owner = gv_schema and table_name = iv_table_name;
  v_clob clob;
  begin
    DBMS_LOB.CREATETEMPORARY(v_clob,true);
    dbms_lob.append(v_clob,'    
  --Begin '||gv_schema||'.'||iv_table_name||'
    begin
    ');
    for col in v_columns loop      
      DBMS_LOB.append(v_clob,build_constraint(col.table_name,col.column_name));
    end loop;
    dbms_lob.append(v_clob,'
    end;
  --End '||gv_schema||'.'||iv_table_name||'
    ');    
    DBMS_LOB.append(gv_script,'
      /
    ');
    DBMS_LOB.append(gv_script,v_clob);    
  end;


  function build_constraint(iv_table_name varchar2,
                            iv_column_name varchar2) return clob 
  as
    v_owner varchar2(100) := gv_schema;
    v_table_name varchar2(100) := iv_table_name;
    v_column_name varchar2(100) := iv_column_name;
    
    type fk_columns_varary is varray(1) of varchar2(100);
    fk_columns fk_columns_varary := fk_columns_varary();
    
    type rslt_cursor is ref cursor;
    v_constrants rslt_cursor;
    v_const_cols rslt_cursor;
    v_r_const_cols rslt_cursor;
    
    v_cons_row sys.all_constraints%ROWTYPE;
    v_cons_col_row sys.all_cons_columns%ROWTYPE;
    v_r_cons_col_row sys.all_cons_columns%ROWTYPE;
    
    v_text clob;
    
    v_fk_columns_txt clob;
    v_r_fk_columns_txt clob;
    v_r_table varchar2(100);
  begin
    DBMS_LOB.CREATETEMPORARY(v_text,true);
    --Get the constraints
    open v_constrants for select * from sys.all_constraints where upper(owner) = v_owner and upper(table_name) = v_table_name ;
    loop fetch v_constrants into v_cons_row;
    exit when v_constrants%notfound;
      
      -- Get the columns for the constraint
      open v_const_cols for select * from sys.all_cons_columns where constraint_name = v_cons_row.constraint_name and upper(owner) = v_owner and upper(table_name) = v_table_name;
      DBMS_LOB.CREATETEMPORARY(v_fk_columns_txt,true);
      
      loop fetch v_const_cols into v_cons_col_row;
      exit when v_const_cols%NOTFOUND;
      
      DBMS_LOB.APPEND(v_text, '
      if '||
          case v_cons_row.GENERATED
            when 'USER NAME' then 'has_constraint_by_name('''||v_owner||''','''||v_table_name||''','''||v_cons_row.constraint_name||''') = 0'
            else 'has_constraint_by_type('''||v_owner||''','''||v_table_name||''','''||v_cons_col_row.column_name||''','''||v_cons_row.constraint_type||''') = 0'
          end
      ||' then
        execute immediate
        ''alter table '||v_owner||'.'||v_table_name||' add ( constraint '||
            case v_cons_row.GENERATED
              when 'USER NAME' then v_cons_row.constraint_name||' '
              else ''
            end);
          
        case v_cons_row.constraint_type
          when 'C' then -- Check Constraint
            case when instr(v_cons_row.SEARCH_CONDITION,'NOT NULL') = 0 then
              dbms_lob.append(v_text,'check ('||v_cons_row.search_condition||')');
            else
              v_text := 'alter table '||v_owner||'.'||v_table_name||' alter column ('|| v_cons_col_row.column_name||' not null';
            end case;            
          when 'R' then -- Referential Constraint (FK)
            if length(v_fk_columns_txt) = 0 then
              --Get the referenced columns
              open v_r_const_cols for select * from all_cons_columns where constraint_name = v_cons_row.R_CONSTRAINT_NAME;
              DBMS_LOB.CREATETEMPORARY(v_r_fk_columns_txt,true);
              
              loop fetch v_r_const_cols into v_r_cons_col_row;
              exit when v_r_const_cols%NOTFOUND;
                v_r_table := v_r_cons_col_row.TABLE_NAME;
                DBMS_LOB.append(v_r_fk_columns_txt,v_r_cons_col_row.COLUMN_NAME||',');
              end loop;
              
              DBMS_LOB.APPEND(v_text,'foreign key ([fk_columns]) references '||v_r_cons_col_row.owner||'.'||v_r_table||'('||DBMS_LOB.substr(v_r_fk_columns_txt,length(v_r_fk_columns_txt)-1,1)||')');
            end if;
            DBMS_LOB.APPEND(v_fk_columns_txt,v_cons_col_row.column_name||',');
            null;
          when 'P' then -- Primary Key
            DBMS_LOB.append(v_text,'primary key ('||v_cons_col_row.column_name||')');
            null;
          when 'O' then -- Read Only on a view
            null;
          when 'U' then -- Unique Key
           if length(v_fk_columns_txt) = 0 then
            DBMS_LOB.APPEND(v_text,'unique ([fk_columns])');
          end if;
          DBMS_LOB.APPEND(v_fk_columns_txt,v_cons_col_row.column_name||',');
            null;
          when 'V' then -- Check option on a View
            null;
        end case;
        DBMS_LOB.APPEND(v_text,')'';
      end if;');
      end loop;
      select replace(v_text,'[fk_columns]',dbms_lob.substr(v_fk_columns_txt,length(v_fk_columns_txt)-1,1)) into v_text from dual;
    end loop;
    
    if length(v_text) < 1 then
      dbms_lob.append(v_text,'
        null;');
    end if;
    return v_text;
  end build_constraint;
                            

  procedure print(msg varchar2) as
  begin
    if gv_do_print = 1 then
      dbms_output.put_line(msg);
    end if;
  end;

  procedure printClob(msg clob)as
  begin
    if gv_do_print = 1 then
      DBMS_XSLPROCESSOR.CLOB2FILE(msg, upper('CHI_INT_DIR'), 'CLOB_EXPORT'||'.sql');
    end if;
  end;


  procedure print_instalation_functions as
    v_functions varchar2(4500) := '
      create or replace
      procedure         rename_constraint(
        in_owner in varchar2,
        in_table in varchar2,
        in_column in varchar2,
        in_constraint_type in varchar2,
        in_constraint_name in varchar2  
      )
      as
        v_old_constraint_name varchar2(200);
        constraintChar varchar2(5000) := in_constraint_type;
      begin
          select
            case when upper(in_constraint_type) in (''PRIMARY'',''PRIMARY_KEY'') then
              ''P''
            when upper(in_constraint_type) in (''FOREIGN'',''FOREIGN KEY'') then
              ''R''
            when upper(in_constraint_type) in (''UNIQUE'') then
              ''U''
            when upper(in_constraint_type) in (''CHECK'') then
              ''C'' 
            else
              in_constraint_type
            end into constraintChar
          from dual;
      
          select ac.constraint_name into v_old_constraint_name
          from
          all_constraints ac 
          left join 
          all_cons_columns acc on ac.owner = acc.owner and ac.constraint_name = acc.constraint_name 
          where upper(ac.owner) = upper(in_owner) and upper(ac.table_name) = upper(in_table) and upper(acc.column_name) = upper(in_column) and upper(ac.constraint_type) = upper(constraintChar);
          
          execute immediate
          ''alter table ''||upper(in_owner)||''.''||upper(in_table)||'' rename constraint ''||upper(v_old_constraint_name)||'' to ''||upper(in_constraint_name);
      end;
      /
      create or replace
      function has_constraint_by_name(ownerName in varchar2, tableName in varchar2, constraintName in varchar2) return integer
        as
          returnRslt integer := 0;
        begin
          select count(1) into returnRslt from all_constraints where owner = ownerName and table_name = upper(tableName) and constraint_name = upper(constraintName);
          return returnRslt;
      end;
      /
      create or replace
        function has_constraint_by_type(ownerName in varchar2, tableName in varchar2,columnName in varchar2, constraintType in varchar2) return integer   
        as        
          returnRslt integer := 0;
          constraintChar varchar2(5000);
        begin
          select
            case when upper(constraintType) in (''PRIMARY'',''PRIMARY_KEY'') then
              ''P''
            when upper(constraintType) in (''FOREIGN'',''FOREIGN KEY'') then
              ''R''
            when upper(constraintType) in (''UNIQUE'') then
              ''U''
            when upper(constraintType) in (''CHECK'') then
              ''C''
            else
              constraintType  
            end into constraintChar
          from dual;  
          select count(1) into returnRslt   
          from
            all_constraints ac 
            left join 
            all_cons_columns acc on ac.owner = acc.owner and ac.constraint_name = acc.constraint_name 
            where upper(ac.owner) = upper(ownerName) and 
            upper(ac.table_name) = upper(tableName) and 
            upper(acc.column_name) = upper(columnName) and 
            upper(ac.constraint_type) = upper(constraintChar);          
          return returnRslt;
      end;
      /
      create or replace
      function has_object(
          ownerName in varchar2,
          objectName in varchar2) return integer
          as
            returnRslt integer := 0;
          begin
            select count(1) into returnRslt from all_objects where upper(owner) = upper(ownerName) and upper(object_name) = upper(objectName);
            return returnRslt;
      end;
      /
      create or replace
      function has_user(
        user_name in varchar2
      ) return integer
      as
        returnVal integer := 0;
      begin
        select count(1) into returnVal from all_users where upper(username) = upper(user_name);
        return returnVal;
      end;
      /
      create or replace
      function OBJEXISTS(ownerName in varchar2, tableName in varchar2, columnName in varchar2) return number AUTHID CURRENT_USER
      is
        v_count number;
      begin
        select count(*) into v_count from all_tab_cols where owner = upper(ownerName) and table_name = upper(tableName) and column_name = upper(columnName);
        
        return v_count;
      end;
      /
    ';      
  begin
    DBMS_LOB.APPEND(GV_SCRIPT,V_FUNCTIONS);
  end;

END SCRIPT_BUILDER;