declare
  v_owner varchar2(100) := [SCHEMA_NAME];
  v_table_name varchar2(100) := [TABLE_NAME];
  v_column_name varchar2(100) := '';
  
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
  exit when v_constrants%NOTFOUND;
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
          DBMS_LOB.APPEND(v_text,'check ('||v_cons_row.SEARCH_CONDITION||')');
          null;
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
            
            DBMS_LOB.APPEND(v_text,'foreign key ([fk_columns]) references '||v_r_table||'('||DBMS_LOB.substr(v_r_fk_columns_txt,length(v_r_fk_columns_txt)-1,1)||')');
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
    select replace(v_text,'[fk_columns]',DBMS_LOB.substr(v_fk_columns_txt,length(v_fk_columns_txt)-1,1)) into v_text from dual;
  end loop;
  DBMS_OUTPUT.PUT_LINE(v_text);
end;
/