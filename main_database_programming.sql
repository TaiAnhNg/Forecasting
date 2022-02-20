create or replace package body PKG_2020Main as
-- Author: Tai Anh Nguyen
-- Last Modified: 05/06/2020
-- Description: This is body part of the package, every methods and explaination are here.
-- ALL methods will support for procedure RM16_forecast.

-- Global Variables
g_package_name VARCHAR2(25):= 'PKG_2020Main';
v_procedure_name  VARCHAR2(35);

-- The entry module used to run assignment:

/* Description: This function will set the appropriate value 
   corresponding to the P_CATEGORY and P_CODE in dbp_parameter we pass in. */
function get_parameter(p_category varchar2, p_code varchar2)
                       RETURN varchar2 IS 
--                       
v_value varchar2(35);
--
BEGIN
    select value INTO v_value 
    from dbp_parameter
    where category = p_category AND code = p_code;
    return v_value;
--    
EXCEPTION
        WHEN NO_DATA_FOUND THEN
            return null;
            common.log('No Data Found, Fail to get parameter');
        WHEN OTHERS THEN
            return null;
            common.log('Fail to get parameter. Error at ' || SQLCODE || ':' || SQLERRM);
END;

/* Description: this function will set the value 
   based on the parameter that is corresponding to HOLIDAY_DATE in DBP_HOLIDAY table then decide to return true or False. */
-- variable v_is_holiday will retrieve value 1 and return True if forecast day is a holiday.
function is_holiday(p_forecast_date DATE) 
                    RETURN BOOLEAN IS 
--                    
v_is_holiday VARCHAR2(1);
--
BEGIN
select '1' INTO v_is_holiday
from dbp_holiday 
where holiday_date = p_forecast_date;

RETURN TRUE;
--
EXCEPTION
        WHEN NO_DATA_FOUND THEN
        RETURN FALSE;
END;

/* Description: this function will set the value 
   based on the day in v_nem_rm16 that is corresponding to HOLIDAY_DATE in DBP_HOLIDAY and parameter. */
-- variable v_past_holiday will retrieve value 1 and return True if forecast day is a holiday and no historical data exists.
function past_holiday_exist(p_forecast_date DATE) 
                            RETURN boolean IS
--                            
v_past_holiday VARCHAR2(1);
--
BEGIN
select '1' into v_past_holiday
from v_nem_rm16
where day in (select * from dbp_holiday)
and day < trunc(p_forecast_date);

return true;
--
EXCEPTION
        WHEN NO_DATA_FOUND THEN
        RETURN FALSE;
END;

-- Description: this procedure will write forecasted data into LOCAL_RM16 table.
procedure update_local_rm16(p_day_num varchar2, p_date date) IS
--
BEGIN
insert into local_rm16(statement_type, tni, lr, frmp, change_date, day, hh, volume)
      (select 'FORECAST', tni, lr, frmp, sysdate, trunc(p_date), hh, average_vol
      from v_nem_average n
      where p_day_num = n.day_num);
--      
      EXCEPTION
      WHEN OTHERS THEN
      common.log('Fail to insert forecasted data. Error at ' || SQLCODE || ':' || SQLERRM);
END;

-- Description: this procedure will update new values into the current record of program session
procedure update_run_table(p_outcome varchar2, p_remarks varchar2) IS
--
v_run_id number;
--
BEGIN
select MAX(run_id) into v_run_id
from run_table;
update run_table
set run_end = sysdate,
    outcome = p_outcome,
    remarks = p_remarks
    where run_id = v_run_id;
    commit;
--    
    EXCEPTION
    WHEN OTHERS THEN
    common.log('Fail to Finish Program. Error at ' || SQLCODE || ':' || SQLERRM);
END;

-- Description: this procedure will create a new record whenever start a new program session.
procedure start_run_table IS
--
v_run_id number;
c_status varchar2(25) := get_parameter('RUN_TABLE', 'START_STATUS');
--
BEGIN
  select seq_run_id.NEXTVAL into v_run_id from dual;
  insert into run_table(run_id, run_start, outcome, remarks)
  values(v_run_id, sysdate, c_status, 'Running');
  commit;
-- 
  EXCEPTION
  WHEN OTHERS THEN
  update_run_table(get_parameter('RUN_TABLE', 'ERROR'), 'Fail');
  common.log('Fail to Run Program. Error at ' || SQLCODE || ':' || SQLERRM);
END;

/* Description: this procedure will predict the average electricity volume at every half hours of each forecast day in the future.
   LOCAL_RM16 table will be truncated whenever start a new program session to avoid duplicate data and update the newest predicted value.
   Loop 14 days in the future to see whether it is holiday or not. */
-- Note*: Holiday and Sunday can be regconised by day_num = '9' and '7' respectively.
-- it is a normal day then using average consumption of every same day in the past.
-- it is a holiday day and having historical data of that day, then using the consumption of that same holiday in the past. by using day_num = '9'.
-- it is a holiday day and no historical data of that day, then using the average consumption of every sundays in the past. by using day_num = '7'.
procedure generate_forecast is
--
    v_forecast_date  DATE;

--
BEGIN
      delete from local_rm16
      where day >= trunc(sysdate);
      commit;
      
      v_procedure_name  := 'generate_forecast';
      COMMON.log('In procedure '||v_procedure_name);

for counter in 1..14
loop
 v_forecast_date := trunc(sysdate) + counter;
 COMMON.log('The day is '||v_forecast_date);  
 if not is_holiday(v_forecast_date) THEN
 update_local_rm16(to_char(v_forecast_date, 'D'), v_forecast_date);
 elsif is_holiday(v_forecast_date) and past_holiday_exist(v_forecast_date) THEN
 common.log('The day is '||v_forecast_date||' and the day is a holiday');
 update_local_rm16('9', v_forecast_date);
 else
 common.log('The day is '||v_forecast_date||' and no past holiday');
 update_local_rm16('7', v_forecast_date);
 end if;
 --
end loop;

EXCEPTION
      WHEN OTHERS THEN
      common.log('Fail to forecast data. Error at ' || SQLCODE || ':' || SQLERRM);
END;    

/* Description: This procedure will xml file into ORALAB server through a directory in rerun.
   XML file will contain a list of TNI with each total electricity consumption. */
procedure update_xml IS
--
v_file utl_file.file_type;
v_my_dir varchar2(30) := get_parameter('Directory', 'My_Dir');
v_date varchar2(30) := to_char(sysdate + 1, 'DD-MON-YYYY HH24:MI:SS');
v_file_name varchar2(30) := USER || '.xml';
Ctx DBMS_XMLGEN.ctxHandle;
xml Clob := Null;
temp_xml Clob := Null;
Query varchar2(2000) := 'select tni, sum(volume) total_consumption
                         from local_rm16
                         where day = '''||v_date||'''
                         group by tni';
--                         
BEGIN
v_file := utl_file.fopen(v_my_dir, v_file_name, 'W');
Ctx := DBMS_XMLGEN.newContext(QUERY);
   DBMS_XMLGEN.setRowsetTag( Ctx, 'ROWSETTAG');
   DBMS_XMLGEN.setRowTag( Ctx, 'RowTag');
   temp_xml := DBMS_XMLGEN.getXML(Ctx);
   IF temp_xml IS NOT NULL THEN
      IF xml IS NOT NULL THEN
         DBMS_LOB.APPEND( xml, temp_xml );
      ELSE
         xml := temp_xml;
      END IF;
   END IF;

   DBMS_XMLGEN.closeContext (Ctx);
   UTL_FILE.put_line(v_file, 'XML File Print at '||v_date);
   UTL_FILE.put_line(v_file, xml);
   UTL_FILE.fclose(v_file); 
   
   common.log('Create XML successfully');
--   
EXCEPTION
     WHEN OTHERS THEN
          common.log('Fail to create XML file. Error at '|| SQLCODE || ':' || SQLERRM);
END;

-- Description: This is the main procedure. everyone can call this method to run the program because it is public.
procedure RM16_forecast is
--
v_procedure_name  VARCHAR2(35) := 'RM16_forecast';
BEGIN
v_procedure_name := 'RM16_forecast';
COMMON.log('In procedure '||v_procedure_name);

start_run_table;
generate_forecast;
update_xml;
update_run_table(get_parameter('RUN_TABLE', 'END_STATUS'), 'End');
--
COMMON.log('Back In procedure '||v_procedure_name);

END;



end PKG_2020Main;