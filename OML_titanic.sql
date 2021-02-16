-- ####################################################
-- # Kaggle: Titanic - Machine Learning from Disaster #
-- # Oracle Machine Learning                          #
-- ####################################################

SET LINES 200
SET PAGES 500
SET TIMING OFF
SET TRIMSPOOL ON
SET DEFINE ON
SET ECHO ON
SET TERMOUT ON
SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;

SPOOL OML_titanic.log

alter session set nls_date_format='DD-MON-YYYY HH24:MI';

-- ###############
-- # Import Data #
-- ###############

-- drop external table
DECLARE
  v_table   VARCHAR2(30) := 'titanic_train_ext';
  sql_stmt  VARCHAR2(200);
BEGIN
  sql_stmt := 'drop table '|| v_table;
  EXECUTE IMMEDIATE sql_stmt;
  dbms_output.put_line('Dropped table: '|| v_table);
EXCEPTION
  WHEN others THEN
    dbms_output.put_line('Table '|| v_table ||' does not exist.');
END;
/

-- create external table
create table titanic_train_ext ( 
  PassengerId	NUMBER,
  Survived	NUMBER,
  Pclass	NUMBER,
  Name		VARCHAR2(128),
  Sex		VARCHAR2(8),
  Age		NUMBER,
  SibSp		NUMBER,
  Parch		NUMBER,
  Ticket	VARCHAR2(20),
  Fare		NUMBER,
  Cabin		VARCHAR2(15),
  Embarked	VARCHAR2(3)
) 
  organization external ( 
	type   ORACLE_LOADER 
	default directory titanic_dir
	access parameters ( 
	   records delimited by NEWLINE 
           skip 1
	   badfile titanic_dir:'titanic_train.bad'
	   nodiscardfile
	   logfile titanic_dir:'titanic_train.log'
	   fields terminated by ',' optionally enclosed by '"'
	   reject rows with all null fields 
	   ( 
             "PASSENGERID",
             "SURVIVED",
             "PCLASS",
             "NAME",
             "SEX",
             "AGE",
             "SIBSP",
             "PARCH",
             "TICKET",
             "FARE",
             "CABIN",
             "EMBARKED"
           ) 
	) 
	location ('train.csv') 
  ) 
reject limit unlimited;

-- confirm data
select count(*) from titanic_train_ext ;

col name format a30
select * from titanic_train_ext where rownum<=5;

-- confirm data types
desc titanic_train_ext

-- create internal table to improve performance
DECLARE
  v_table   VARCHAR2(30) := 'titanic_train';
  sql_stmt  VARCHAR2(200);
BEGIN
  sql_stmt := 'drop table '|| v_table;
  EXECUTE IMMEDIATE sql_stmt;
  dbms_output.put_line('Dropped table: '|| v_table);
EXCEPTION
  WHEN others THEN
    dbms_output.put_line('Table '|| v_table ||' does not exist.');
END;
/

create table titanic_train as
  select * from titanic_train_ext;

-- confirm data
select count(*) from titanic_train;

col name format a30
select * from titanic_train where rownum<=5;

-- confirm data types
desc titanic_train


-- #############################
-- # Exploratory Data Analysis #
-- #############################

-- create tables for storing table stats
create or replace procedure dm_create_stats_tbl_proc
  (p_owner IN VARCHAR2, p_data_table IN VARCHAR2)
AUTHID CURRENT_USER is
  v_max_varchar  NUMBER;
  select_sql  VARCHAR2(500);
  table_sql   VARCHAR2(500);
  drop_sql    VARCHAR2(500);
begin
  BEGIN
    drop_sql := 'drop table '||p_data_table||'_char_stats';
    EXECUTE IMMEDIATE drop_sql;
    dbms_output.put_line('Table '||p_data_table||'_char_stats has been dropped');
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Table '||p_data_table||'_char_stats does not exist.');
  END;

  BEGIN
    drop_sql := 'drop table '||p_data_table||'_num_stats';
    EXECUTE IMMEDIATE drop_sql;
    dbms_output.put_line('Table '||p_data_table||'_num_stats has been dropped');
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Table '||p_data_table||'_num_stats does not exist.');
  END;

  BEGIN
    select_sql := 'select max(DATA_LENGTH) 
                   from all_tab_cols where owner=upper('''||p_owner||''')
                   and table_name=upper('''||p_data_table||''')
                   and DATA_TYPE=''VARCHAR2''';
    dbms_output.put_line(select_sql);
    execute immediate select_sql into v_max_varchar;
    dbms_output.put_line('Max data length is '||v_max_varchar);

    -- create numeric table
    table_sql := 'CREATE TABLE '||p_data_table||'_num_stats (
      COLUMN_NAME VARCHAR2(128),
      "COUNT" NUMBER,
      mean    NUMBER,
      std     NUMBER,
      "MIN"   NUMBER,
      "25%"   NUMBER,
      "50%"   NUMBER,
      "75%"   NUMBER,
      "MAX"   NUMBER)';
    dbms_output.put_line(table_sql);
    execute immediate table_sql;

    -- create text table
    table_sql := 'CREATE TABLE '||p_data_table||'_char_stats (
      COLUMN_NAME VARCHAR2(128),
      "COUNT"  NUMBER,
      "UNIQUE" NUMBER,
      top    VARCHAR2('||v_max_varchar||'),
      freq   NUMBER)';
    dbms_output.put_line(table_sql);
    execute immediate table_sql;
  END;
end;
/

exec dm_create_stats_tbl_proc('DMUSER','titanic_train');
desc titanic_train_char_stats;
desc titanic_train_num_stats;

-- populate statistics tables
create or replace procedure dm_populate_stats_tbl_proc
  (p_owner    IN VARCHAR2, 
   p_data_tbl IN VARCHAR2, 
   p_num_tbl  IN VARCHAR2,
   p_char_tbl IN VARCHAR2)
AUTHID CURRENT_USER is
  table_sql   VARCHAR2(500);
  insert_sql  VARCHAR2(500);
  select_sql  VARCHAR2(500);
  trunc_sql   VARCHAR2(500);
  v_count     NUMBER;
  v_unique    NUMBER;
  v_top       VARCHAR2(128);
  v_freq      VARCHAR2(128);
  s           DBMS_STAT_FUNCS.SummaryType;
begin
  BEGIN
    trunc_sql := 'truncate table '||p_num_tbl;
    execute immediate trunc_sql;
    dbms_output.put_line('Truncated: '||p_num_tbl);
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Table '||p_num_tbl||' does not exist.');
  END;

  BEGIN
    trunc_sql := 'truncate table '||p_char_tbl;
    execute immediate trunc_sql;
    dbms_output.put_line('Truncated: '||p_char_tbl);
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Table '||p_char_tbl||' does not exist.');
  END;

    For tab_cols_c
      IN (select COLUMN_NAME, DATA_TYPE
          from all_tab_cols 
          where owner=upper(p_owner)
          and table_name=upper(p_data_tbl))
    LOOP
      if (tab_cols_c.data_type = 'VARCHAR2') then
        execute immediate 'select count('||tab_cols_c.column_name||') from '||p_data_tbl INTO v_count;
        execute immediate 'select count(distinct '||tab_cols_c.column_name||') from '||p_data_tbl INTO v_unique;
        execute immediate 'select stats_mode('||tab_cols_c.column_name||') from '||p_data_tbl INTO v_top;

        select_sql := 'select count(*) from '||p_data_tbl||' where '||tab_cols_c.column_name||'='''||v_top||'''';
        execute immediate select_sql INTO v_freq;

        insert_sql := 'insert into '||p_char_tbl||' (COLUMN_NAME, "COUNT", "UNIQUE", TOP, FREQ)
                       values ('''||tab_cols_c.column_name||''','||v_count||','||v_unique||','''||v_top||''','||v_freq||')';
        execute immediate insert_sql;
        commit;

      else
        DBMS_STAT_FUNCS.SUMMARY(p_owner,p_data_tbl,tab_cols_c.column_name,3,s);

        insert_sql := 'insert into '||p_num_tbl||' (COLUMN_NAME, "COUNT", mean, std, "MIN", "25%", "50%", "75%", "MAX")
                       values ('''||tab_cols_c.column_name||''','||s.count||','||s.mean||','||s.stddev||','||s.min||','||s.quantile_25||','||s.median||','||s.quantile_75||','||s.max||')';
        execute immediate insert_sql;
        commit;

      end if;
    end loop;
end;
/

exec dm_populate_stats_tbl_proc(-
  p_owner    => 'DMUSER',-
  p_data_tbl => 'titanic_train',-
  p_num_tbl  => 'titanic_train_num_stats',-
  p_char_tbl => 'titanic_train_char_stats');

col column_name format a15
col count format 999,990
select COLUMN_NAME, 
       "COUNT", 
       round(MEAN,3) MEAN, 
       round(STD,3) STD,
       round("MIN",3) "MIN",
       round("25%",3) "25%",
       round("50%",3) "50%",
       round("75%",3) "75%",
       round("MAX",3) "MAX"
from titanic_train_num_stats;

col column_name format a15
col top format a40
select * from titanic_train_char_stats;

-- check for missing data
create or replace procedure dm_create_null_tbl_proc
  (p_owner IN VARCHAR2, p_data_tbl IN VARCHAR2)
AUTHID CURRENT_USER is
  v_null_tbl VARCHAR2(128);
  insert_sql  VARCHAR2(500);
  select_sql  VARCHAR2(500);
  drop_sql    VARCHAR2(500);
  create_sql  VARCHAR2(500);
  v_null_ct   NUMBER;
  v_total_ct  NUMBER;
  v_pct_null  NUMBER;
begin
  v_null_tbl := p_data_tbl||'_null_ct';

  BEGIN
    drop_sql := 'drop table '||v_null_tbl;
    execute immediate drop_sql;
    dbms_output.put_line('Dropped: '||v_null_tbl);
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Table '||v_null_tbl||' does not exist.');
  END;

  create_sql := 'CREATE TABLE '||v_null_tbl|| '(
                   COLUMN_NAME  VARCHAR2(128),
                   NULL_CT      NUMBER,
                   PCT_NULL     NUMBER)';
  execute immediate create_sql;

  select_sql := 'select count(*) from '||p_data_tbl;
  execute immediate select_sql into v_total_ct;

  For tab_cols_c
    IN (select COLUMN_NAME
        from all_tab_cols 
        where owner=upper(p_owner)
        and table_name=upper(p_data_tbl))
  LOOP
    select_sql := 'select count(*) - count('||tab_cols_c.column_name||') from '||p_data_tbl;
    execute immediate select_sql into v_null_ct;

    IF (v_null_ct > 0) THEN
      select round((v_null_ct/v_total_ct)*100,0) into v_pct_null from dual;
      insert_sql := 'insert into '||v_null_tbl||' (COLUMN_NAME, NULL_CT, PCT_NULL)
                     values ('''||tab_cols_c.column_name||''','||v_null_ct||','||v_pct_null||')';
      execute immediate insert_sql;
      commit;
    END IF;
  END LOOP;
end;
/

exec dm_create_null_tbl_proc('DMUSER','TITANIC_TRAIN');
select * from titanic_train_null_ct;

-- Exclude the Cabin column
CREATE OR REPLACE VIEW titanic_train_eda_vw AS
  SELECT PASSENGERID, SURVIVED, PCLASS, NAME, SEX, AGE, SIBSP, PARCH, TICKET, FARE, EMBARKED
  FROM   titanic_train;

desc titanic_train_eda_vw

-- Fill in the missing age values with the mean
CREATE OR REPLACE VIEW titanic_train_eda_vw AS
  SELECT PASSENGERID, SURVIVED, PCLASS, NAME, SEX, 
         NVL(AGE, a.AVG_AGE) AGE,
         SIBSP, PARCH, TICKET, FARE, EMBARKED
  FROM   titanic_train,
         (SELECT avg(age) AVG_AGE
          FROM   titanic_train) a;

select tbl.PASSENGERID, tbl.AGE, vw.AGE
from   titanic_train_eda_vw vw,
       titanic_train tbl
where  vw.PASSENGERID=tbl.PASSENGERID
and    tbl.PASSENGERID in 
       (select PASSENGERID from titanic_train 
        where AGE is null)
and    rownum<6;

-- Replace the missing values of Embarked with the mode
CREATE OR REPLACE VIEW titanic_train_eda_vw AS
  SELECT PASSENGERID, SURVIVED, PCLASS, NAME, SEX, 
         NVL(AGE, a.AVG_AGE) AGE,
         SIBSP, PARCH, TICKET, FARE, 
         NVL(EMBARKED, b.emb_mode) EMBARKED
  FROM   titanic_train,
         (SELECT avg(age) AVG_AGE
          FROM   titanic_train) a,
         (select stats_mode(EMBARKED) emb_mode
          from titanic_train) b;

select tbl.PASSENGERID, tbl.EMBARKED, vw.EMBARKED
from   titanic_train_eda_vw vw,
       titanic_train tbl
where  vw.PASSENGERID=tbl.PASSENGERID
and    tbl.PASSENGERID in 
       (select PASSENGERID from titanic_train 
        where EMBARKED is null);

-- Get a summary of the survival number by passenger class (statistically significant???)
select *
from
  (select survived, pclass, 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by pclass;

-- Create a bar chart of survival by sex
select *
from
  (select survived, sex, 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by sex;

-- Out of curriosity I want to combine these two columns to see the combined correlation
select *
from
  (select survived, to_char(pclass)||','||sex "CLS_SEX", 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by "CLS_SEX";

-- Check survival rates by sibling/spouse numbers
select *
from
  (select survived, sibsp, 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by sibsp;

-- Check survival rates by parent/child numbers
select *
from
  (select survived, parch, 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by parch;

-- Check the survival rates by the port of embarkment
select *
from
  (select survived, embarked, 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by embarked;

-- Bin the age
-- Check the survival by age
CREATE OR REPLACE VIEW titanic_train_eda_vw AS
  SELECT PASSENGERID, SURVIVED, PCLASS, NAME, SEX, 
         case
           when NVL(AGE, a.AVG_AGE) < 10 then '0-9'
           when NVL(AGE, a.AVG_AGE) < 20 then '10-19'
           when NVL(AGE, a.AVG_AGE) < 30 then '20-29'
           when NVL(AGE, a.AVG_AGE) < 40 then '30-39'
           when NVL(AGE, a.AVG_AGE) < 50 then '40-49'
           when NVL(AGE, a.AVG_AGE) < 60 then '50-59'
           when NVL(AGE, a.AVG_AGE) < 70 then '60-69'
           else '70plus' 
         end age_bin,
         SIBSP, PARCH, TICKET, FARE, 
         NVL(EMBARKED, b.emb_mode) EMBARKED
  FROM   titanic_train,
         (SELECT avg(age) AVG_AGE
          FROM   titanic_train) a,
         (select stats_mode(EMBARKED) emb_mode
          from titanic_train) b;

select *
from
  (select survived, age_bin, 1 ct
   from titanic_train_eda_vw)
pivot (sum(ct)
  for survived in (0, 1))
order by age_bin;

-- Drop columns that are not useful
CREATE OR REPLACE VIEW titanic_train_eda_vw AS
  SELECT PASSENGERID, SURVIVED, PCLASS, SEX, 
         case
           when NVL(AGE, a.AVG_AGE) < 10 then '0-9'
           when NVL(AGE, a.AVG_AGE) < 20 then '10-19'
           when NVL(AGE, a.AVG_AGE) < 30 then '20-29'
           when NVL(AGE, a.AVG_AGE) < 40 then '30-39'
           when NVL(AGE, a.AVG_AGE) < 50 then '40-49'
           when NVL(AGE, a.AVG_AGE) < 60 then '50-59'
           when NVL(AGE, a.AVG_AGE) < 70 then '60-69'
           else '70plus' 
         end age_bin,
         SIBSP, PARCH, FARE, 
         NVL(EMBARKED, b.emb_mode) EMBARKED
  FROM   titanic_train,
         (SELECT avg(age) AVG_AGE
          FROM   titanic_train) a,
         (select stats_mode(EMBARKED) emb_mode
          from titanic_train) b;

-- Check again the statistical overview
exec dm_populate_stats_tbl_proc(-
  p_owner    => 'DMUSER',-
  p_data_tbl => 'titanic_train_eda_vw',-
  p_num_tbl  => 'titanic_train_num_stats',-
  p_char_tbl => 'titanic_train_char_stats');

col column_name format a15
col count format 999,990
select COLUMN_NAME, 
       "COUNT", 
       round(MEAN,3) MEAN, 
       round(STD,3) STD,
       round("MIN",3) "MIN",
       round("25%",3) "25%",
       round("50%",3) "50%",
       round("75%",3) "75%",
       round("MAX",3) "MAX"
from titanic_train_num_stats;

col column_name format a15
col top format a40
select * from titanic_train_char_stats;

exec dm_create_null_tbl_proc('DMUSER','titanic_train_eda_vw');
select * from titanic_train_eda_vw_null_ct;


-- ################
-- # Apply Models #
-- ################

-- recreate view with only desired columns
CREATE OR REPLACE VIEW titanic_train_vw AS
  SELECT PASSENGERID, SURVIVED, PCLASS, SEX, 
         AGE, SIBSP, PARCH, FARE, EMBARKED
  FROM   titanic_train;

/*-------------------- Train/Test Split --------------------*/

select count(*) from titanic_train_vw;

BEGIN
  execute immediate 'drop view titanic_build_data_vw';
EXCEPTION
  WHEN others THEN
    null;
END;
/

create view titanic_build_data_vw as
  select * from titanic_train_vw
  where ora_hash(passengerid, 99, 0) <= 60;

select count(*) from titanic_build_data_vw;

-- use opposite in where to create test set
BEGIN
  execute immediate 'drop view titanic_test_data_vw';
EXCEPTION
  WHEN others THEN
    null;
END;
/

create view titanic_test_data_vw
as select * from titanic_train_vw
   where ora_hash(passengerid, 99, 0) > 60;

select count(*) from titanic_test_data_vw;

-- Create table to capture accuracy of each model
BEGIN
  execute immediate 'drop table titanic_model_accuracy';
EXCEPTION
  WHEN others THEN
    null;
END;
/

create table titanic_model_accuracy (
  MODEL_NAME VARCHAR2(128),
  ALGO_NAME  VARCHAR2(128),
  ACCURACY   NUMBER);


/*-------------------- Decision Tree --------------------*/

DECLARE
  v_model_set_tbl VARCHAR2(30) := 'titanic_dt_settings';
  sql_stmt        VARCHAR2(200);
BEGIN
  -- Drop settings table
  BEGIN
    sql_stmt := 'drop table '||v_model_set_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- create settings table
  sql_stmt := 'CREATE TABLE '||v_model_set_tbl||'
      (setting_name   VARCHAR2(30),
       setting_value  VARCHAR2(4000)
    )';
  execute immediate sql_stmt;
END;
/

BEGIN
  -- insert settings for a decision tree
  INSERT INTO titanic_dt_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.algo_name, dbms_data_mining.algo_decision_tree);

  INSERT INTO titanic_dt_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.prep_auto, dbms_data_mining.prep_auto_on);

  INSERT INTO titanic_dt_settings
    (setting_name, setting_value)
  values
    ('TREE_IMPURITY_METRIC','TREE_IMPURITY_ENTROPY');

  INSERT INTO titanic_dt_settings
    (setting_name, setting_value)
  values
    ('TREE_TERM_MAX_DEPTH',4);
  commit;
END;
/

DECLARE
  v_data_tbl      VARCHAR2(30) := 'titanic_build_data_vw';
  v_miss_num_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_NUM';
  v_miss_cat_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_CAT';
  v_model_set_tbl VARCHAR2(30) := 'titanic_dt_settings';
  v_model_name    VARCHAR2(30) := 'TITANIC_DT_MODEL';
  v_case_id_col   VARCHAR2(30) := 'PASSENGERID';
  v_target_col    VARCHAR2(30) := 'SURVIVED';
  sql_stmt        VARCHAR2(200);
  transform_stack dbms_data_mining_transform.TRANSFORM_LIST;
BEGIN
  -- Drop xform tables
  BEGIN
    sql_stmt := 'drop table '||v_miss_num_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;
  BEGIN
    sql_stmt := 'drop table '||v_miss_cat_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- Transform numeric attributes
  dbms_data_mining_transform.CREATE_MISS_NUM (
    miss_table_name => v_miss_num_tbl);

  dbms_data_mining_transform.INSERT_MISS_NUM_MEAN (
    miss_table_name => v_miss_num_tbl,
    data_table_name => v_data_tbl,
    exclude_list    => dbms_data_mining_transform.column_list (
                       v_target_col,
                       v_case_id_col));

  -- Transform categorical attributes
  dbms_data_mining_transform.CREATE_MISS_CAT (
    miss_table_name => v_miss_cat_tbl);

  dbms_data_mining_transform.INSERT_MISS_CAT_MODE (
    miss_table_name => v_miss_cat_tbl,
    data_table_name => v_data_tbl,
    exclude_list    => dbms_data_mining_transform.column_list (
                       v_target_col,
                       v_case_id_col));

  -- drop model
  BEGIN
    DBMS_DATA_MINING.DROP_MODEL(v_model_name);
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- stack missing numeric xforms
  dbms_data_mining_transform.STACK_MISS_NUM (
    miss_table_name	=> v_miss_num_tbl,
    xform_list		=> transform_stack);

  -- stack missing categorical xforms
  dbms_data_mining_transform.STACK_MISS_CAT (
    miss_table_name	=> v_miss_cat_tbl,
    xform_list		=> transform_stack);

  -- create the model
  DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => v_model_name,
    mining_function     => dbms_data_mining.classification,
    data_table_name     => v_data_tbl,
    case_id_column_name => v_case_id_col,
    target_column_name  => v_target_col,
    settings_table_name => v_model_set_tbl,
    xform_list		=> transform_stack);

END;
/

-- create view of predictions
CREATE OR REPLACE VIEW titanic_dt_test_results
AS SELECT PASSENGERID,
       prediction(TITANIC_DT_MODEL USING *) predicted_value,
       prediction_probability(TITANIC_DT_MODEL USING *) probability
FROM   titanic_test_data_vw;

-- create confusion matrix
CREATE or REPLACE PROCEDURE dm_create_conf_mtrx_proc
  (p_matrix_tbl       IN VARCHAR2, 
   p_apply_result_tbl IN VARCHAR2,
   p_target_tbl       IN VARCHAR2, 
   p_case_id          IN VARCHAR2,
   p_target_col       IN VARCHAR2, 
   p_accuracy_tbl     IN VARCHAR2,
   p_model_name       IN VARCHAR2)
AUTHID CURRENT_USER is
  v_accuracy  NUMBER;
  sql_stmt    VARCHAR2(200);
  v_algo      all_mining_models.ALGORITHM%TYPE;
BEGIN
  BEGIN
    sql_stmt := 'drop table '|| p_matrix_tbl;
    EXECUTE IMMEDIATE sql_stmt;
    dbms_output.put_line('Dropped table: '|| p_matrix_tbl);
  EXCEPTION
    WHEN others THEN
      dbms_output.put_line('Table '|| p_matrix_tbl ||' does not exist.');
  END;

  DBMS_DATA_MINING.COMPUTE_CONFUSION_MATRIX (
      accuracy                     => v_accuracy,
      apply_result_table_name      => p_apply_result_tbl,
      target_table_name            => p_target_tbl,
      case_id_column_name          => p_case_id,
      target_column_name           => p_target_col,
      confusion_matrix_table_name  => p_matrix_tbl,
      score_column_name            => 'PREDICTED_VALUE',
      score_criterion_column_name  => 'PROBABILITY',
      cost_matrix_table_name       => NULL,
      apply_result_schema_name     => NULL,
      target_schema_name           => NULL,
      cost_matrix_schema_name      => NULL,
      score_criterion_type         => 'PROBABILITY');
  DBMS_OUTPUT.PUT_LINE('**** MODEL ACCURACY ****: ' || ROUND(v_accuracy,4));

  select ALGORITHM into v_algo
  from all_mining_models 
  where model_name=p_model_name;

  sql_stmt := 'insert into '||p_accuracy_tbl||' (MODEL_NAME, ALGO_NAME, ACCURACY)
               values ('''||p_model_name||''','''||v_algo||''','||v_accuracy||')';
  execute immediate sql_stmt;
  commit;
END;
/

exec dm_create_conf_mtrx_proc( -
  p_matrix_tbl       => 'TITANIC_DT_CONFUSION_MATRIX', -
  p_apply_result_tbl => 'titanic_dt_test_results', -
  p_target_tbl       => 'titanic_test_data_vw', -
  p_case_id          => 'PASSENGERID', -
  p_target_col       => 'SURVIVED', -
  p_accuracy_tbl     => 'titanic_model_accuracy', -
  p_model_name       => 'TITANIC_DT_MODEL');

col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy;

-- show confusion matrix
break on report;
compute sum label 'TOTAL' of "PRED_0" "PRED_1" "TOTAL" on report;
col ACTUAL_TARGET_VALUE heading 'ACTUAL'
col PRED_0 heading 'PREDICTED|0'
col PRED_1 heading 'PREDICTED|1'
select *
FROM
  (SELECT ACTUAL_TARGET_VALUE, PREDICTED_TARGET_VALUE, VALUE
   FROM   TITANIC_DT_CONFUSION_MATRIX
   UNION
   SELECT ACTUAL_TARGET_VALUE, -1 PREDICTED_TARGET_VALUE, sum(value) VALUE
   FROM   TITANIC_DT_CONFUSION_MATRIX
   GROUP BY ACTUAL_TARGET_VALUE, -1)
PIVOT (
  SUM(value)
  FOR predicted_target_value
  IN (0 "PRED_0",1 "PRED_1",-1 "TOTAL"))
ORDER BY actual_target_value;


/*-------------------- Random Forest --------------------*/

DECLARE
  v_model_set_tbl VARCHAR2(30) := 'titanic_rf_settings';
  sql_stmt        VARCHAR2(200);
BEGIN
  -- Drop settings table
  BEGIN
    sql_stmt := 'drop table '||v_model_set_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- create settings table
  sql_stmt := 'CREATE TABLE '||v_model_set_tbl||'
      (setting_name   VARCHAR2(30),
       setting_value  VARCHAR2(4000)
    )';
  execute immediate sql_stmt;
END;
/

BEGIN
  -- insert settings for a random forest
  INSERT INTO titanic_rf_settings
    (setting_name, setting_value)
  values (dbms_data_mining.algo_name, dbms_data_mining.ALGO_RANDOM_FOREST);

  INSERT INTO titanic_rf_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.prep_auto, dbms_data_mining.prep_auto_on);

  INSERT INTO titanic_rf_settings
    (setting_name, setting_value)
  values
    ('RFOR_NUM_TREES', 100);
  commit;
END;
/

DECLARE
  v_data_tbl      VARCHAR2(30) := 'titanic_build_data_vw';
  v_miss_num_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_NUM';
  v_miss_cat_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_CAT';
  v_model_set_tbl VARCHAR2(30) := 'titanic_rf_settings';
  v_model_name    VARCHAR2(30) := 'TITANIC_RF_MODEL';
  v_case_id_col   VARCHAR2(30) := 'PASSENGERID';
  v_target_col    VARCHAR2(30) := 'SURVIVED';
  sql_stmt        VARCHAR2(200);
  transform_stack dbms_data_mining_transform.TRANSFORM_LIST;
BEGIN
  -- drop model
  BEGIN
    DBMS_DATA_MINING.DROP_MODEL(v_model_name);
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- stack missing numeric xforms
  dbms_data_mining_transform.STACK_MISS_NUM (
    miss_table_name	=> v_miss_num_tbl,
    xform_list		=> transform_stack);

  -- stack missing categorical xforms
  dbms_data_mining_transform.STACK_MISS_CAT (
    miss_table_name	=> v_miss_cat_tbl,
    xform_list		=> transform_stack);

  -- create the model
  DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => v_model_name,
    mining_function     => dbms_data_mining.classification,
    data_table_name     => v_data_tbl,
    case_id_column_name => v_case_id_col,
    target_column_name  => v_target_col,
    settings_table_name => v_model_set_tbl,
    xform_list		=> transform_stack);
END;
/

-- create view of predictions
CREATE OR REPLACE VIEW titanic_rf_test_results
AS SELECT PASSENGERID,
       prediction(TITANIC_RF_MODEL USING *) predicted_value,
       prediction_probability(TITANIC_RF_MODEL USING *) probability
FROM   titanic_test_data_vw;

exec dm_create_conf_mtrx_proc( -
  p_matrix_tbl       => 'TITANIC_RF_CONFUSION_MATRIX', -
  p_apply_result_tbl => 'titanic_rf_test_results', -
  p_target_tbl       => 'titanic_test_data_vw', -
  p_case_id          => 'PASSENGERID', -
  p_target_col       => 'SURVIVED', -
  p_accuracy_tbl     => 'titanic_model_accuracy', -
  p_model_name       => 'TITANIC_RF_MODEL');

col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy;

-- show confusion matrix
break on report;
compute sum label 'TOTAL' of "PRED_0" "PRED_1" "TOTAL" on report;
col ACTUAL_TARGET_VALUE heading 'ACTUAL'
col PRED_0 heading 'PREDICTED|0'
col PRED_1 heading 'PREDICTED|1'
select *
FROM
  (SELECT ACTUAL_TARGET_VALUE, PREDICTED_TARGET_VALUE, VALUE
   FROM   TITANIC_RF_CONFUSION_MATRIX
   UNION
   SELECT ACTUAL_TARGET_VALUE, -1 PREDICTED_TARGET_VALUE, sum(value) VALUE
   FROM   TITANIC_RF_CONFUSION_MATRIX
   GROUP BY ACTUAL_TARGET_VALUE, -1)
PIVOT (
  SUM(value)
  FOR predicted_target_value
  IN (0 "PRED_0",1 "PRED_1",-1 "TOTAL"))
ORDER BY actual_target_value;


/*-------------------- Logistic Regression --------------------*/

DECLARE
  v_model_set_tbl VARCHAR2(30) := 'titanic_glr_settings';
  sql_stmt        VARCHAR2(200);
BEGIN
  -- Drop settings table
  BEGIN
    sql_stmt := 'drop table '||v_model_set_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- create settings table
  sql_stmt := 'CREATE TABLE '||v_model_set_tbl||'
      (setting_name   VARCHAR2(30),
       setting_value  VARCHAR2(4000)
    )';
  execute immediate sql_stmt;
END;
/

BEGIN
  -- insert settings for a random forest
  INSERT INTO titanic_glr_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.algo_name, dbms_data_mining.ALGO_GENERALIZED_LINEAR_MODEL);

  INSERT INTO titanic_glr_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.prep_auto, dbms_data_mining.prep_auto_on);
  commit;
END;
/

DECLARE
  v_data_tbl      VARCHAR2(30) := 'titanic_build_data_vw';
  v_miss_num_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_NUM';
  v_miss_cat_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_CAT';
  v_model_set_tbl VARCHAR2(30) := 'titanic_glr_settings';
  v_model_name    VARCHAR2(30) := 'TITANIC_GLR_MODEL';
  v_case_id_col   VARCHAR2(30) := 'PASSENGERID';
  v_target_col    VARCHAR2(30) := 'SURVIVED';
  sql_stmt        VARCHAR2(200);
  transform_stack dbms_data_mining_transform.TRANSFORM_LIST;
BEGIN
  -- drop model
  BEGIN
    DBMS_DATA_MINING.DROP_MODEL(v_model_name);
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- stack missing numeric xforms
  dbms_data_mining_transform.STACK_MISS_NUM (
    miss_table_name	=> v_miss_num_tbl,
    xform_list		=> transform_stack);

  -- stack missing categorical xforms
  dbms_data_mining_transform.STACK_MISS_CAT (
    miss_table_name	=> v_miss_cat_tbl,
    xform_list		=> transform_stack);

  -- create the model
  DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => v_model_name,
    mining_function     => dbms_data_mining.classification,
    data_table_name     => v_data_tbl,
    case_id_column_name => v_case_id_col,
    target_column_name  => v_target_col,
    settings_table_name => v_model_set_tbl,
    xform_list		=> transform_stack);
END;
/

-- create view of predictions
CREATE OR REPLACE VIEW titanic_glr_test_results AS
  SELECT PASSENGERID,
         prediction(TITANIC_GLR_MODEL USING *) predicted_value,
         prediction_probability(TITANIC_GLR_MODEL USING *) probability
  FROM   titanic_test_data_vw;

exec dm_create_conf_mtrx_proc( -
  p_matrix_tbl       => 'TITANIC_GLR_CONFUSION_MATRIX', -
  p_apply_result_tbl => 'titanic_glr_test_results', -
  p_target_tbl       => 'titanic_test_data_vw', -
  p_case_id          => 'PASSENGERID', -
  p_target_col       => 'SURVIVED', -
  p_accuracy_tbl     => 'titanic_model_accuracy', -
  p_model_name       => 'TITANIC_GLR_MODEL');

col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy;

-- show confusion matrix
break on report;
compute sum label 'TOTAL' of "PRED_0" "PRED_1" "TOTAL" on report;
col ACTUAL_TARGET_VALUE heading 'ACTUAL'
col PRED_0 heading 'PREDICTED|0'
col PRED_1 heading 'PREDICTED|1'
select *
FROM
  (SELECT ACTUAL_TARGET_VALUE, PREDICTED_TARGET_VALUE, VALUE
   FROM   TITANIC_GLR_CONFUSION_MATRIX
   UNION
   SELECT ACTUAL_TARGET_VALUE, -1 PREDICTED_TARGET_VALUE, sum(value) VALUE
   FROM   TITANIC_GLR_CONFUSION_MATRIX
   GROUP BY ACTUAL_TARGET_VALUE, -1)
PIVOT (
  SUM(value)
  FOR predicted_target_value
  IN (0 "PRED_0",1 "PRED_1",-1 "TOTAL"))
ORDER BY actual_target_value;


/*-------------------- Naive Bayes --------------------*/

DECLARE
  v_model_set_tbl VARCHAR2(30) := 'titanic_nb_settings';
  sql_stmt        VARCHAR2(200);
BEGIN
  -- Drop settings table
  BEGIN
    sql_stmt := 'drop table '||v_model_set_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- create settings table
  sql_stmt := 'CREATE TABLE '||v_model_set_tbl||'
      (setting_name   VARCHAR2(30),
       setting_value  VARCHAR2(4000)
    )';
  execute immediate sql_stmt;
END;
/

BEGIN
  INSERT INTO titanic_nb_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.algo_name, dbms_data_mining.ALGO_NAIVE_BAYES);

  INSERT INTO titanic_nb_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.prep_auto, dbms_data_mining.prep_auto_on);
  commit;
END;
/

DECLARE
  v_data_tbl      VARCHAR2(30) := 'titanic_build_data_vw';
  v_miss_num_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_NUM';
  v_miss_cat_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_CAT';
  v_model_set_tbl VARCHAR2(30) := 'titanic_nb_settings';
  v_model_name    VARCHAR2(30) := 'TITANIC_NB_MODEL';
  v_case_id_col   VARCHAR2(30) := 'PASSENGERID';
  v_target_col    VARCHAR2(30) := 'SURVIVED';
  sql_stmt        VARCHAR2(200);
  transform_stack dbms_data_mining_transform.TRANSFORM_LIST;
BEGIN
  -- drop model
  BEGIN
    DBMS_DATA_MINING.DROP_MODEL(v_model_name);
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- stack missing numeric xforms
  dbms_data_mining_transform.STACK_MISS_NUM (
    miss_table_name	=> v_miss_num_tbl,
    xform_list		=> transform_stack);

  -- stack missing categorical xforms
  dbms_data_mining_transform.STACK_MISS_CAT (
    miss_table_name	=> v_miss_cat_tbl,
    xform_list		=> transform_stack);

  -- create the model
  DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => v_model_name,
    mining_function     => dbms_data_mining.classification,
    data_table_name     => v_data_tbl,
    case_id_column_name => v_case_id_col,
    target_column_name  => v_target_col,
    settings_table_name => v_model_set_tbl,
    xform_list		=> transform_stack);
END;
/

-- create view of predictions
CREATE OR REPLACE VIEW titanic_nb_test_results AS
  SELECT PASSENGERID,
         prediction(TITANIC_NB_MODEL USING *) predicted_value,
         prediction_probability(TITANIC_NB_MODEL USING *) probability
  FROM   titanic_test_data_vw;

exec dm_create_conf_mtrx_proc( -
  p_matrix_tbl       => 'TITANIC_NB_CONFUSION_MATRIX', -
  p_apply_result_tbl => 'titanic_nb_test_results', -
  p_target_tbl       => 'titanic_test_data_vw', -
  p_case_id          => 'PASSENGERID', -
  p_target_col       => 'SURVIVED', -
  p_accuracy_tbl     => 'titanic_model_accuracy', -
  p_model_name       => 'TITANIC_NB_MODEL');

col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy;

-- show confusion matrix
break on report;
compute sum label 'TOTAL' of "PRED_0" "PRED_1" "TOTAL" on report;
col ACTUAL_TARGET_VALUE heading 'ACTUAL'
col PRED_0 heading 'PREDICTED|0'
col PRED_1 heading 'PREDICTED|1'
select *
FROM
  (SELECT ACTUAL_TARGET_VALUE, PREDICTED_TARGET_VALUE, VALUE
   FROM   TITANIC_NB_CONFUSION_MATRIX
   UNION
   SELECT ACTUAL_TARGET_VALUE, -1 PREDICTED_TARGET_VALUE, sum(value) VALUE
   FROM   TITANIC_NB_CONFUSION_MATRIX
   GROUP BY ACTUAL_TARGET_VALUE, -1)
PIVOT (
  SUM(value)
  FOR predicted_target_value
  IN (0 "PRED_0",1 "PRED_1",-1 "TOTAL"))
ORDER BY actual_target_value;


/*-------------------- Support Vector Machines --------------------*/

DECLARE
  v_model_set_tbl VARCHAR2(30) := 'titanic_svm_settings';
  sql_stmt  VARCHAR2(200);
BEGIN
  -- Drop settings table
  BEGIN
    sql_stmt := 'drop table '||v_model_set_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- create settings table
  sql_stmt := 'CREATE TABLE '||v_model_set_tbl||'
      (setting_name   VARCHAR2(30),
       setting_value  VARCHAR2(4000)
    )';
  execute immediate sql_stmt;
END;
/

BEGIN
  INSERT INTO titanic_svm_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.algo_name, dbms_data_mining.ALGO_SUPPORT_VECTOR_MACHINES);

  INSERT INTO titanic_svm_settings
    (setting_name, setting_value)
  values
    (dbms_data_mining.prep_auto, dbms_data_mining.prep_auto_on);
  commit;
END;
/

DECLARE
  v_data_tbl      VARCHAR2(30) := 'titanic_build_data_vw';
  v_miss_num_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_NUM';
  v_miss_cat_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_CAT';
  v_model_set_tbl VARCHAR2(30) := 'titanic_svm_settings';
  v_model_name    VARCHAR2(30) := 'TITANIC_SVM_MODEL';
  v_case_id_col   VARCHAR2(30) := 'PASSENGERID';
  v_target_col    VARCHAR2(30) := 'SURVIVED';
  sql_stmt        VARCHAR2(200);
  transform_stack dbms_data_mining_transform.TRANSFORM_LIST;
BEGIN
  -- drop model
  BEGIN
    DBMS_DATA_MINING.DROP_MODEL(v_model_name);
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- stack missing numeric xforms
  dbms_data_mining_transform.STACK_MISS_NUM (
    miss_table_name	=> v_miss_num_tbl,
    xform_list		=> transform_stack);

  -- stack missing categorical xforms
  dbms_data_mining_transform.STACK_MISS_CAT (
    miss_table_name	=> v_miss_cat_tbl,
    xform_list		=> transform_stack);

  -- create the model
  DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => v_model_name,
    mining_function     => dbms_data_mining.classification,
    data_table_name     => v_data_tbl,
    case_id_column_name => v_case_id_col,
    target_column_name  => v_target_col,
    settings_table_name => v_model_set_tbl,
    xform_list		=> transform_stack);
END;
/

-- create view of predictions
CREATE OR REPLACE VIEW titanic_svm_test_results
AS SELECT PASSENGERID,
       prediction(TITANIC_SVM_MODEL USING *) predicted_value,
       prediction_probability(TITANIC_SVM_MODEL USING *) probability
FROM   titanic_test_data_vw;

exec dm_create_conf_mtrx_proc( -
  p_matrix_tbl       => 'TITANIC_SVM_CONFUSION_MATRIX', -
  p_apply_result_tbl => 'titanic_svm_test_results', -
  p_target_tbl       => 'titanic_test_data_vw', -
  p_case_id          => 'PASSENGERID', -
  p_target_col       => 'SURVIVED', -
  p_accuracy_tbl     => 'titanic_model_accuracy', -
  p_model_name       => 'TITANIC_SVM_MODEL');

col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy;

-- show confusion matrix
break on report;
compute sum label 'TOTAL' of "PRED_0" "PRED_1" "TOTAL" on report;
col ACTUAL_TARGET_VALUE heading 'ACTUAL'
col PRED_0 heading 'PREDICTED|0'
col PRED_1 heading 'PREDICTED|1'
select *
FROM
  (SELECT ACTUAL_TARGET_VALUE, PREDICTED_TARGET_VALUE, VALUE
   FROM   TITANIC_SVM_CONFUSION_MATRIX
   UNION
   SELECT ACTUAL_TARGET_VALUE, -1 PREDICTED_TARGET_VALUE, sum(value) VALUE
   FROM   TITANIC_SVM_CONFUSION_MATRIX
   GROUP BY ACTUAL_TARGET_VALUE, -1)
PIVOT (
  SUM(value)
  FOR predicted_target_value
  IN (0 "PRED_0",1 "PRED_1",-1 "TOTAL"))
ORDER BY actual_target_value;


-- ################################
-- # Re-train Most Accurate Model #
-- ################################

-- put accuracy measures into table/view for easy comparison
col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy;

-- retrain model with full training data set
DECLARE
  v_data_tbl      VARCHAR2(30) := 'titanic_train_vw';
  v_miss_num_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_NUM_FULL';
  v_miss_cat_tbl  VARCHAR2(30) := 'TITANIC_XFORM_MISS_CAT_FULL';
  v_model_set_tbl VARCHAR2(30) := 'titanic_rf_settings';
  v_model_name    VARCHAR2(30) := 'TITANIC_RF_MODEL_FULL';
  v_case_id_col   VARCHAR2(30) := 'PASSENGERID';
  v_target_col    VARCHAR2(30) := 'SURVIVED';
  sql_stmt        VARCHAR2(200);
  transform_stack dbms_data_mining_transform.TRANSFORM_LIST;
BEGIN
  -- Drop xform tables
  BEGIN
    sql_stmt := 'drop table '||v_miss_num_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;
  BEGIN
    sql_stmt := 'drop table '||v_miss_cat_tbl;
    execute immediate sql_stmt;
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- Transform numeric attributes
  dbms_data_mining_transform.CREATE_MISS_NUM (
    miss_table_name => v_miss_num_tbl);

  dbms_data_mining_transform.INSERT_MISS_NUM_MEAN (
    miss_table_name => v_miss_num_tbl,
    data_table_name => v_data_tbl,
    exclude_list    => dbms_data_mining_transform.column_list (
                       v_target_col,
                       v_case_id_col));

  -- Transform categorical attributes
  dbms_data_mining_transform.CREATE_MISS_CAT (
    miss_table_name => v_miss_cat_tbl);

  dbms_data_mining_transform.INSERT_MISS_CAT_MODE (
    miss_table_name => v_miss_cat_tbl,
    data_table_name => v_data_tbl,
    exclude_list    => dbms_data_mining_transform.column_list (
                       v_target_col,
                       v_case_id_col));

  -- drop model
  BEGIN
    DBMS_DATA_MINING.DROP_MODEL(v_model_name);
  EXCEPTION
    WHEN others THEN
      null;
  END;

  -- stack missing numeric xforms
  dbms_data_mining_transform.STACK_MISS_NUM (
    miss_table_name	=> v_miss_num_tbl,
    xform_list		=> transform_stack);

  -- stack missing categorical xforms
  dbms_data_mining_transform.STACK_MISS_CAT (
    miss_table_name	=> v_miss_cat_tbl,
    xform_list		=> transform_stack);

  -- create the model
  DBMS_DATA_MINING.CREATE_MODEL(
    model_name          => v_model_name,
    mining_function     => dbms_data_mining.classification,
    data_table_name     => v_data_tbl,
    case_id_column_name => v_case_id_col,
    target_column_name  => v_target_col,
    settings_table_name => v_model_set_tbl,
    xform_list		=> transform_stack);
END;
/

-- create view of predictions
CREATE OR REPLACE VIEW titanic_rf_test_results_full AS
  SELECT PASSENGERID,
         prediction(TITANIC_RF_MODEL_FULL USING *) predicted_value,
         prediction_probability(TITANIC_RF_MODEL_FULL USING *) probability
  FROM   titanic_test_data_vw;

exec dm_create_conf_mtrx_proc( -
  p_matrix_tbl       => 'TITANIC_RF_CONFUSION_MATRIX_FULL', -
  p_apply_result_tbl => 'titanic_rf_test_results_full', -
  p_target_tbl       => 'titanic_test_data_vw', -
  p_case_id          => 'PASSENGERID', -
  p_target_col       => 'SURVIVED', -
  p_accuracy_tbl     => 'titanic_model_accuracy', -
  p_model_name       => 'TITANIC_RF_MODEL_FULL');

col model_name format a40
col algo_name format a30
col accuracy format 0.9990
select * from titanic_model_accuracy order by accuracy desc;

-- show confusion matrix
break on report;
compute sum label 'TOTAL' of "PRED_0" "PRED_1" "TOTAL" on report;
col ACTUAL_TARGET_VALUE heading 'ACTUAL'
col PRED_0 heading 'PREDICTED|0'
col PRED_1 heading 'PREDICTED|1'
select *
FROM
  (SELECT ACTUAL_TARGET_VALUE, PREDICTED_TARGET_VALUE, VALUE
   FROM   TITANIC_RF_CONFUSION_MATRIX_FULL
   UNION
   SELECT ACTUAL_TARGET_VALUE, -1 PREDICTED_TARGET_VALUE, sum(value) VALUE
   FROM   TITANIC_RF_CONFUSION_MATRIX_FULL
   GROUP BY ACTUAL_TARGET_VALUE, -1)
PIVOT (
  SUM(value)
  FOR predicted_target_value
  IN (0 "PRED_0",1 "PRED_1",-1 "TOTAL"))
ORDER BY actual_target_value;


-- ####################
-- # Import Test Data #
-- ####################

-- drop external table
DECLARE
  v_table   VARCHAR2(30) := 'titanic_test_ext';
  sql_stmt  VARCHAR2(200);
BEGIN
  sql_stmt := 'drop table '|| v_table;
  EXECUTE IMMEDIATE sql_stmt;
  dbms_output.put_line('Dropped table: '|| v_table);
EXCEPTION
  WHEN others THEN
    dbms_output.put_line('Table '|| v_table ||' does not exist.');
END;
/

-- create external table
create table titanic_test_ext ( 
  PassengerId	NUMBER,
  Pclass	NUMBER,
  Name		VARCHAR2(128),
  Sex		VARCHAR2(8),
  Age		NUMBER,
  SibSp		NUMBER,
  Parch		NUMBER,
  Ticket	VARCHAR2(20),
  Fare		NUMBER,
  Cabin		VARCHAR2(15),
  Embarked	VARCHAR2(3)
) 
  organization external ( 
	type   ORACLE_LOADER 
	default directory titanic_dir
	access parameters ( 
	   records delimited by NEWLINE 
           skip 1
	   badfile titanic_dir:'titanic_test.bad'
	   nodiscardfile
	   logfile titanic_dir:'titanic_test.log'
	   fields terminated by ',' optionally enclosed by '"'
	   reject rows with all null fields 
	   ( 
             "PASSENGERID",
             "PCLASS",
             "NAME",
             "SEX",
             "AGE",
             "SIBSP",
             "PARCH",
             "TICKET",
             "FARE",
             "CABIN",
             "EMBARKED"
           ) 
	) 
	location ('test.csv') 
  ) 
reject limit unlimited;

-- confirm data
select count(*) from titanic_test_ext ;

col name format a30
select * from titanic_test_ext where rownum<=5;

-- confirm data types
desc titanic_test_ext

-- create internal table to improve performance
DECLARE
  v_table   VARCHAR2(30) := 'titanic_test';
  sql_stmt  VARCHAR2(200);
BEGIN
  sql_stmt := 'drop table '|| v_table;
  EXECUTE IMMEDIATE sql_stmt;
  dbms_output.put_line('Dropped table: '|| v_table);
EXCEPTION
  WHEN others THEN
    dbms_output.put_line('Table '|| v_table ||' does not exist.');
END;
/

create table titanic_test as
  select * from titanic_test_ext;

-- confirm data
select count(*) from titanic_test;

col name format a30
select * from titanic_test where rownum<=5;

-- confirm data types
desc titanic_test

-- stats overview
exec dm_create_stats_tbl_proc('DMUSER','titanic_test');
exec dm_populate_stats_tbl_proc(-
  p_owner    => 'DMUSER',-
  p_data_tbl => 'titanic_test',-
  p_num_tbl  => 'titanic_test_num_stats',-
  p_char_tbl => 'titanic_test_char_stats');


-- ###################
-- # Apply the model #
-- ###################

-- Create view with only desired columns
CREATE OR REPLACE VIEW titanic_test_vw AS
  SELECT PASSENGERID, PCLASS, SEX, 
         AGE, SIBSP, PARCH, FARE, EMBARKED
  FROM   titanic_test;

select * from titanic_test_vw where rownum<6;

-- drop results table
DECLARE
  drop_sql    VARCHAR2(500);
BEGIN
  drop_sql := 'drop table titanic_test_results';
  execute immediate drop_sql;
  dbms_output.put_line('Dropped: titanic_test_results');
EXCEPTION
  WHEN others THEN
    dbms_output.put_line('Table titanic_test_results does not exist.');
END;
/

-- apply model to full data
begin
  DBMS_DATA_MINING.APPLY (
    model_name           => 'TITANIC_RF_MODEL_FULL',
    data_table_name      => 'titanic_test_vw',
    case_id_column_name  => 'PASSENGERID',
    result_table_name    => 'titanic_test_results',
    data_schema_name     => 'DMUSER');
end;
/

-- Do a quick check of the results
select count(*) from titanic_test_results;

col probability format 0.9990
col cost format 0.9990
select * from titanic_test_results where rownum<6;

-- create view of predictions greater than 50%
create or replace view titanic_test_results_vm as
  select PASSENGERID, PREDICTION survived
  from titanic_test_results
  where PROBABILITY > 0.5;

-- confirm we have predictions for every record
select tbl.passengerid
from titanic_test tbl
where not exists
  (select '1'
   from titanic_test_results_vm vw
   where vw.passengerid=tbl.passengerid)
order by 1;

select * from titanic_test_results_vm where rownum<6;

SPOOL OFF

-- Export results to a CSV file
set echo off
set feedback off
SET MARKUP CSV ON QUOTE OFF
spool /u01/app/oracle/oml/titanic_test_results.csv
select * from titanic_test_results_vm;
SPOOL OFF
