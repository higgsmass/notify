SET ECHO OFF;
SET SQLN OFF;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET SERVEROUTPUT ON;
SET PAGES 0;
SET LINESIZE 1000;
SET LONG 1000000;

DECLARE 
  L_REQUESTSET_ID NUMBER;
  L_BATCH_ID NUMBER;
BEGIN
    L_BATCH_ID := '&1';
    DBMS_OUTPUT.PUT_LINE('Batch ID = ' || L_BATCH_ID);
    CTB_BL_REQUESTSET_RUN.REQUESTSET_RUN(BATCH_ID => L_BATCH_ID, REQUESTSET_ID => L_REQUESTSET_ID);
    DBMS_OUTPUT.PUT_LINE('Request Set ID = ' || L_REQUESTSET_ID);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR' || SQLERRM);
END
;
/
EXIT
