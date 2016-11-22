SET ECHO OFF;
SET SQLN OFF;
SET VERIFY OFF;
SET FEEDBACK OFF;
SET SERVEROUTPUT ON;
SET FEEDBACK OFF
SET HEADING OFF
SET TRIMSPOOL ON
SET TAB OFF

SET PAGES 0;
SET LINESIZE 1000;
SET LONG 1000000;

DECLARE
  L_VALIDATE_STATUS VARCHAR2(2000);
  L_TRANSFER_STATUS VARCHAR2(2000);
BEGIN
  CTB_BL_MONITOR_STATUS.MONITOR_STATUS (
      REQUEST_SET_ID => ${request_set_id},
      VALIDATE_STATUS => L_VALIDATE_STATUS,
      TRANSFER_STATUS => L_TRANSFER_STATUS);
  DBMS_OUTPUT.PUT_LINE('VALIDATE_STATUS ' || L_VALIDATE_STATUS);
  DBMS_OUTPUT.PUT_LINE('TRANSFER_STATUS ' || L_TRANSFER_STATUS);
EXCEPTION
WHEN OTHERS THEN
  DBMS_OUTPUT.PUT_LINE('ERROR' || SQLERRM);
END
;
/
EXIT
