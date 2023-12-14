/*
Objective:
==========
	The following PL/SQL code will be used to create 
	an automatic process to remove 90 days older partitions 
	in FILETRANSFERSDB and FILEMETADATA tables in production database.

Database: 
=========
    These code is applied to the following databases:

	- production:    cms_analysis_reqmgr@cmsr
    - preproduction: cmsweb_analysis_preprod@int2r

Dependency/Requirements:
========================
	1. We must ensure that we have proper permission to create 
	scheduler jobs and send email alerts. If we do not have it
	then ask from Oracle support.

*/

/*
--------------------------------------------------------------------------------------------------------------------------

*/

/*

Since oracle 23.c (which is not in production yet at CERN IT), this can be replaced by 

CREATE OR REPLACE PROCEDURE CREATE_TABLE_PTNAME
IS
BEGIN

	create table PTNAME (pname varchar2(100),tname varchar2(100));

END CREATE_TABLE_PTNAME;

Ref: https://stackoverflow.com/a/15633220

*/

CREATE OR REPLACE PROCEDURE CREATE_TABLE_PTNAME
IS
c NUMBER;
v_sql clob;
BEGIN
        select count(*) into c  from user_tables   where table_name='PTNAME';
        IF c=0 THEN
				v_sql := '
				create table PTNAME
				(
					pname varchar2(100),
					tname varchar2(100)
				)';

				execute immediate v_sql;
        END IF;

END CREATE_TABLE_PTNAME;
/

/*=========================================================================================================================*/

CREATE OR REPLACE FUNCTION  TIME_TAKEN

	( start_time TIMESTAMP,end_time   TIMESTAMP )
	return varchar2
IS
	data varchar2(500);
BEGIN
	data     := 	    '<b>Above Query execution time</b>'||
                            '<table border=2><tr><td>Start Time</td><td>'|| TO_CHAR(start_time)||'</td></tr>'||
                            '<tr><td>End Time</td><td>' || TO_CHAR(end_time) || '</td></tr>'||
                            '<tr><td>Diff Time</td><td>' || TO_CHAR(end_time-start_time)||'</td></tr></table>';
	return data;

END TIME_TAKEN;
/


/*=========================================================================================================================*/
CREATE OR REPLACE PROCEDURE SEND_EMAIL
( data  IN varchar2 )
IS
	username varchar2(100);
	dbname   varchar2(100);
	emails   varchar(200) := 'cms-service-crab-operators@cern.ch';
	  /*emails   varchar(200) := 'alccs.prajesh.sharma@gmail.com,prajesh.sharma@cern.ch,Stefano.Belforte@cern.ch,daina.dirmaite@cern.ch';*/
--	emails   varchar(200) := 'dario.mapelli@cern.ch';

BEGIN
	select sys_context('USERENV','SESSION_USER')  into username from dual;
	select sys_context('USERENV','INSTANCE_NAME') into dbname   from dual;

        UTL_MAIL.send(
                sender     => 'crab-email-placeholder@cern.ch',
                recipients =>  emails,
                subject    => 'Alert from - '||username||'@'||dbname,
                message    => data,
                mime_type => 'text/html; charset=us-ascii');

END SEND_EMAIL;

/

/*=========================================================================================================================*/

CREATE OR REPLACE FUNCTION  INFO_INDEX


        ( tname  IN varchar2 )

        return varchar2

IS

        data varchar2(5000);
BEGIN
	data := data || '<table border=2><caption><b>Status of indexes  of '||tname||' table</b></caption>';
	data := data || '<tr>  <th>Index Name</th> <th>Index Type</th> <th>Status</th></tr>';
	
	FOR row IN (select index_name, index_type, status from user_indexes where TABLE_NAME=tname)
        LOOP
		data := data    || '<tr><td>'||row.index_name || '</td>'
                                ||     '<td>'||row.index_type     || '</td>'
                                ||     '<td>'||row.status    || '</td></tr>';

	END LOOP;
	data := data || '</table>';

return data;
END INFO_INDEX;
/

/*=========================================================================================================================*/
CREATE OR REPLACE FUNCTION  INFO_JOB

        return varchar2

IS

        data varchar2(5000);
BEGIN
        data := data || '<table border=2><caption><b>All JOBs and their status.</b></caption>';
        data := data || '<tr>  <th>JOB_NAME</th> <th>START_DATE</th> <th>REPEAT_INTERVAL</th> <th>NEXT_RUN_DATE</th> <th>STATE</th> <th>COMMENTS</th></tr>';

        FOR row IN (select JOB_NAME,START_DATE,REPEAT_INTERVAL,NEXT_RUN_DATE,STATE,COMMENTS from user_scheduler_jobs order by START_DATE desc)
        LOOP
                data := data    || '<tr><td>'||row.JOB_NAME 		|| '</td>'
                                ||     '<td>'||row.START_DATE     	|| '</td>'
                                ||     '<td>'||row.REPEAT_INTERVAL    	|| '</td>'
				||     '<td>'||row.NEXT_RUN_DATE    	|| '</td>'
				||     '<td>'||row.STATE    		|| '</td>'
				||     '<td>'||row.COMMENTS    		|| '</td></tr>';

        END LOOP;
        data := data || '</table>';

return data;
END INFO_JOB;
/

/*=========================================================================================================================*/
CREATE OR REPLACE FUNCTION  INFO_USER_ERRORS

        return varchar2

IS

        data varchar2(5000);
BEGIN
        data := data || '<table border=2><caption><b>USER ERRORS(if any) in our database </b></caption>';
        data := data || '<tr>  <th>NAME</th> <th>TYPE</th> <th>SEQUENCE</th> <th>LINE</th> <th>POSITION</th> <th>TEXT</th> <th>ATTRIBUTE</th> <th>MESSAGE_NUMBER</th> </tr>';

        FOR row IN (select NAME,TYPE,SEQUENCE,LINE,POSITION,TEXT,ATTRIBUTE,MESSAGE_NUMBER from USER_ERRORS)
        LOOP
                data := data    || '<tr><td>'||row.NAME             	|| '</td>'
                                ||     '<td>'||row.TYPE           	|| '</td>'
                                ||     '<td>'||row.SEQUENCE      	|| '</td>'
                                ||     '<td>'||row.LINE        		|| '</td>'
                                ||     '<td>'||row.POSITION             || '</td>'
                                ||     '<td>'||row.TEXT             	|| '</td>'
				||     '<td>'||row.ATTRIBUTE            || '</td>'
				||     '<td>'||row.MESSAGE_NUMBER       || '</td></tr>';

        END LOOP;
        data := data || '</table>';

return data;
END INFO_USER_ERRORS;
/

/*=========================================================================================================================*/
CREATE OR REPLACE FUNCTION  INFO_PARTITION 

	( tname  IN varchar2 )

	return varchar2

IS

	data varchar2(6000);
	high_value_date date;
	day_diff number;
	pname varchar(100):=NULL;
	gday_diff number :=0;

BEGIN
	data :=  '<table border=2><caption><b>All partitions from '||tname ||' table</b><caption>';
        data := data || '<tr>  <th>Partition Name</th> <th>Closed on</th> <th>Date</th> <th>Age (days)</th> </tr>';


        FOR row IN (select partition_name,high_value from user_tab_partitions where table_name = tname order by partition_name)
        LOOP
                high_value_date :=      TO_DATE(substr(row.high_value,11,11),'YYYY-MM-DD');
                day_diff        :=      trunc(sysdate) - high_value_date;

                IF day_diff > 90
                THEN
			
			IF day_diff > gday_diff
			THEN
				gday_diff	:=	day_diff;
				pname		:=	row.partition_name;
			END IF;

                END IF;


                data := data 	|| '<tr><td>'||TO_CHAR(row.partition_name )|| '</td>' 
				||     '<td>'||TO_CHAR(row.high_value 	  )|| '</td>'  
				||     '<td>'||TO_CHAR(high_value_date    )|| '</td>'
				||     '<td>'||TO_CHAR(day_diff           )|| '</td></tr>';

        END LOOP;
	data := data || '</table>';


	delete from PTNAME where  PTNAME.tname=INFO_PARTITION.tname;
	insert into PTNAME values ( pname,tname);
	commit;

return data;
END INFO_PARTITION;

/

/*=========================================================================================================================*/
CREATE OR REPLACE FUNCTION PARTITION_NAME_TOBE_DELETE
        ( tname  IN varchar2 )
        return varchar2
IS
        pname varchar(100):=NULL;
      
BEGIN
	SELECT PTNAME.pname INTO PARTITION_NAME_TOBE_DELETE.pname FROM PTNAME WHERE PTNAME.tname = PARTITION_NAME_TOBE_DELETE.tname;

	IF PARTITION_NAME_TOBE_DELETE.pname IS NULL
	THEN
		PARTITION_NAME_TOBE_DELETE.pname	:=	'NA';

	END IF;
	return PARTITION_NAME_TOBE_DELETE.pname;

END PARTITION_NAME_TOBE_DELETE;

/



/*=========================================================================================================================*/
CREATE OR REPLACE PROCEDURE DELETE_PARTITION
( tname varchar2)
IS
	pname varchar2(100);
	data varchar2(5000);
	start_time TIMESTAMP;
	end_time   TIMESTAMP;
	HEADER  varchar2(500);
	query   varchar2(500);

BEGIN
select systimestamp into start_time from dual;

	 pname	 := PARTITION_NAME_TOBE_DELETE(tname);

	 IF pname = 'NA' THEN

		 SEND_EMAIL('There is no partition eligible to remove in '||tname||' table');
		 return;
	 END IF;

	 HEADER  := '<b>ACTION:</b> <ol>'||
			 '<li>Collected index info before drop partition of the table '||tname||'</li>'||
			'<li>Start process of deletion of partition('||pname|| ') of the table(' || tname || ')</li>'||
			'<li>This process will alert you after delettion process completed.</li>'||
		    '</ol>';

	 query   := '<b>Query Execution start</b>:<br/>'||
			'ALTER TABLE '||tname||' DROP PARTITION ('||pname||') UPDATE INDEXES;';

	 SEND_EMAIL(HEADER||'<hr/>'||INFO_INDEX(tname)||'<hr/>'||query);
	 /*===================================================================================================*/

		/* execute immediate 'ALTER TABLE '||tname ||' SET  INTERVAL  ( numtoyminterval (1,''MONTH'') )';*/
		 execute immediate 'ALTER TABLE '||tname ||' DROP PARTITION ('||pname||') UPDATE INDEXES';

	/*====================================================================================================*/
	select systimestamp into end_time from dual;

         HEADER  := '<b>ACTION:</b> <ol>'||
                         '<li>Collected index info after drop partition of  the table '||tname||'</li>'||
                        '<li>Completed - process of deletion of partition('||pname|| ') of the table(' || tname || ')</li>'||
                        '<li>Time taken by this deletion process.</li>'||
                    '</ol>';

	 SEND_EMAIL(HEADER||'<hr/>'||INFO_INDEX(tname)||'<hr/>'||TIME_TAKEN(start_time,end_time));
	

END DELETE_PARTITION;
/

/*=========================================================================================================================*/
CREATE OR REPLACE PROCEDURE DELETE_PARTITION_MAIN IS

data varchar2(10000);
start_time TIMESTAMP;
end_time   TIMESTAMP;
FILETRANSFERSDB_pname varchar2(100);
FILEMETADATA_pname varchar2(100);
HEADER  varchar2(500);

BEGIN
select systimestamp into start_time from dual;


	HEADER	:= '<b>ACTION:</b> <ol> 
				<li>Collected partition information from FILETRANSFERSDB and FILEMETADATA tables.</li>
				<li>Collected partition names from FILETRANSFERSDB and FILEMETADATA tables to be delete.</li>
				<li>Collected indexes info/state realted to FILETRANSFERSDB and FILEMETADATA tables.</li>
				<li>Collected all scheduler jobs related to partition dropping.</li>
			</ol>';

	data 	:= INFO_PARTITION('FILETRANSFERSDB');
	FILETRANSFERSDB_pname   :=      PARTITION_NAME_TOBE_DELETE('FILETRANSFERSDB');
	data    := data || '<hr/>' || 'Partition Name of FILETRANSFERSDB table to be delete :<b>' || FILETRANSFERSDB_pname  ||'</b>';

        data 	:= data || '<hr/>' || INFO_PARTITION('FILEMETADATA');
	FILEMETADATA_pname      :=      PARTITION_NAME_TOBE_DELETE('FILEMETADATA');
	data    := data || '<hr/>' || 'Partition Name of FILEMETADATA table to be delete :<b>' || FILEMETADATA_pname  ||'</b>';

select systimestamp into end_time from dual;


        SEND_EMAIL(
			HEADER||'<hr/>'||
			data||'<hr/>'||
			INFO_INDEX('FILETRANSFERSDB')||'<hr/>'||
			INFO_INDEX('FILEMETADATA') ||'<hr/>'||
			INFO_JOB()||'<hr/>'||INFO_USER_ERRORS() || '<hr/>' ||
			TIME_TAKEN(start_time,end_time)||'<hr/>'||
			'<b>If you want to stop/modify/understand partition dropping process then '||
			'please click:<a href=''https://twiki.cern.ch/twiki/bin/view/CMSPublic/NotesAboutDatabaseManagement''>here.</a></b><br/>'
		);


END DELETE_PARTITION_MAIN;
/

/*=========================================================================================================================*/
CREATE OR REPLACE PROCEDURE DEL_PART_FILEMETADATA
IS
BEGIN
	DELETE_PARTITION('FILEMETADATA');

END DEL_PART_FILEMETADATA;
/

CREATE OR REPLACE PROCEDURE DEL_PART_FILETRANSFERSDB 
IS
BEGIN
        DELETE_PARTITION('FILETRANSFERSDB');

END DEL_PART_FILETRANSFERSDB;
/

/* ========================================================================================================================*/

CREATE OR REPLACE PROCEDURE DELETE_PARTITION_MANUAL
IS
BEGIN
	
DELETE_PARTITION_MAIN;
DEL_PART_FILEMETADATA;
DEL_PART_FILETRANSFERSDB;
	
END DELETE_PARTITION_MANUAL;

/*=========================================================================================================================*/

CREATE OR REPLACE PROCEDURE DISABLE_JOBS
IS
BEGIN
	dbms_scheduler.disable('JOB_DEL_PART_MAIN');
	dbms_scheduler.disable('JOB_DEL_PART_FILETRANSFERSDB');
	dbms_scheduler.disable('JOB_DEL_PART_FILEMETADATA');

	DELETE_PARTITION_MAIN();

END DISABLE_JOBS;
/

/*=========================================================================================================================*/

CREATE OR REPLACE PROCEDURE ENABLE_JOBS
IS
BEGIN
        dbms_scheduler.enable('JOB_DEL_PART_MAIN');
        dbms_scheduler.enable('JOB_DEL_PART_FILETRANSFERSDB');
        dbms_scheduler.enable('JOB_DEL_PART_FILEMETADATA');

        DELETE_PARTITION_MAIN();

END ENABLE_JOBS;
/

/*=========================================================================================================================*/
DECLARE
	c NUMBER;
BEGIN

        select count(*) into c from user_scheduler_jobs where job_name='JOB_DEL_PART_MAIN';
        IF c=1 THEN
                dbms_scheduler.drop_job(job_name => 'JOB_DEL_PART_MAIN');
        END IF;

        select count(*) into c from user_scheduler_jobs where job_name='JOB_DEL_PART_FILETRANSFERSDB';
        IF c=1 THEN
                dbms_scheduler.drop_job(job_name => 'JOB_DEL_PART_FILETRANSFERSDB');
        END IF;

        select count(*) into c from user_scheduler_jobs where job_name='JOB_DEL_PART_FILEMETADATA';
        IF c=1 THEN
                dbms_scheduler.drop_job(job_name => 'JOB_DEL_PART_FILEMETADATA');
        END IF;

 DBMS_SCHEDULER.CREATE_JOB (
   job_name           =>  'JOB_DEL_PART_MAIN',
   job_type           =>  'STORED_PROCEDURE',
   job_action         =>  'DELETE_PARTITION_MAIN',
   start_date         =>  '25-MAY-21 11.30.00 AM',
   repeat_interval    =>  'FREQ=MONTHLY',
   comments           =>  'This job will execute DELETE_PARTITION_MAIN procedure and collect all info related to partition dropping.',
   enabled            =>  TRUE);


  DBMS_SCHEDULER.CREATE_JOB (
   job_name           =>  'JOB_DEL_PART_FILETRANSFERSDB',
   job_type           =>  'STORED_PROCEDURE',
   job_action         =>  'DEL_PART_FILETRANSFERSDB',
   start_date         =>  '26-MAY-21 11.00.00 AM',
   repeat_interval    =>  'FREQ=MONTHLY',
   comments           =>  'This job will execute DEL_PART_FILETRANSFERSDB procedure and remove oldest partition of FILETRANSFERSDB table,selected by DELETE_PARTITION_MAIN procedure.',
   enabled            =>  TRUE);


  DBMS_SCHEDULER.CREATE_JOB (
   job_name           =>  'JOB_DEL_PART_FILEMETADATA',
   job_type           =>  'STORED_PROCEDURE',
   job_action         =>  'DEL_PART_FILEMETADATA',
   start_date         =>  '27-MAY-21 11.00.00 AM',
   repeat_interval    =>  'FREQ=MONTHLY',
   comments           =>  'This job will execute DEL_PART_FILEMETADATA procedure and remove oldest partition of FILEMETADATA table,selected by DELETE_PARTITION_MAIN procedure.',
   enabled            =>  TRUE);


END;
/

/*=========================================================================================================================*/



execute DELETE_PARTITION_MAIN;
/*execute DELETE_PARTITION_FILEMETADATA;*/

-- SELECT sum(bytes) AS db_size
-- FROM dba_segments WHERE owner LIKE 'CMSWEB_CRAB_%'

