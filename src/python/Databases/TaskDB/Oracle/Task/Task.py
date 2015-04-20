#!/usr/bin/env python

import logging
from collections import namedtuple

class Task(object):
    """
    """
     #ID
    ID_tuple = namedtuple("ID", ["taskname", "panda_jobset_id", "task_status", "user_role", "user_group", \
             "task_failure", "split_args", "panda_resubmitted_jobs", "save_logs", "username", \
             "user_dn", "arguments", "input_dataset", "dbs_url", "task_warnings", "publication", "user_webdir", \
             "asourl", "output_dataset", "collector", "schedd", "dry_run"])
    ID_sql = "SELECT tm_taskname, panda_jobset_id, tm_task_status, tm_user_role, tm_user_group, \
             tm_task_failure, tm_split_args, panda_resubmitted_jobs, tm_save_logs, tm_username, \
             tm_user_dn, tm_arguments, tm_input_dataset, tm_dbs_url, tm_task_warnings, tm_publication, tm_user_webdir, tm_asourl, \
             tm_output_dataset, tm_collector, tm_schedd, tm_dry_run \
             FROM tasks WHERE tm_taskname=:taskname"

    IDAll_sql = "SELECT tm_taskname, tm_task_status, tm_user_role, tm_user_group, \
             tm_save_logs, tm_username, tm_user_dn \
             FROM tasks WHERE tm_taskname = :taskname"

    #INSERTED BY ERIC SUMMER STUDENT
    ALLUSER_sql = "SELECT DISTINCT(tm_username) FROM tasks"
    TASKSUMMARY_sql = "select tm_username, tm_task_status, count(*) from tasks group by tm_username, tm_task_status order by tm_username"
    #get taskname by user and status
    GetByUserAndStatus_sql = "select tm_taskname from tasks where tm_username=:username and tm_task_status=:status"
    #quick search
    QuickSearch_sql = "SELECT * FROM tasks WHERE tm_taskname = :taskname"
    #get all jobs with a specified status
    TaskByStatus_sql = "SELECT tm_task_status,tm_taskname FROM tasks WHERE tm_task_status = :taskstatus AND tm_username=:username_"
    #get all the tasks in a certain state in the last :minutes minutes
    CountLastTasksByStatus = "SELECT tm_task_status, count(*) FROM tasks WHERE tm_start_time > SYS_EXTRACT_UTC(SYSTIMESTAMP) - (:minutes/1440)  GROUP BY tm_task_status"
    #get all the task failures sorted by username in the last :minutes minutes
    LastFailures = "SELECT tm_username, tm_taskname, tm_task_failure from tasks WHERE tm_start_time > SYS_EXTRACT_UTC(SYSTIMESTAMP) - (:minutes/1440) and tm_task_status='FAILED' \
                    AND tm_task_failure IS NOT NULL ORDER BY tm_username"

    #New
    New_sql = "INSERT INTO tasks ( \
              tm_taskname,tm_activity, panda_jobset_id, tm_task_status, tm_start_time, tm_task_failure, tm_job_sw, \
              tm_job_arch, tm_input_dataset, tm_use_parent, tm_site_whitelist, tm_site_blacklist, \
              tm_split_algo, tm_split_args, tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, \
              tm_user_vo, tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, tm_publish_dbs_url, \
              tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, tm_job_type, tm_generator, tm_arguments, \
              panda_resubmitted_jobs, tm_save_logs, tm_user_infiles, tm_maxjobruntime, tm_numcores, tm_maxmemory, tm_priority, \
              tm_scriptexe, tm_scriptargs, tm_extrajdl, tm_asourl, tm_events_per_lumi, tm_collector, tm_schedd, tm_dry_run, \
              tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode) \
              VALUES (:task_name, :task_activity, :jobset_id, upper(:task_status), SYS_EXTRACT_UTC(SYSTIMESTAMP), :task_failure, :job_sw, \
              :job_arch, :input_dataset, :use_parent, :site_whitelist, :site_blacklist, :split_algo, :split_args, \
              :total_units, :user_sandbox, :cache_url, :username, :user_dn, \
              :user_vo, :user_role, :user_group, :publish_name, :asyncdest, :dbs_url, :publish_dbs_url, \
              :publication, :outfiles, :tfile_outfiles, :edm_outfiles, :job_type, :generator, :arguments, \
              :resubmitted_jobs, :save_logs, :user_infiles, :maxjobruntime, :numcores, :maxmemory, :priority, \
              :scriptexe, :scriptargs, :extrajdl, :asourl, :events_per_lumi, :collector, :schedd_name, :dry_run, \
              :user_files, :transfer_outputs, :output_lfn, :ignore_locality, :fail_limit, :one_event_mode)"

    #GetFailedTasks
    GetFailedTasks_sql = "SELECT tm_taskname, tm_task_status FROM tasks WHERE tm_task_status = 'FAILED'"

    #GetInjectedTasks
    GetInjectedTasks_sql = "SELECT tm_taskname, tm_task_status FROM tasks WHERE tm_task_status = 'INJECTED'"

    #GetKillTasks
    GetKillTasks_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                       tm_start_time, tm_start_injection, tm_end_injection, \
                       tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                       tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                       tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                       tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                       tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                       tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                       tm_user_infiles, tw_name, tm_maxjobruntime, tm_numcores, tm_maxmemory, tm_priority, tm_activity, \
                       tm_scriptexe, tm_scriptargs, tm_extrajdl, tm_generator, tm_asourl, tm_events_per_lumi, \
                       tm_use_parent, tm_collector, tm_schedd, tm_dry_run, \
                       tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode \
                       FROM tasks WHERE tm_task_status = 'KILL' """

    #GetNewResubmit
    GetNewResubmit_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                       tm_start_time, tm_start_injection, tm_end_injection, \
                       tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                       tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                       tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                       tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                       tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                       tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                       tm_user_infiles, tw_name, tm_maxjobruntime, tm_numcores, tm_maxmemory, tm_priority, tm_activity, \
                       tm_scriptexe, tm_scriptargs, tm_extrajdl, tm_generator, tm_asourl, tm_events_per_lumi, \
                       tm_use_parent, tm_collector, tm_schedd, tm_dry_run, \
                       tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode \
                       FROM tasks WHERE tm_task_status = 'NEW' OR tm_task_status = 'RESUBMIT' """

    #GetReadyTasks
    GetReadyTasks_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                       tm_start_time, tm_start_injection, tm_end_injection, \
                       tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                       tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                       tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                       tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                       tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                       tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                       tm_user_infiles, tw_name, tm_maxjobruntime, tm_numcores, tm_maxmemory, tm_priority, tm_activity, \
                       tm_scriptexe, tm_scriptargs, tm_extrajdl, tm_generator, tm_asourl, tm_events_per_lumi, \
                       tm_use_parent, tm_collector, tm_schedd, tm_dry_run, \
                       tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode \
                       FROM tasks WHERE tm_task_status = :get_status AND ROWNUM <= :limit AND tw_name = :tw_name"""

    #GetUserFromID
    GetUserFromID_sql ="SELECT tm_username FROM tasks WHERE tm_taskname=:taskname"

    #GetTasksFromUser
    GetTasksFromUser_sql ="SELECT tm_taskname,tm_task_status, tw_name,tm_user_dn tm_spliti_args FROM tasks WHERE tm_username=:username AND tm_start_time>TO_TIMESTAMP(:timestamp, 'YYYY-MM-DD')"

    #GetResubmitParams
    GetResubmitParams_sql = "SELECT tm_site_blacklist, tm_site_whitelist, tm_maxjobruntime, tm_maxmemory, tm_numcores, tm_priority \
                             FROM tasks \
                             WHERE tm_taskname = :taskname"

    #SetArgumentsTask
    SetArgumentsTask_sql = "UPDATE tasks SET tm_arguments = :arguments WHERE tm_taskname = :taskname"

    # Obsolete
    ##SetEndInjection
    #SetEndInjection_sql = "UPDATE tasks SET tm_end_injection = :tm_end_injection  
    #                      WHERE tm_taskname = :tm_taskname"
    #    time_sql = "select SYS_EXTRACT_UTC(SYSTIMESTAMP) from dual"

    #SetFailedTasks
    SetFailedTasks_sql = "UPDATE tasks SET tm_end_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP), \
                         tm_task_status = UPPER(:tm_task_status), tm_task_failure = :failure \
                         WHERE tm_taskname = :tm_taskname"
   
    #SetInjectedTasks
    SetInjectedTasks_sql = """UPDATE tasks SET tm_end_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP), \
                             tm_task_status = upper(:tm_task_status), panda_jobset_id = :panda_jobset_id, \
                             panda_resubmitted_jobs = :resubmitted_jobs \
                             WHERE tm_taskname = :tm_taskname""" 
   
    #SetJobSetId
    SetJobSetId_sql = "UPDATE tasks SET panda_jobset_id = :jobsetid WHERE tm_taskname = :taskname"
   
    #SetReadyTasks
    SetReadyTasks_sql = "UPDATE tasks SET tm_start_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP), \
                        tm_task_status = upper(:tm_task_status)  WHERE tm_taskname = :tm_taskname"
   
    #SetSplitargsTask
    SetSplitargsTask_sql = "UPDATE tasks SET tm_split_args = :splitargs WHERE tm_taskname = :taskname"
   
    #SetStartInjection
    SetStartInjection_sql = "UPDATE tasks SET tm_start_injection = SYS_EXTRACT_UTC(SYSTIMESTAMP) \
                            WHERE tm_taskname = :tm_taskname"
   
    #SetStatusTask
    SetStatusTask_sql = "UPDATE tasks SET tm_task_status = upper(:status) WHERE tm_taskname = :taskname"
   
    #UpdateWorker
    UpdateWorker_sql = """UPDATE tasks SET tw_name = :tw_name, tm_task_status = :set_status \
                         WHERE tm_taskname IN (SELECT tm_taskname FROM (SELECT tm_taskname, rownum as counter \
                         FROM tasks WHERE tm_task_status = :get_status ORDER BY tm_start_time) \
                         WHERE counter <= :limit)"""

    #UpdateOutDataset
    SetUpdateOutDataset_sql = """UPDATE tasks SET tm_output_dataset = :tm_output_dataset \
                                WHERE tm_taskname = :tm_taskname"""

    #UpdateWarnings
    SetWarnings_sql = """UPDATE tasks SET tm_task_warnings=:warnings WHERE tm_taskname=:workflow"""

    #TaskUpdateWebDir
    UpdateWebUrl_sql = """UPDATE tasks SET tm_user_webdir = :webdirurl \
                              WHERE tm_taskname = :workflow"""

    SetDryRun_sql = "UPDATE tasks set tm_dry_run = :dry_run WHERE tm_taskname = :taskname"

    #UpdateSchedd_sql
    UpdateSchedd_sql = """UPDATE tasks SET tm_schedd = :scheddname \
                              WHERE tm_taskname = :workflow"""
