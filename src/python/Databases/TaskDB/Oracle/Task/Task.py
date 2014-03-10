#!/usr/bin/env python

import logging

class Task(object):
    """
    """
     #ID
    ID_sql = "SELECT tm_taskname, panda_jobset_id, tm_task_status, tm_user_role, tm_user_group, \
             tm_task_failure, tm_split_args, panda_resubmitted_jobs, tm_save_logs, tm_username, \
             tm_user_dn, tm_arguments FROM tasks WHERE tm_taskname=:taskname"
   
    #New
    New_sql = "INSERT INTO tasks ( \
              tm_taskname,panda_jobset_id, tm_task_status, tm_start_time, tm_task_failure, tm_job_sw, \
              tm_job_arch, tm_input_dataset, tm_site_whitelist, tm_site_blacklist, \
              tm_split_algo, tm_split_args, tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, \
              tm_user_vo, tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, tm_publish_dbs_url, \
              tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, tm_transformation, tm_job_type, tm_arguments,\
              panda_resubmitted_jobs, tm_save_logs, tm_user_infiles, tm_maxjobruntime, tm_numcores, tm_maxmemory, tm_priority) \
              VALUES (:task_name, :jobset_id, upper(:task_status), SYS_EXTRACT_UTC(SYSTIMESTAMP), :task_failure, :job_sw, \
              :job_arch, :input_dataset, :site_whitelist, :site_blacklist, :split_algo, :split_args, :total_units, :user_sandbox, \
              :cache_url, :username, :user_dn, \
              :user_vo, :user_role, :user_group, :publish_name, :asyncdest, :dbs_url, :publish_dbs_url, \
              :publication, :outfiles, :tfile_outfiles, :edm_outfiles, :transformation, :job_type, :arguments,\
              :resubmitted_jobs, :save_logs, :user_infiles, :maxjobruntime, :numcores, :maxmemory, :priority)"
   
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
                       tm_transformation, tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, tm_user_infiles \
                       FROM tasks WHERE tm_task_status = 'KILL' """
   
    #GetNewResubmit
    GetNewResubmit_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                       tm_start_time, tm_start_injection, tm_end_injection, \
                       tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                       tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                       tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                       tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                       tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                       tm_transformation, tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                       tm_user_infiles, tw_name \
                       FROM tasks WHERE tm_task_status = 'NEW' OR tm_task_status = 'RESUBMIT' """
   
    #GetReadyTasks
    GetReadyTasks_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                       tm_start_time, tm_start_injection, tm_end_injection, \
                       tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                       tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                       tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                       tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                       tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                       tm_transformation, tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                       tm_user_infiles, tw_name, tm_maxjobruntime, tm_numcores, tm_maxmemory, tm_priority \
                       FROM tasks WHERE tm_task_status = :get_status AND ROWNUM <= :limit AND tw_name = :tw_name"""
   
    #GetUserFromID
    GetUserFromID_sql ="SELECT tm_username FROM tasks WHERE tm_taskname=:taskname"
   
   
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


