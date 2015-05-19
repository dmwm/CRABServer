#!/usr/bin/env python

import logging

class Task(object):
    """
    """
    GetFailedTasks_sql = "SELECT tm_taskname, tm_task_status FROM tasks WHERE tm_task_status = 'FAILED'"

    GetInjectedTasks_sql = "SELECT tm_taskname, tm_task_status FROM tasks WHERE \
    		        tm_task_status = 'INJECTED'"

    GetKillTasks_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                   tm_start_time, tm_start_injection, tm_end_injection, \
                   tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                   tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                   tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                   tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                   tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                   tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                   tm_user_infiles, tw_name, tm_dry_run, \
                   tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode \
                   FROM tasks WHERE tm_task_status = 'KILL' """

    GetNewResubmit_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                   tm_start_time, tm_start_injection, tm_end_injection, \
                   tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                   tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                   tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                   tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                   tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                   tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                   tm_user_infiles, tw_name, tm_dry_run, \
                   tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode \
                   FROM tasks WHERE tm_task_status = 'NEW' OR tm_task_status = 'RESUBMIT' """

    GetReadyTasks_sql = """SELECT tm_taskname, panda_jobset_id, tm_task_status, \
                   tm_start_time, tm_start_injection, tm_end_injection, \
                   tm_task_failure, tm_job_sw, tm_job_arch, tm_input_dataset, \
                   tm_site_whitelist, tm_site_blacklist, tm_split_algo, tm_split_args, \
                   tm_totalunits, tm_user_sandbox, tm_cache_url, tm_username, tm_user_dn, tm_user_vo, \
                   tm_user_role, tm_user_group, tm_publish_name, tm_asyncdest, tm_dbs_url, \
                   tm_publish_dbs_url, tm_publication, tm_outfiles, tm_tfile_outfiles, tm_edm_outfiles, \
                   tm_job_type, tm_arguments, panda_resubmitted_jobs, tm_save_logs, \
                   tm_user_infiles, tw_name, tm_dry_run, \
                   tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode \
                   FROM tasks WHERE tm_task_status = %(get_status)s AND tw_name = %(tw_name)s limit %(limit)s """

    GetUserFromID_sql = "SELECT tm_username FROM tasks WHERE tm_taskname=%(taskname)s"

    ID_sql = "SELECT tm_taskname, panda_jobset_id, tm_task_status, tm_user_role, \
             tm_user_group, tm_task_failure, tm_split_args, panda_resubmitted_jobs, \
             tm_save_logs, tm_username, tm_user_dn, tm_arguments, tm_input_dataset, tm_dbs_url, tm_publication, \
             tm_user_webdir, tm_output_dataset, tm_dry_run \
             FROM tasks WHERE tm_taskname=%(taskname)s"

    New_sql = "INSERT INTO tasks ( \
               tm_taskname,panda_jobset_id, tm_task_status, tm_start_time, tm_task_failure, \
   	       tm_job_sw, tm_job_arch, tm_input_dataset, tm_site_whitelist, tm_site_blacklist, \
               tm_split_algo, tm_split_args, tm_totalunits, tm_user_sandbox, tm_cache_url, \
   	       tm_username, tm_user_dn, tm_user_vo, tm_user_role, tm_user_group, tm_publish_name, \
   	       tm_asyncdest, tm_dbs_url, tm_publish_dbs_url, tm_publication, tm_outfiles, \
   	       tm_tfile_outfiles, tm_edm_outfiles, tm_job_type, tm_arguments,\
               panda_resubmitted_jobs, tm_save_logs, tm_user_infiles, tm_maxjobruntime, \
               tm_numcores, tm_maxmemory, tm_priority, tm_dry_run, \
               tm_user_files, tm_transfer_outputs, tm_output_lfn, tm_ignore_locality, tm_fail_limit, tm_one_event_mode) \
               VALUES (%(task_name)s, %(jobset_id)s, upper(%(task_status)s), UTC_TIMESTAMP(), \
   	       %(task_failure)s, %(job_sw)s, %(job_arch)s, %(input_dataset)s, %(site_whitelist)s, \
   	       %(site_blacklist)s, %(split_algo)s, %(split_args)s, %(total_units)s, \
               %(user_sandbox)s, %(cache_url)s, %(username)s, %(user_dn)s, %(user_vo)s, %(user_role)s, \
   	       %(user_group)s, %(publish_name)s, %(asyncdest)s, %(dbs_url)s, %(publish_dbs_url)s, \
               %(publication)s, %(outfiles)s, %(tfile_outfiles)s, %(edm_outfiles)s, \
   	       %(job_type)s, %(arguments)s, %(resubmitted_jobs)s, %(save_logs)s, %(user_infiles)s, %(maxjobruntime)s, \
               %(numcores)s, %(maxmemory)s, %(priority)s, %(dry_run)s, \
               %(user_files)s, %(transfer_outputs)s, %(output_lfn)s, %(ignore_locality)s, %(fail_limit)s, %(one_event_mode)s)"

    GetResubmitParams_sql = "SELECT tm_site_blacklist, tm_site_whitelist, tm_maxjobruntime, tm_maxmemory, tm_numcores, tm_priority \
                             FROM tasks \
                             WHERE tm_taskname = %(taskname)s"

    SetArgumentsTask_sql = "UPDATE tasks SET tm_arguments = %(arguments)s WHERE tm_taskname = %(taskname)s"

    SetEndInjection_sql = "UPDATE tasks SET tm_end_injection = %(tm_end_injection)s \
		          WHERE tm_taskname = %(tm_taskname)s"

    SetFailedTasks_sql = "UPDATE tasks SET tm_end_injection = UTC_TIMESTAMP(), \
                          tm_task_status = UPPER(%(tm_task_status)s), tm_task_failure = %(failure)s \
                          WHERE tm_taskname = %(tm_taskname)s"

    SetInjectedTasks_sql = """UPDATE tasks SET tm_end_injection = UTC_TIMESTAMP(), \
                         tm_task_status = upper(%(tm_task_status)s), panda_jobset_id = %(panda_jobset_id)s, \
			 panda_resubmitted_jobs = %(resubmitted_jobs)s WHERE tm_taskname = %(tm_taskname)s"""

    SetJobSetId_sql = "UPDATE tasks SET panda_jobset_id = %(jobsetid)s WHERE tm_taskname = %(taskname)s"

    SetReadyTasks_sql = "UPDATE tasks SET tm_start_injection = UTC_SYSTIMESTAMP(), \
                         tm_task_status = upper(%(tm_task_status)s)  WHERE tm_taskname = %(tm_taskname)s"

    SetSplitargsTask_sql = "UPDATE tasks SET tm_split_args = %(splitargs)s WHERE tm_taskname = %(taskname)"

    SetStartInjection_sql = "UPDATE tasks SET tm_start_injection = UTC_TIMESTAMP() \
		             WHERE tm_taskname = %(tm_taskname)s"

    SetStatusTask_sql = "UPDATE tasks SET tm_task_status = upper(%(status)s) WHERE tm_taskname = %(taskname)s"

    UpdateWorker_sql = """UPDATE tasks SET tw_name = %(tw_name)s, tm_task_status = %(set_status)s \
                WHERE tm_taskname IN (SELECT tm_taskname FROM (SELECT tm_taskname FROM tasks \
                WHERE tm_task_status = %(get_status)s ORDER BY tm_start_time limit %(limit)s) alias_name)"""

    SetUpdateOutDataset_sql = """UPDATE tasks SET tm_output_dataset = %(tm_output_dataset)s \
                                WHERE tm_taskname = %(tm_taskname)s"""

    UpdateWebUrl_sql = """UPDATE tasks SET tm_user_webdir = %(webdirurl)s \
                              WHERE tm_taskname = %(workflow)s"""

    SetDryRun_sql = "UPDATE tasks set tm_dry_run = %(dry_run)s WHERE tm_taskname = %(taskname)s"

    UpdateSchedd_sql = """UPDATE tasks SET tm_schedd = %(scheddname)s \
                              WHERE tm_taskname = %(workflow)s"""
