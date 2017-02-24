#!/usr/bin/env python

import logging

class FileTransfers(object):
    """
    """

# Specific calls only for [ASO|CRAB3] Operator
# -------------------------------------------------------------------------
    AddNewFileTransfer_sql = "INSERT INTO filetransfersdb ( \
               tm_id, tm_username, tm_taskname, tm_destination, tm_destination_lfn, \
               tm_source, tm_source_lfn, tm_filesize, tm_publish, tm_jobid, tm_job_retry_count, tm_type, \
               tm_rest_host, tm_rest_uri, tm_transfer_state, tm_publication_state, tm_last_update, \
               tm_transfer_max_retry_count, tm_publication_max_retry_count, tm_start_time) \
               VALUES (:id, :username, :taskname, :destination, :destination_lfn, \
                       :source, :source_lfn, :filesize, :publish, :job_id, :job_retry_count, :type, \
                       :rest_host, :rest_uri, :transfer_state, :publication_state, :last_update, \
                       :transfer_max_retry_count, :publication_max_retry_count, :start_time)"

    AcquireTransfers_sql = "UPDATE filetransfersdb SET tm_aso_worker = :asoworker, \
                               tm_last_update = :last_update, \
                               tm_transfer_state = :new_transfer_state \
                            WHERE tm_aso_worker IS NULL AND \
                                  tm_transfer_state = :transfer_state AND \
                                  tm_username = :username AND \
                                  rownum <= :limit"

    AcquirePublication_sql = "UPDATE filetransfersdb SET tm_last_update = :last_update, \
                                                         tm_publication_state = :new_publication_state, \
                                                         tm_aso_worker = :asoworker \
                              WHERE tm_aso_worker = :asoworker AND \
                              WHERE (tm_aso_worker = :asoworker or tm_aso_worker is NULL) AND \
                                    tm_transfer_state = :transfer_state AND \
                                    tm_publish = :publish_flag AND \
                                    tm_publication_state = :publication_state"

    UpdateTransfers_sql = "UPDATE filetransfersdb SET tm_transfer_state = :transfer_state, \
                                                      tm_last_update = :last_update, \
						      tm_transfer_failure_reason = :fail_reason, \
						      tm_transfer_retry_count = tm_transfer_retry_count + :retry_value, \
					          tm_fts_id = CASE WHEN :fts_id is NULL THEN tm_fts_id ELSE :fts_id END,\
						      tm_fts_instance = CASE WHEN :fts_instance is NULL THEN tm_fts_instance ELSE :fts_instance END\
                           WHERE tm_id = :id AND \
                                 tm_aso_worker = :asoworker"


    UpdateTransfers1_sql = "UPDATE filetransfersdb SET tm_transfer_state = :transfer_state, \
                                                       tm_last_update = :last_update, \
                                                       tm_fts_instance = :fts_instance, \
                                                       tm_fts_id = :fts_id \
                            WHERE tm_id = :id AND \
                                  tm_aso_worker = :asoworker"

    UpdatePublication_sql = "UPDATE filetransfersdb SET tm_publication_state = :publication_state, \
                                                        tm_last_update = :last_update, \
							tm_publication_failure_reason = :fail_reason, \
							tm_publication_retry_count = tm_publication_retry_count + :retry_value \
                             WHERE tm_id = :id AND \
                                   tm_aso_worker = :asoworker"

    RetryPublication_sql = "UPDATE filetransfersdb SET tm_publication_state = :new_publication_state, \
                                                       tm_last_update = :last_update, \
                                                       tm_aso_worker = NULL, \
                                                       tm_publication_retry_count = tm_publication_retry_count + 1 \
                            WHERE tm_id = :id AND \
                                  tm_aso_worker = :asoworker AND \
                                  tm_publication_state = :publication_state AND \
                                  tm_publication_max_retry_count <= tm_publication_retry_count"

    RetryTransfers_sql = "UPDATE filetransfersdb SET tm_transfer_state = :new_transfer_state, \
                                                     tm_last_update = :last_update, \
                                                     tm_aso_worker = NULL, \
                                                     tm_transfer_retry_count = tm_transfer_retry_count + 1 \
                          WHERE tm_transfer_state = :transfer_state AND \
                                tm_aso_worker = :asoworker AND \
                                (:last_update - tm_last_update) > :time_to AND \
                                tm_transfer_max_retry_count >= tm_transfer_retry_count"

    KillTransfers_sql = "UPDATE filetransfersdb SET tm_transfer_state = :new_transfer_state, \
                                                    tm_last_update = :last_update, \
                         WHERE tm_id = :id AND \
                               tm_transfer_state = :transfer_state"

# Move to taskDB
    GetVOMSAttr_sql = "SELECT tm_user_role, tm_user_group from tasks WHERE tm_taskname = :taskname AND ROWNUM = 1"

    GetDocsTransfer0_sql = "SELECT f.*, t.tm_user_role, t.tm_user_group \
			    FROM filetransfersdb f \
			    LEFT OUTER JOIN tasks t ON t.tm_taskname = f.tm_taskname \
                            WHERE tm_transfer_state = :state AND \
                                  tm_aso_worker = :asoworker AND \
                                  rownum < :limit \
                            ORDER BY rownum"

    GetDocsTransfer1_sql = "SELECT f.*, t.tm_user_role, t.tm_user_group \
			    FROM filetransfersdb f \
			    LEFT OUTER JOIN tasks t ON t.tm_taskname = f.tm_taskname \
                            WHERE f.tm_transfer_state = :state AND \
                                  f.tm_username = :username AND \
                                  NVL(t.tm_user_role, 'None') = :vorole AND \
                                  NVL(t.tm_user_group, 'None') = :vogroup AND \
                                  f.tm_aso_worker = :asoworker AND \
                                  rownum < :limit \
                            ORDER BY rownum"

    GetDocsTransfer2_sql = "SELECT * FROM filetransfersdb \
                            WHERE tm_transfer_state = :state AND \
                                  tm_source = :source AND \
                                  tm_aso_worker = :asoworker AND \
                                  rownum < :limit \
                            ORDER BY rownum"

    GetDocsTransfer3_sql = "SELECT * FROM filetransfersdb \
                            WHERE tm_transfer_state = :state AND \
                                  tm_source = :source AND \
                                  tm_destination = :destination AND \
                                  tm_aso_worker = :asoworker AND \
                                  rownum < :limit \
                            ORDER BY rownum"

    GetDocsPublication0_sql = "SELECT f.*, t.tm_user_role, t.tm_user_group, \
				      t.tm_input_dataset, t.tm_cache_url, \
				      t.tm_dbs_url \
			       FROM filetransfersdb f \
			       LEFT OUTER JOIN tasks t ON t.tm_taskname = f.tm_taskname \
                               WHERE tm_publication_state = :state AND \
                                     tm_aso_worker = :asoworker AND \
                                     rownum < :limit \
                               ORDER BY rownum"

    GetDocsPublication1_sql = "SELECT f.*, t.tm_user_role, t.tm_user_group, \
				      t.tm_input_dataset, t.tm_cache_url, \
				      t.tm_dbs_url \
			       FROM filetransfersdb f \
			       LEFT OUTER JOIN tasks t ON t.tm_taskname = f.tm_taskname \
                               WHERE f.tm_username = :username AND \
                                     tm_publication_state = :state AND \
                                     tm_aso_worker = :asoworker AND \
                                     rownum < :limit \
                               ORDER BY rownum"

    GetGroupedTransferStatistics0_sql = "SELECT count(*) as count, tm_aso_worker, tm_transfer_state FROM filetransfersdb \
                                         GROUP BY tm_aso_worker, tm_transfer_state"

    GetGroupedTransferStatistics1_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_aso_worker = :asoworker \
                                         GROUP BY tm_aso_worker, tm_username, tm_transfer_state"


    GetGroupedTransferStatistics1a_sql = "SELECT count(*) as count, tm_username, tm_transfer_state FROM filetransfersdb \
                                          WHERE tm_username = :username \
                                          GROUP BY tm_username, tm_transfer_state"

    GetGroupedTransferStatistics2_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_taskname, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_aso_worker = :asoworker AND tm_taskname = :taskname \
                                         GROUP BY tm_aso_worker, tm_username, tm_taskname, tm_transfer_state"

    GetGroupedTransferStatistics2a_sql = "SELECT count(*) as count, tm_username, tm_taskname, tm_transfer_state FROM filetransfersdb \
                                          WHERE tm_username = :username AND tm_taskname = :taskname \
                                          GROUP BY tm_username, tm_taskname, tm_transfer_state"

    GetGroupedTransferStatistics3_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_source, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_aso_worker = :asoworker AND tm_source = :source \
                                         GROUP BY tm_aso_worker, tm_username, tm_source, tm_transfer_state"

    GetGroupedTransferStatistics3a_sql = "SELECT count(*) as count, tm_username, tm_source, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_source = :source \
                                         GROUP BY tm_username, tm_source, tm_transfer_state"

    GetGroupedTransferStatistics4_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_source, tm_destination, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_aso_worker = :asoworker AND tm_source = :source AND tm_destination = :destination \
                                         GROUP BY tm_aso_worker, tm_username, tm_source, tm_destination, tm_transfer_state"

    GetGroupedTransferStatistics4a_sql = "SELECT count(*) as count, tm_username, tm_source, tm_destination, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_source = :source AND tm_destination = :destination \
                                         GROUP BY tm_username, tm_source, tm_destination, tm_transfer_state"

    GetGroupedTransferStatistics5_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_taskname, tm_source, tm_destination, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_aso_worker = :asoworker AND tm_source = :source AND tm_destination = :destination AND tm_taskname = :taskname \
                                         GROUP BY tm_aso_worker, tm_username, tm_taskname, tm_source, tm_destination, tm_transfer_state"

    GetGroupedTransferStatistics5a_sql = "SELECT count(*) as count, tm_username, tm_taskname, tm_source, tm_destination, tm_transfer_state FROM filetransfersdb \
                                         WHERE tm_username = :username AND tm_source = :source AND tm_destination = :destination AND tm_taskname = :taskname  \
                                         GROUP BY tm_username, tm_taskname, tm_source, tm_destination, tm_transfer_state"


    GetGroupedPublicationStatistics0_sql = "SELECT count(*) as count, tm_aso_worker, tm_publication_state FROM filetransfersdb \
                                            GROUP BY tm_aso_worker, tm_publication_state"

    GetGroupedPublicationStatistics1_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_publication_state FROM filetransfersdb \
                                            WHERE tm_username = :username AND tm_aso_worker = :asoworker \
                                            GROUP BY tm_aso_worker, tm_username, tm_publication_state"

    GetGroupedPublicationStatistics1a_sql = "SELECT count(*) as count, tm_username, tm_publication_state FROM filetransfersdb \
                                             WHERE tm_username = :username \
                                             GROUP BY tm_username, tm_publication_state"

    GetGroupedPublicationStatistics2_sql = "SELECT count(*) as count, tm_aso_worker, tm_username, tm_taskname, tm_publication_state FROM filetransfersdb \
                                            WHERE tm_username = :username AND tm_aso_worker = :asoworker AND tm_taskname = :taskname \
                                            GROUP BY tm_aso_worker, tm_username, tm_taskname, tm_publication_state"

    GetGroupedPublicationStatistics2a_sql = "SELECT count(*) as count, tm_username, tm_taskname, tm_publication_state FROM filetransfersdb \
                                             WHERE tm_username = :username AND tm_taskname = :taskname \
                                             GROUP BY tm_username, tm_taskname, tm_publication_state"

# -------------------------------------------------------------------------

    KillUserTransfers_sql = "UPDATE filetransfersdb SET tm_transfer_state = :new_transfer_state, \
                                                        tm_last_update = :last_update \
                           WHERE tm_username = :username AND tm_transfer_state = :transfer_state \
                           AND tm_taskname = :taskname"

    KillUserTransfersById_sql = "UPDATE filetransfersdb SET tm_transfer_state = :new_transfer_state, \
                                                            tm_last_update = :last_update \
                                 WHERE tm_username = :username AND tm_id = :id AND tm_transfer_state = :transfer_state"

    UpdateUserTransfersById_sql = "UPDATE filetransfersdb SET tm_transfer_state = :transfer_state, \
                                                              tm_last_update = :last_update, \
                                                              tm_start_time = :start_time, \
                                                              tm_source = :source, \
                                                              tm_source_lfn = :source_lfn, \
                                                              tm_filesize = :filesize, \
                                                              tm_publication_state = :publication_state, \
                                                              tm_transfer_retry_count = CASE WHEN :transfer_retry_count = NULL THEN tm_transfer_retry_count ELSE :transfer_retry_count END, \
                                                              tm_jobid = :job_id, \
                                                              tm_job_retry_count = :job_retry_count, \
                                                              tm_aso_worker = NULL \
                                   WHERE tm_username = :username AND \
                                         tm_taskname = :taskname AND \
                                         tm_id = :id "


    RetryUserPublication_sql = "UPDATE filetransfersdb SET tm_publication_state = :new_publication_state, \
                                                       tm_last_update = :last_update, \
                                                       tm_aso_worker = NULL, \
                            WHERE tm_username = :username AND tm_taskname = :takname \
                            AND tm_publication_state = :publication_state"

    RetryUserTransfers_sql = "UPDATE filetransfersdb SET tm_transfer_state = :new_transfer_state, \
                                                         tm_last_update = :last_update \
                              WHERE tm_username = :username AND tm_transfer_state = :transfer_state \
                              AND tm_taskname = :taskname"

    GetById_sql = "SELECT * FROM filetransfersdb where tm_id = :id"

# As jobs can be retried we should look only at the last ones. For that specific case this needs to be relooked.
    GetTaskStatusForTransfers_sql = "SELECT tm_id, tm_jobid, tm_transfer_state, tm_start_time, tm_last_update, tm_fts_id, tm_fts_instance FROM filetransfersdb \
                                     WHERE tm_username = :username AND tm_taskname = :taskname"  # ORDER BY tm_job_retry_count"
    GetTaskStatusForPublication_sql = "SELECT tm_id, tm_jobid, tm_publication_state, tm_start_time, tm_last_update FROM filetransfersdb \
                                       WHERE tm_username = :username AND tm_taskname = :taskname"  # ORDER BY tm_job_retry_count"

    GetActiveUsers_sql = "SELECT t.tm_username, t.tm_user_role, t.tm_user_group, count(*) FROM filetransfersdb f LEFT OUTER JOIN tasks t ON t.tm_taskname = f.tm_taskname \
                                       WHERE (tm_transfer_state=0 AND tm_aso_worker IS NULL) OR (tm_transfer_state=1 AND tm_aso_worker=:asoworker) GROUP BY t.tm_username,t.tm_user_role,t.tm_user_group"


