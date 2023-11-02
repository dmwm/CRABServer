# testenv

Convinience playbook for download files from task_process of CRAB task from Schedd. To run ASO script locally.

## Prerequisite

- ansible >= 2.10
- Able to ssh to Schedd without password prompt.

## Run playbook

- Edit `inventory`
  - `input_task_name`: task name you want to copy.
  - find and replace `vocms059` with ssh destination name (the Schedd name in your local ssh_config)
- Run playbook
    ```bash
    ansible-playbook -i inventory playbook.yml
    ```

Playbook will create new directory with task name (replace `:` with `__`) and copy files into this directory.

## Run Rucio ASO script

Before we run, setup python environment and ensure connection to CRABRest and Rucio server.

```bash
# source the_environment (WMCore deps and export PYTHONPATH at ../src/python)
cd <to_task_name_directory>
cp /tmp/x509up_u1000 <x509-proxy-file-name> # update your proxy cert, Rucio ASO use this files as creds to talk with rucio and crabserver
# rm -rf task_process/transfers # in case you want to clean up bookkeeping
python3 ../../scripts/task_process/RUCIO_Transfers.py --force-publishanme /GenericTTbar/tseethon-integrationtest-1/USER
```
