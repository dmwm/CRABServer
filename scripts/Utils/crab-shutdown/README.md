# CRAB Shutdown - useful tools

## Hold and release the dagmans on all the schedds

This directory contains two ansible playbooks to help with hold and release
the dagmans in all the productions schedds.

Further details on these operation are available on the CRAB 
[docs](https://cmscrab.docs.cern.ch/technical/crab-shutdown.html)

Install ansible on your laptop, then cd into this directory and hold the
dagmans in all the schedds with

```bash
ansible-playbook --diff -i inventory.ini 000-hold.yml
```


Then use this command to release all the dagmans

```bash
ansible-playbook --diff -i inventory.ini 001-release.yml
```

An example of a successful release is:

```plaintext
> ansible-playbook --diff -i inventory.txt 000-restart.yml

PLAY [schedd] **********************************************************************************************************************************************************************************************************************************************

TASK [Gathering Facts] *************************************************************************************************************************************************************************************************************************************
ok: [vocms0106.cern.ch]
[...]

TASK [qedit] ***********************************************************************************************************************************************************************************************************************************************
changed: [vocms0107.cern.ch]
[...]

TASK [ansible.builtin.debug] *******************************************************************************************************************************************************************************************************************************
ok: [vocms0106.cern.ch] => {
    "shelloutput.stdout_lines": [
        "Set attribute \"HoldKillSig\" for 39 matching jobs."
    ]
}
[...]

TASK [release] *********************************************************************************************************************************************************************************************************************************************
changed: [vocms0106.cern.ch]
[...]

TASK [ansible.builtin.debug] *******************************************************************************************************************************************************************************************************************************
ok: [vocms0106.cern.ch] => {
    "shelloutput.stdout_lines": [
        "All jobs matching constraint (jobuniverse==7&&jobstatus==5&&HoldReason==\"ORACLEOFF (by user condor)\"&&HoldReasonSubCode==20231212) have been released"
    ]
}
[...]

PLAY RECAP *************************************************************************************************************************************************************************************************************************************************
vocms0106.cern.ch          : ok=5    changed=2    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
[...]
```
 
