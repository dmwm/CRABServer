# Trigger-ci

Simple scripts to trigger pipeline by tag and push in one go.

## Usage
Copy `env-example` to `env` and change the `ENV` variable to your own env (`test{2,11,12,14}`).
Then run:

```bash
bash tag_and_push.sh
```

The other `tag_and_push_*.sh` scripts also include the push option to control pipeline, described in [here](https://cmscrab.docs.cern.ch/technical/crab-cicd/gitlab/triggering.html#variables-in-rules).
