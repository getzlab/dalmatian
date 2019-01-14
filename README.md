
## dalmatian

[FISS](https://github.com/broadinstitute/fiss)' faithful companion.

dalmatian is a collection of high-level functions for interacting with Firecloud via Pandas dataframes.

### Install

`pip install firecloud-dalmatian`

### Requirements

FireCloud uses the Google Cloud SDK (https://cloud.google.com/sdk/) to manage authorization. To use dalmatian, you must install the SDK and login locally with

```
gcloud auth application-default login

```
### Examples
Dalmatian provides the WorkspaceManager class for interacting with FireCloud workspaces.
```
import dalmatian
wm = dalmatian.WorkspaceManager(namespace, workspace)
```

#### Creating and managing workspaces
Create the workspace:
```
wm.create_workspace()
```

Upload samples and sample attributes (e.g., BAM paths). The attributes must be provided as a pandas DataFrame, in the following form:
 * the index must be named 'sample_id', and contain the sample IDs
 * the dataframe must contain the column 'participant_id'
 * if a 'sample_set_id' columns is provided, the corresponding sample sets will be generated
```
wm.upload_samples(attributes_df, add_participant_samples=True)
```
If `add_participant_samples=True`, all samples of a participant are stored in `participant.samples_`.

Add or update workspace attributes:
```
attr = {
    'attribute_name':'gs://attribute_path',
}
wm.update_attributes(attr)
```

Get attributes on samples, sample sets, participants:
```
samples_df = wm.get_samples()
sets_df = wm.get_sample_sets()
participants_df = wm.get_participants()
```

Create or update sets:
```
wm.update_sample_set('all_samples', samples_df.index)
wm.update_participant_set('all_participants', participant_df.index)
```

Copy/move data from workspace:
```
samples_df = wm.get_samples()
dalmatian.gs_copy(samples_df[attribute_name], dest_path)
dalmatian.gs_move(samples_df[attribute_name], dest_path)
```

Clone a workspace:
```
wm2 = dalmatian.WorkspaceManager(namespace2, workspace2)
wm2.create_workspace(wm)
```

#### Running jobs
Submit jobs:
```
wm.create_submission(config_namespace, config_name, sample_id, 'sample', use_callcache=True)
wm.create_submission(config_namespace, config_name, sample_set_id, 'sample_set', expression='this.samples', use_callcache=True)
wm.create_submission(config_namespace, config_name, participant_id, 'participant', expression='this.samples_', use_callcache=True)
```

Monitor jobs:
```
wm.get_submission_status()
```

Get runtime statistics (including cost estimates):
```
status_df = wm.get_sample_status(config_name)
workflow_status_df, task_dfs = wm.get_stats(status_df)
```

Re-run failed jobs (for a sample set):
```
status_df = wm.get_sample_set_status(config_name)
print(status_df['status'].value_counts())  # list sample statuses
wm.update_sample_set('reruns', status_df[status_df['status']=='Failed'].index)
wm.create_submission(config_namespace, config_name, sample_set_id, 'reruns', expression=this.samples, use_callcache=True)
```

### Contents

Including additional FireCloud Tools (enumerated below)

```
workflow_time
create_workspace
delete_workspace
upload_samples
upload_participants
update_participant_samples
update_attributes
get_submission_status
get_storage
get_stats
publish_config
get_samples
get_sample_sets
update_sample_set
delete_sample_set
update_configuration
check_configuration
get_google_metadata
parse_google_stats
calculate_google_cost
list_methods
get_method
get_method_version
list_configs
get_config
get_config_version
print_methods
print_configs
get_wdl
compare_wdls
compare_wdl
redact_outdated_method_versions
update_method
get_vm_cost
```


### Usage

Some functionality depends on the installed `gsutil`.

When using PY3 this creates a potential issue of requiring multiple accessible python installs.

Remediate this issue by defining an `env` variable for gsutil python

```
# replace path with path to local python 2.7 path.
# if using pyenv the following should work
# (assuming of course 2.7.12 is installed)
export CLOUDSDK_PYTHON=/usr/local/var/pyenv/versions/2.7.12/bin/python
```
