
## dalmatian

[Fiss'](https://github.com/broadinstitute/fiss) faithful companion.

dalmatian is a collection of high-level companion functions for
Firecloud and FISS.

### Install

Instructions: See INSTALL in the repository.

### Requirements

FireCloud uses the Google Cloud SDK (https://cloud.google.com/sdk/) to manage authorization. To use dalmatian, you must install the SDK and login locally with

```
gcloud auth application-default login

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
