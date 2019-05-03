
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


# Features in development

## Breaking Changes
* **No Python 2 Support**
* Removed the _cnamespace_ argument for `dalmatian.WorkspaceManager.create_submission`
* The _workflow\_inputs_ argument to `dalmatian.autofill_config_template` is now
**kewyord-only**
* The _mode_ argument to `dalmatian.redact_method` is now **keyword-only**

## New Features
* `dalmatian.fetch_method`: new function to handle the variety of acceptable argument
types and return a method JSON dictionary
* [Blob API](https://googleapis.github.io/google-cloud-python/latest/storage/index.html) now available via `dalmatian.getblob`
* `dalmatian.WorkspaceManager.upload_entities` now automatically checks for and uploads valid filepaths present in the dataframe
* `dalmatian.WorkspaceManager.update_attributes` now automatically checks for and uploads valid filepaths present in the attributes
* `dalmatian.WorkspaceManager.update_entity_attributes` now automatically attempts to convert native python lists into FireCloud lists
* `dalmatian.WorkspaceManager.update_participant_entities` now takes an additional
**optional** argument _target\_set_, which, if specified, restricts the operation
only to samples/pairs belonging to the given set.
* `dalmatian.WorkspaceManager.preflight`: new function which can be used to validate
your submission configuration and entity before sending to firecloud.
* `dalmatian.WorkspaceManager.validate_config`: new function which can be used to
check that required inputs are defined on a method configuration
* `dalmatian.WorkspaceManager.fetch_config_name`: new function which can be used
to get the canonical configuration name given a reference in any of the following
argument combinations:
    * `(config_namespace, config_name)` (Typical syntax used in legacy dalmatian)
    * `(config_name)` (Only works if the config name is unique within the workspace)
    * `("config_namespace/config_name")` (single string argument containing the canonical name)
    * `({"namespace": "config_namespace", "name": "config_name"})` (Format returned by `list_configs`)
* `dalmatian.WorkspaceManager.evaluate_expression`: Can be used to evaluate workspace
expressions (such as "this.samples.sample_id" or "workspace.gtf") within the context
of the workspace. Works in both online and offline modes
* `dalmatian.WorkspaceManager.update_acl`: new function to update user permissions
on the workspace. Takes an argument of `{"user_email": "permissions"}`, where _permissions_ can be `"OWNER"`, `"WRITER"`, `"READER"`, or `"NO ACCESS"`
* Several methods of `dalmatian.WorkspaceManager` are now also available as **Properties**:
    * `WorkspaceManager.firecloud_workspace` : (Readonly, New method) Returns FireCloud's
    workspace-level metadata including owners, bucket_id, and attributes
    * `WorkspaceManager.entity_types` : (Readonly, New method) Returns the list
    of entity types in the workspace, and some information about each type
    * `WorkspaceManager.acl` : (Readonly, New method) Returns the current permissions
    configuration of the workspace
    * `WorkspaceManager.bucket_id` : (Readonly) Calls `WorkspaceManager.get_bucket_id()`
    * `WorkspaceManager.samples` : (Readonly) Calls `WorkspaceManager.get_samples()`
    * `WorkspaceManager.sample_sets` : (Readonly) Calls `WorkspaceManager.get_sample_sets()`
    * `WorkspaceManager.pairs` : (Readonly) Calls `WorkspaceManager.get_pairs()`
    * `WorkspaceManager.pair_sets` : (Readonly) Calls `WorkspaceManager.get_pair_sets()`
    * `WorkspaceManager.participants` : (Readonly) Calls `WorkspaceManager.get_participants()`
    * `WorkspaceManager.participant_sets` : (Readonly) Calls `WorkspaceManager.get_participant_sets()`
    * `WorkspaceManager.attributes` :
        * Getter: Calls `WorkspaceManager.get_attributes()`
        * Setter: Calls `WorkspaceManager.update_attributes()` (**NOTE:** Unlike a true setter, this only updates the provided attributes)
    * `WorkspaceManager.configs` : (Readonly) Calls `WorkspaceManager.list_configs()`
    * `WorkspaceManager.configurations` : (Readonly) Calls `WorkspaceManager.list_configs()`

### Operator Cache

Lapdog's cache layer, the Operator Cache, has been integrated into Dalmatian.
This means that your WorkspaceManagers _may_ be able to run offline, and disconnected from FireCloud.
This will allow you to continue analyzing your workspaces during Firecloud downtime and then resynchronize afterwards.
You still need to wait for FireCloud to go online before running jobs, but
you can utilize the new robust preflight system to check that your inputs
are valid even while offline

#### Operator Cache Utilities

The following new methods pertain to the workspace's operator cache

* `dalmatian.WorkspaceManager.go_offline`: New function to manually switch the cache
into offline mode
* `dalmatian.WorkspaceManager.sync` (also `.go_live`): New function to re-synchronize
with FireCloud. Changes made to the workspace will be replayed and sent to Firecloud.
If anything fails, the workspace will remain in offline mode
* `dalmatian.WorkspaceManager.populate_cache`: new function to preload all available
workspace data into the operator cache
* `dalmatian.WorkspaceManager.tentative_json`: new function which takes a `requests.Response` object
and tentatively unpacks the json. If the status code indicates failure, or anything
else goes wrong when unpacking, this switches the workspace into offline mode
* `dalmatian.WorkspaceManager.timeout`: (Context Manager) enforces the given timeout
on all requests made to firecloud while within the context. The timeout may be an
integer, in which case it is used as the timeout in seconds, or a string, in which
case a timeout will be selected dependent on whether or not that string appears
in the workspace cache

### Method Referencing

**Most** methods (except those listed at the bottom of this section) which operate
on methods, can now take arguments in a variety of formats.
In general, you can reference a method with:
* A method configuration JSON dictionary
* A method JSON dictionary
* The namespace and name as two separate arguments (Legacy syntax)
    * In functions which take a version, you may provide it as an optional 3rd argument
* A string in the format "namespace/name"
* A string in the format "namespace/name/version"

Unchanged functions:
* `dalmatian.compare_wdls` was left unchanged
* `dalmatian.compare_wdl` was left unchanged
* `dalmatian.update_method` was left unchanged

## Other Changes
* The _name_ argument on `dalmatian.get_method` is now optional
* The _name_ argument on `dalmatian.get_method_version` is now optional
* The _name_ and _version_ arguments on `dalmatian.get_config_template` are now optional
* Changed the call syntax for `dalmatian.autofill_config_template`:
    * _method_ argument is now optional
    * Added optional **keyword-only** argument _version_
    * **Breaking Change:** _workflow\_inputs_ is now **keyword-only**, but still required
* The _method\_name_ argument to `dalmatian.get_wdl` is now optional
* Changed the call syntax for `dalmatian.redact_method`:
    * _method\_name_ argument is now optional
    * **Breaking Change:** _mode_ is now **keword-only** but still required
* `dalmatian.WorkspaceManager.update_attributes` can now take attributes as keyword arguments in addition to providing a premade dictionary
* Replaced `AssertionErrors` with `APIExceptions`, which are more descriptive and easier to handle
* `dalmatian.WorkspaceManager.update_config` now accepts an optional `wdl` argument, which is a new version of the method WDL to upload. This can be either the path to a wdl file or the raw wdl text
* Method configurations with `methodRepoMethod.methodNamespace` set to "latest" will use the latest method version **at the time the configuration was uploaded**
* `dalmatian.WorkspaceManager.create_submission` now runs preflight before submission
* Changed the call syntax for `dalmatian.create_submission`:
    * _etype_ argument is now optional, and defaults to `config.RootEntityType`
    * **Breaking Change:** Removed the _cnamespace_ argument
    * _config_ argument now takes any of the following single-argument formats for config referencing:
      * `"method namespace/method name"`
      * `"method name"` (only if name is unique within the workspace)
      * `{method config JSON dictionary}` (Automatically upload the configuration if it's not present in the workspace)
* `dalmatian.WorkspaceManager.get_config` can now also take the following argument combinations to reference a method configuration:
    * `(config_namespace, config_name)` (Original syntax)
    * `(config_name)` (Only works if the config name is unique within the workspace)
    * `("config_namespace/config_name")`
    * `({"namespace": "config_namespace", "name": "config_name"})` (Format returned by `list_configs`)
* `dalmatian.WorkspaceManager.update_participant_entities` now makes up to 5 attempts
