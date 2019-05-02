# Lapdog > Dalmatian Integration

This branch is dedicated towards bringing all features which are not specific to
running lapdog jobs into dalmatian

## Workspace Model
- [x] Automated Entity uploads
  * Get rid of the separation between `prepare_*_df` and `upload_*`
- [x] Automated attribute uploads
  * Include lapdog's kwarg syntax

## Methods
- [x] Add _latest_ handling
- [x] Add handling for method metadata inference from config

## Operator
- [x] Replace assertions with APIExceptions
- [x] Integrate internal cache with lapdog parity
  * Actually remove the array translator from `upload_*` methods
  * Drop wdl from cache. No reason to have this without running jobs offline
- [x] Integrate expression evaluator
- [ ] Add additional operator cache points
- [x] Add firecloud timeout shim
- [x] Add background synchronizer
  * WONTFIX: I think the background synchronizer added more uncertainty and problems
  than it actually solved. In addition, it's less useful in dalmatain than it was
  in lapdog
- [x] Robust preflight for firecloud submissions
- [ ] Add back array translation for uploads (upload first then update using a no-op)
- [ ] Check handling on Series/Index=False

## CLI
- [ ] Add lapdog CLI for uploads
  * Accept arbitrary entity types as an argument
- [ ] Add lapdog CLI for methods
- [ ] Add lapdog CLI for workspace creation
- [ ] Add lapdog CLI for job submission

## Other
- [x] Migrate GetBlob API
- [ ] Integrate ACL API
- [x] Properties!

# Writeup

In this section, below the divider, I will keep track of the PR writeup

---

# Lapdog feature transfer

This PR aims to move many features from broadinstitute/lapdog into broadinstitute/dalmatian

Any features which were not specific to running Lapdog jobs have been migrated.

## Breaking Changes
* Changed the syntax for `dalmatian.WorkspaceManager.create_submission`:
    * _cnamespace_ and _config_ have been combined into a single _config_ argument.
    This may be just the name, the namespace and name in "namespace/name" format,
    or the config summary as returned by `dalmatian.WorkspaceManager.list_configs`
    * _etype_ argument is now optional. If not provided, it defaults to `config.rootEntityType`

## New Features
* [Blob API](https://googleapis.github.io/google-cloud-python/latest/storage/index.html) now available via `dalmatian.getblob`
* `dalmatian.WorkspaceManager.upload_entities` now automatically checks for and uploads valid filepaths present in the dataframe
* `dalmatian.WorkspaceManager.update_attributes` now automatically checks for and uploads valid filepaths present in the attributes
* `dalmatian.WorkspaceManager.update_entity_attributes` now automatically attempts to convert native python lists into FireCloud lists
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
* Several methods of `dalmatian.WorkspaceManager` are now also available as **Properties**:
    * `WorkspaceManager.firecloud_workspace` : (Readonly, New method) Returns FireCloud's
    workspace-level metadata including owners, bucket_id, and attributes
    * `WorkspaceManager.entity_types` : (Readonly, New method) Returns the list
    of entity types in the workspace, and some information about each type
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

## Other Changes
* `dalmatian.WorkspaceManager.update_attributes` can now take attributes as keyword arguments in addition to providing a premade dictionary
* Replaced `AssertionErrors` with `APIExceptions`, which are more descriptive and easier to handle
* `dalmatian.WorkspaceManager.update_config` now accepts an optional `wdl` argument, which is a new version of the method WDL to upload. This can be either the path to a wdl file or the raw wdl text
* Method configurations with `methodRepoMethod.methodNamespace` set to "latest" will use the latest method version **at the time the configuration was uploaded**
* `dalmatian.WorkspaceManager.create_submission` now runs preflight before submission
* `dalmatian.WorkspaceManager.get_config` can now also take the following argument combinations to reference a method configuration:
    * `(config_namespace, config_name)` (Original syntax)
    * `(config_name)` (Only works if the config name is unique within the workspace)
    * `("config_namespace/config_name")`
    * `({"namespace": "config_namespace", "name": "config_name"})` (Format returned by `list_configs`)
