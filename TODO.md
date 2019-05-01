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
- [ ] Integrate expression evaluator
- [ ] Add additional operator cache points
- [x] Add firecloud timeout shim
- [ ] Add background synchronizer
- [ ] Robust preflight for firecloud submissions

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

## New Features
* [Blob API](https://googleapis.github.io/google-cloud-python/latest/storage/index.html) now available via `dalmatian.getblob`
* `dalmatian.WorkspaceManager.upload_entities` now automatically checks for and uploads valid filepaths present in the dataframe
* `dalmatian.WorkspaceManager.update_attributes` now automatically checks for and uploads valid filepaths present in the attributes
* `dalmatian.WorkspaceManager.update_entity_attributes` now automatically attempts to convert native python lists into FireCloud lists

### Operator Cache

Lapdog's cache layer, the Operator Cache, has been integrated into Dalmatian.
This means that your WorkspaceManagers _may_ be able to run offline, and disconnected from FireCloud.
This will allow you to continue analyzing your workspaces during Firecloud downtime and then resynchronize afterwards.
You still need to wait for FireCloud to go online before running jobs, but
you can utilize the new robust preflight system to check that your inputs
are valid even while offline

## Other Changes
* `dalmatian.WorkspaceManager.update_attributes` can now take attributes as keyword arguments in addition to providing a premade dictionary
* Replaced `AssertionErrors` with `APIExceptions`, which are more descriptive and easier to handle
* `dalmatian.WorkspaceManager.update_config` now accepts an optional `wdl` argument, which is a new version of the method WDL to upload. This can be either the path to a wdl file or the raw wdl text
* Method configurations with `methodRepoMethod.methodNamespace` set to "latest" will use the latest method version **at the time the configuration was uploaded**
