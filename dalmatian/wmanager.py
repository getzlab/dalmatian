from __future__ import print_function
import pandas as pd
import numpy as np
import subprocess
import os
import sys
import io
from collections import defaultdict, namedtuple
from functools import wraps, partial
import time
import tempfile
import firecloud.api
from firecloud import fiss
import iso8601
import pytz
import traceback
import requests
from datetime import datetime
from threading import RLock, local
import contextlib
from agutil import status_bar, splice, byteSize
from agutil.parallel import parallelize
from google.cloud import storage
import crayons
import warnings
from .core import *
from .base import LegacyWorkspaceManager
from .schema import Evaluator

# =============
# Shim in FireCloud request timeouts
# =============

timeout_state = local()

# For legacy methods or non-cached operations, the timeout will be None (infinite)
DEFAULT_LONG_TIMEOUT = 30 # Seconds to wait if a value is not cached
DEFAULT_SHORT_TIMEOUT = 5 # Seconds to wait if a value is already cached

@contextlib.contextmanager
def set_timeout(n):
    """
    Context Manager:
    Temporarily sets a timeout on all firecloud requests within context
    Resets timeout on context exit
    Thread safe! (Each thread has an independent timeout)
    """
    try:
        if not hasattr(timeout_state, 'timeout'):
            timeout_state.timeout = None
        old_timeout = timeout_state.timeout
        timeout_state.timeout = n
        yield
    finally:
        timeout_state.timeout = old_timeout

# Generate the fiss agent header
getattr(firecloud.api, "_fiss_agent_header")()
# Get the request method on the reusable user session
__CORE_SESSION_REQUEST__ = getattr(firecloud.api, "__SESSION").request

@wraps(__CORE_SESSION_REQUEST__)
def _firecloud_api_timeout_wrapper(*args, **kwargs):
    """
    Wrapped version of the fiss reusable session request method
    Applies a default timeout based on the current thread's timeout value
    Default timeout can be overridden by kwargs
    """
    if not hasattr(timeout_state, 'timeout'):
        timeout_state.timeout = None
    return __CORE_SESSION_REQUEST__(
        *args,
        **{
            **{'timeout': timeout_state.timeout},
            **kwargs
        }
    )

# Monkey Patch the wrapped request method
getattr(firecloud.api, "__SESSION").request = _firecloud_api_timeout_wrapper

#------------------------------------------------------------------------------
#  Top-level classes representing workspace(s)
#------------------------------------------------------------------------------
class WorkspaceCollection(object):
    def __init__(self):
        self.workspace_list = []

    def add(self, workspace_manager):
        assert isinstance(workspace_manager, WorkspaceManager)
        self.workspace_list.append(workspace_manager)

    def remove(self, workspace_manager):
        self.workspace_list.remove(workspace_manager)

    def print_workspaces(self):
        print('Workspaces in collection:')
        for i in self.workspace_list:
            print('  {}/{}'.format(i.namespace, i.workspace))

    def get_submission_status(self, show_namespaces=False):
        """Get status of all submissions across workspaces"""
        dfs = []
        for i in self.workspace_list:
            df = i.get_submission_status(show_namespaces=show_namespaces)
            if show_namespaces:
                df['workspace'] = '{}/{}'.format(i.namespace, i.workspace)
            else:
                df['workspace'] = i.workspace
            dfs.append(df)
        return pd.concat(dfs, axis=0)

# =============
# Preflight helper classes
# =============

PreflightFailure = namedtuple("PreflightFailure", ["result", "reason"])
PreflightSuccess = namedtuple(
    "PreflightSuccess",
    [
        "result",
        "config",
        "entity",
        "etype",
        "workflow_entities",
        "invalid_inputs"
    ]
)

# =============
# Hound Conflict Helper
# =============

ProvenanceConflict = namedtuple(
    "ProvenanceConflict",
    ['liveValue', 'latestRecord']
)

# =============
# Operator Cache Helper Decorators
# =============

def _synchronized(func):
    """
    Synchronizes access to the function using the instance's lock.
    Use if the function touches the operator cache
    """
    @wraps(func)
    def call_with_lock(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)
    return call_with_lock

def _read_from_cache(key, message=None):
    """
    Decorator factory.
    Use to decorate a method which populates a cache value.
    The decorated function will only run if the cache is live.
    The decorated function should attempt to retrieve a live value and return it
    Afterwards, regardless of if the function succeeded, attempt to retrieve
    the cached value or fail.
    Optionally provide a failure message as a second argument

    This decorator factory allows the code to more represent the mechanism
    for a live update and removes the boilerplate of updating and reading from cache

    If the key is callable, call it on all the provided args and kwargs to generate a key

    Use if your function is a relatively straightforward getter. Just decorate with
    this and _synchronized, and then build your function to fetch a result from firecloud.
    Combine with tentative_json in your function for best results
    """

    def decorator(func):

        @wraps(func)
        def call_using_cache(self, *args, **kwargs):
            # First, if the key is callable, use it to get a string key
            if callable(key):
                _key = key(self, *args, **kwargs)
            else:
                _key = key
            # Next, if the workspace is live, attempt a live update
            if self.live:
                # Call with timeout
                # Remember, a request timeout will just switch us to offline mode
                with self.timeout(_key):
                    result = func(self, *args, **kwargs)
                    if result is not None:
                        if _key in self.dirty:
                            self.dirty.remove(_key)
                        self.cache[_key] = result
            # Return the cached value, if present
            if _key in self.cache and self.cache[_key] is not None:
                return self.cache[_key]
            self.fail(message) # Fail otherwise

        return call_using_cache

    return decorator

@contextlib.contextmanager
def capture(display=True):
    """
    Context manager to redirect stdout and err
    """
    try:
        stdout_buff = io.StringIO()
        stderr_buff = io.StringIO()
        with contextlib.redirect_stdout(stdout_buff):
            with contextlib.redirect_stderr(stderr_buff):
                yield (stdout_buff, stderr_buff)
    finally:
        stdout_buff.seek(0,0)
        stderr_buff.seek(0,0)
        if display:
            print(stderr_buff.read(), end='', file=sys.stderr)
            stderr_buff.seek(0,0)
            print(stdout_buff.read(), end='')
            stdout_buff.seek(0,0)

class WorkspaceManager(LegacyWorkspaceManager):
    # This abstraction provides 2 benefits
    # 1) The code is now split between 2 files
    # 2) Base workspace manager functions are now segregated from higher level operator/caching functions

    def __init__(self, namespace, workspace=None, timezone='America/New_York', credentials=None, user_project=None):
        self.pending_operations = []
        self.cache = {}
        self.dirty = set()
        self.live = True
        self.lock = RLock()
        self._last_result = None
        super().__init__(namespace, workspace, timezone)

    def __repr__(self):
        return "<{}.{} {}/{}>".format(
            self.__class__.__module__,
            self.__class__.__name__,
            self.namespace,
            self.workspace
        )

    # =============
    # Operator Cache Internals
    # =============

    def go_offline(self):
        """
        Switches the WorkspaceManager into offline mode
        If there is a current exception being handled, log it
        """
        self.live = False
        a, b, c = sys.exc_info()
        if a is not None and b is not None:
            traceback.print_exc()
        print(
            crayons.red("WARNING:", bold=False),
            "The operation cache is now offline for {}/{}".format(
                self.namespace,
                self.workspace
            ),
            file=sys.stderr
        )

    @_synchronized
    def go_live(self):
        """
        Attempts to switch the WorkspaceManager into online mode
        Queued operations are replayed through the firecloud api
        If any operations fail, they are re-queued
        WorkspaceManager returns to online mode if all queued operations finish
        """
        failures = []
        exceptions = []
        for key, setter, getter in self.pending_operations:
            try:
                if setter is not None:
                    response = setter()
                    if isinstance(response, requests.Response) and response.status_code >= 400:
                        raise APIException(r)
            except Exception as e:
                failures.append((key, setter, getter))
                exceptions.append(e)
                traceback.print_exc()
            else:
                try:
                    if getter is not None:
                        response = getter()
                        if isinstance(response, requests.Response):
                            if response.status_code >= 400:
                                raise APIException(r)
                            else:
                                response = response.json()
                        if key is not None:
                            self.cache[key] = response
                            if key in self.dirty:
                                self.dirty.remove(key)
                except Exception as e:
                    failures.append((key, None, getter))
                    exceptions.append(e)
                    traceback.print_exc()
        self.pending_operations = [item for item in failures]
        self.live = not len(self.pending_operations)
        if len(exceptions):
            print("There were", len(exceptions), "exceptions while attempting to sync with firecloud")
        return self.live, exceptions

    sync = go_live

    @_synchronized
    def tentative_json(self, result, *expected_failures):
        """
        Tentatively unpacks a firecloud response's json
        If the status code is >= 400 (and not present in list of allowed failures)
        this will switch the WorkspaceManager back into Offline mode
        """
        self._last_result = result
        if result.status_code >= 400 and result.status_code not in expected_failures:
            self.go_offline()
            return None
        try:
            return result.json()
        except:
            self.go_offline()
            return None

    def timeout_for_key(self, key):
        """
        Gets an appropriate request timeout based on a given cache key
        If the key is an integer, use it directly as the timeout
        """
        if isinstance(key, str):
            return DEFAULT_SHORT_TIMEOUT if key in self.cache and self.cache[key] is not None else DEFAULT_LONG_TIMEOUT
        return key

    @contextlib.contextmanager
    def timeout(self, key):
        """
        Context Manager: Temporarily sets the request timeout for this thread
        based on the given cache key/timeout value
        Switches into offline mode if any requests time out
        Useful for foreground calls
        """
        try:
            with set_timeout(self.timeout_for_key(key)):
                yield
        except requests.ReadTimeout:
            self.go_offline()

    def call_with_timeout(self, key, func, *args, **kwargs):
        """
        Calls the given function with the given arguments
        Applies a timeout based on the given cache key
        Useful for background calls
        """
        try:
            with set_timeout(self.timeout_for_key(key)):
                return func(*args, **kwargs)
        except requests.ReadTimeout:
            self.go_offline()
            # Do not silence the exception
            # call_with_timeout is used for background calls
            # don't want to accidentally fill the cache with Nones
            raise

    def fail(self, message=None):
        """
        Call when the WorkspaceManager cannot fulfil a user request.
        Raises an APIException based on the last request made.
        Optionally provide a message describing the failure
        """
        if message is None:
            message = "Insufficient data in cache to complete operation"
        if self._last_result is not None:
            raise APIException(message, self._last_result)
        raise APIException(message)

    @_synchronized
    def _upload_config(self, config):
        """
        Internal use. If update_config determines it is safe to upload to firecloud,
        this method takes over
        """
        identifier = '{}/{}'.format(config['namespace'], config['name'])
        if self.initialize_hound() is not None:
            self.hound.write_log_entry(
                'other',
                "Uploaded/Updated method configuration: {}/{}".format(
                    json_body['namespace'],
                    json_body['name']
                )
            )
        if identifier not in {'{}/{}'.format(c['namespace'], c['name']) for c in self.configs}:
            # new config
            r = firecloud.api.create_workspace_config(self.namespace, self.workspace, config)
            if r.status_code == 201:
                print('Successfully added configuration: {}'.format(config['name']))
                return True
            else:
                raise APIException("Failed to upload new configuration", r)
        else:
            # old config
            r = firecloud.api.update_workspace_config(self.namespace, self.workspace,
                    config['namespace'], config['name'], config)
            if r.status_code == 200:
                print('Successfully updated configuration {}/{}'.format(config['namespace'], config['name']))
                return True
            else:
                raise APIException("Failed to update existing configuration", r)
        return False # Shouldn't be a control path to get here

    @_synchronized
    def _entities_live_update(self):
        """
        Internal Use. Fetches the list of entity types, but forces
        the workspace to go online
        """
        state = self.live
        try:
            self.live = True
            return self.entity_types
        finally:
            self.live = state

    def __patch(self, url, **kwargs):
        return getattr(firecloud.api, "__SESSION").patch(
            firecloud.api.urljoin(
                firecloud.api.fcconfig.root_url,
                url
            ),
            headers=firecloud.api._fiss_agent_header({"Content-type":  "application/json"}),
            **kwargs
        )

    def _df_upload_translation_layer(self, func, etype, updates, *args):
        result = self.__df_upload_translation_layer_internal(func, etype, updates, *args)
        if result:
            print("Manually updated one or more {}s requiring array translation".format(etype))

    @_synchronized
    def __df_upload_translation_layer_internal(self, func, etype, updates, *args):
        translations = False
        if isinstance(updates, pd.DataFrame):
            selector = {*updates.index}
            for name, data in updates.iterrows():
                for attr, val in data.items():
                    if isinstance(val, list):
                        selector.remove(name)
                        break
                if name not in selector:
                    # Apply the manual translated update to this row
                    translations |= self.__df_upload_translation_layer_internal(func, workspace, etype, data)
            func(etype, updates.loc[[*selector]], *args)
        else:
            response = self.__patch(
                '/api/workspaces/{}/{}/entities/{}/{}'.format(
                    self.namespace,
                    self.workspace,
                    etype,
                    updates.name
                ),
                json=[
                    {
                        'op': 'AddUpdateAttribute',
                        'attributeName': attr,
                        'addUpdateAttribute': (
                            {
                                'itemsType': "AttributeValue",
                                'items': val
                            } if isinstance(val, list) else val
                        )
                    }
                    for attr, val in updates.items()
                ]
            )
            if response.status_code >= 400:
                warnings.warn("Not Uploaded: Unable to translate entity %s" % repr(updates), stacklevel=2)
                self.tentative_json(response) # Just to log response as last error
                print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            else:
                # FIXME Translation layer hound log?
                translations = True
        return translations

    def _get_entities_internal(self, etype):
        return getattr(self, 'get_{}s'.format(etype))()

    @_synchronized
    @_read_from_cache(lambda self, namespace, name=None: 'valid:config:{}'.format('/'.join(self.get_config(namespace, name, decode_only=True))))
    def _validate_config_internal(self, namespace, name=None):
        """
        Internal component for config validation
        """
        return self.tentative_json(firecloud.api.validate_config(
            self.namespace,
            self.workspace,
            namespace,
            name
        ))

    @_synchronized
    def _update_participant_entities_internal(self, etype, column, participants, entities):

        @parallelize(3)
        def update_participant(participant_id):
            attr_dict = {
                column: {
                    "itemsType": "EntityReference",
                    "items": [{"entityType": etype, "entityName": i} for i in entities[participant_id]]
                }
            }
            attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
            # It adds complexity to put the context manager here, but
            # since the timeout is thread-specific it needs to be set within
            # the thread workers
            with set_timeout(DEFAULT_SHORT_TIMEOUT):
                try:
                    r = firecloud.api.update_entity(self.namespace, self.workspace, 'participant', participant_id, attrs)
                except requests.ReadTimeout:
                    return participant_id, 500 # fake a bad status code to requeue
            return participant_id, r.status_code

        n_participants = len(participants)

        for attempt in range(5):
            retries = []

            for k, status in status_bar.iter(update_participant(participants), len(participants), prepend="Updating {}s for participants ".format(etype)):
                if status >= 400:
                    retries.append(k)
                elif self.initialize_hound() is not None:
                    self.hound.update_entity_attribute(
                        'participant',
                        k,
                        column,
                        list(entities[k]),
                        "<Automated> Populating attribute from entity references"
                    )

            if len(retries):
                if attempt >= 4:
                    print("\nThe following", len(retries), "participants could not be updated:", ', '.join(retries), file=sys.stderr)
                    raise APIException("{} participants could not be updated after 5 attempts".format(len(retries)))
                else:
                    print("\nRetrying remaining", len(retries), "participants")
                    participants = [item for item in retries]
            else:
                break

        print('\n    Finished attaching {}s to {} participants'.format(etype, n_participants))
        if self.initialize_hound() is not None:
            for pid in participants:
                self.hound.update_entity_meta(
                    'participant',
                    pid,
                    "Updated {} membership".format(column)
                )

    @_synchronized
    @_read_from_cache(lambda self, namespace, name: 'config:{}/{}'.format(namespace, name))
    def _get_config_internal(self, namespace, name):
        """
        Get workspace configuration JSON
        Accepts the following formats:
        1) cnamespace = namespace; config = name
        2) cnamespace = "namespace/name"; config = None
        3) cnamespace = name; config = None
        """
        response = firecloud.api.get_workspace_config(
            self.namespace,
            self.workspace,
            namespace,
            name
        )
        if response.status_code == 404:
            raise ConfigNotFound("No such config {}/{} in this workspace".format(namespace, name))
        return self.tentative_json(response)

    @_synchronized
    def _configs_live_update(self):
        """
        Internal Use. Fetches the list of configs, but forces
        the workspace to go online
        """
        state = self.live
        try:
            self.live = True
            return self.configs
        finally:
            self.live = state

    def _check_conflicting_value(self, value, record):
        if isinstance(value, np.ndarray):
            value = list(value)
        rvalue = record.attributeValue if record is not None else None
        if isinstance(rvalue, np.ndarray):
            rvalue = list(rvalue)
        if record is None or np.all(value == rvalue) or (pd.isna(value) and pd.isna(rvalue)):
            return record
        return ProvenanceConflict(value, record)

    def _build_provenance_series_internal(self, etype, entity):
        # Path 1: enumerate all updates under the given entity then filter
        # This is 3x faster than path 2, if there is not a long attribute history
        return {
            **{
                attr: None
                for attr in entity.index
            },
            **{
                entry.attributeName: self._check_conflicting_value(
                    entity[entry.attributeName] if entry.attributeName in entity.index else None,
                    entry
                )
                for entry in self.hound.ientity_attribute_provenance(etype, entity.name)
            }
        }
        # # Path 2: Make a fast query for each individual attribute
        # # This is faster if we are fetching a small number of attributes
        # # That's hard to predict, so we'll leave this path disabled
        # return {
        #     attr: self.hound.latest(
        #         self.hound.get_entries(os.path.join('hound', etype, entity.name, attr)),
        #         'updates'
        #     )
        #     for attr in entity.index
        # }

    # =============
    # Operator Cache Method Overrides
    # =============

    @property
    @_synchronized
    @_read_from_cache('workspace')
    def firecloud_workspace(self):
        """
        Property: Fetches Firecloud Workspace Metadata
        """
        # Try to fetch workspace metadata from firecloud
        return self.tentative_json(firecloud.api.get_workspace(
            self.namespace,
            self.workspace
        ))

    def get_bucket_id(self):
        """Get the GCS bucket ID associated with the workspace"""
        metadata = self.firecloud_workspace
        if 'workspace' in metadata and 'bucketName' in metadata['workspace']:
            return metadata['workspace']['bucketName']
        self.fail("firecloud_workspace does not match expected format")

    @_synchronized
    @_read_from_cache('configs')
    def list_configs(self):
        """List configurations in workspace"""
        return self.tentative_json(firecloud.api.list_workspace_configs(
            self.namespace,
            self.workspace
        ))

    def get_config(self, reference, name=None, *args, decode_only=False):
        """
        Fetches a configuration by the provided reference
        Returns the configuration JSON object
        Accepts the following argument combinations
        1) reference = {method configuration JSON}
        2) reference = "config namespace", name = "config name"
        3) reference = "config name"
        4) reference = "config namespace/config name"
        """
        if len(args):
            raise TypeError("decode_only is a keyword-only argument")
        if isinstance(reference, dict):
            namespace = reference['namespace']
            name = reference['name']
            if 'inputs' in reference and 'outputs' in reference and not decode_only:
                return reference
        elif name is None:
            data = reference.split('/')
            if len(data) == 2:
                namespace = data[0]
                name = data[1]
            elif len(data) == 1:
                candidates = [
                    cfg for cfg in self.configs
                    if cfg['name'] == data[0]
                ]
                if len(candidates) == 0:
                    raise ConfigNotFound("No such config {} in this workspace".format(reference))
                if len(candidates) > 1:
                    raise ConfigNotUnique("Multiple configs by the name {} in this workspace".format(reference))
                namespace = candidates[0]['namespace']
                name = candidates[0]['name']
        else:
            namespace = reference
        if decode_only:
            return namespace, name
        return self._get_config_internal(namespace, name)

    get_configuration = get_config

    @_synchronized
    def update_config(self, config, wdl=None, synopsis=None):
        """
        Create or update a method configuration (separate API calls)

        config = {
           'namespace': config_namespace,
           'name': config_name,
           'rootEntityType' : entity,
           'methodRepoMethod': {'methodName':method_name, 'methodNamespace':method_namespace, 'methodVersion':version},
           'methodNamespace': method_namespace,
           'inputs':  {},
           'outputs': {},
           'prerequisites': {},
           'deleted': False
        }

        Optionally, if wdl is not None, upload wdl as the latest version of the method.
        Method namespace and name are taken from config['methodRepoMethod']
        wdl may be a filepath or literal WDL text.

        If synopsis is None, a sensible default is used

        """
        if "namespace" not in config or "name" not in config:
            raise ValueError("Config missing required keys 'namespace' and 'name'")
        mnamespace = config['methodRepoMethod']['methodNamespace']
        mname = config['methodRepoMethod']['methodName']
        if wdl is not None:
            with contextlib.ExitStack() as stack:
                if not os.path.isfile(wdl):
                    tmp = tempfile.NamedTemporaryFile('w', suffix='.wdl')
                    stack.enter_context(tmp)
                    tmp.write(wdl)
                    wdl = tmp.name
                if synopsis is None:
                    synopsis = "Runs " + mname
                update_method(mnamespace, mname, synopsis, wdl, delete_old=False)
                time.sleep(5)
        if config['methodRepoMethod']['methodVersion'] == 'latest':
            config['methodRepoMethod']['methodVersion'] = get_method_version(
                mnamespace,
                mname
            )
        identifier = '{}/{}'.format(config['namespace'], config['name'])
        key = 'config:' + identifier
        self.cache[key] = config # add full config object to cache
        try:
            self.list_configs()
        except APIException:
            # Don't worry too much if we can't populate cache here
            self.cache['configs'] = []
        self.dirty.add('configs')
        if identifier not in {'{}/{}'.format(c['namespace'], c['name']) for c in self.cache['configs']}:
            # New config
            # Append to configs cache entry since we know cache was just populated above
            self.cache['configs'].append(
                {
                    'methodRepoMethod': config['methodRepoMethod'],
                    'name': config['name'],
                    'namespace': config['namespace'],
                    'rootEntityType': config['rootEntityType']
                }
            )
        else:
            # update existing config
            self.cache['configs'] = [
                c for c in self.cache['configs']
                if '{}/{}'.format(c['namespace'], c['name']) != identifier
            ] + [
                {
                    'methodRepoMethod': config['methodRepoMethod'],
                    'name': config['name'],
                    'namespace': config['namespace'],
                    'rootEntityType': config['rootEntityType']
                }
            ]
        if config['methodRepoMethod']['methodVersion'] == -1:
            # Wdl was uploaded offline, so we really shouldn't upload this config
            # Just put it in the cache and make the user upload later
            warnings.warn("Not uploading configuration referencing offline WDL")
            return False
        if self.live:
            result = self._upload_config(config)
            if result:
                return result
            self.go_offline()
        self.pending_operations.append((
            None, # Dont worry about a getter. The config entry is exactly as it will appear in FC
            partial(self._upload_config, config),
            None
        ))
        self.pending_operations.append((
            'configs',
            None,
            self._configs_live_update
        ))
        return True # Default to True. If we get here, the config has been written into the cache

    update_configuration = update_config

    @property
    @_synchronized
    @_read_from_cache('entity_types')
    def entity_types(self):
        """
        Returns the different entity types present in the workspace
        Includes the count and column names of the entities
        """
        return self.tentative_json(
            # Because stupid name mangling
            getattr(firecloud.api, '__get')('/api/workspaces/{}/{}/entities'.format(
                self.namespace,
                self.workspace
            ))
        )

    @_synchronized
    @_read_from_cache(lambda self, etype, page_size=1000: "entities:{}".format(etype))
    def get_entities(self, etype, page_size=1000):
        """
        Paginated query replacing get_entities_tsv()
        """
        # This method override is just to gain the operator cache's
        # synchonization and cacheing
        return super().get_entities(etype, page_size).copy()

    @_synchronized
    def upload_entities(self, etype, df, index=True):
        """
        index: True if DataFrame index corresponds to ID
        """
        df = self.upload_entity_metadata(etype, df.copy())
        getter = partial(
            self.call_with_timeout,
            DEFAULT_LONG_TIMEOUT,
            super().self.get_entities,
            etype
        )
        key = 'entities:'+etype
        if index:
            if key not in self.cache or self.cache[key] is None:
                self.cache[key] = df
            else:
                self.cache[key] = self.cache[key].append(
                    df.loc[[k for k in df.index if k not in self.cache[key].index]]
                )
                self.cache[key].update(updates)
            self.dirty.add(key)
            if 'entity_types' not in self.cache:
                self.cache['entity_types'] = {}
            self.cache['entity_types'][etype] = {
                'attributeNames': [*self.cache[key].columns],
                'count': len(self.cache[key]),
                'idName': etype+'_id'
            }
            self.dirty.add('entity_types')
        if self.live:
            try:
                super().upload_entities(etype, df, index)
                self.cache[key] = getter()
                if key in self.dirty:
                    self.dirty.remove(key)
            except APIException:
                self.go_offline()
        if not self.live:
            self.pending_operations.append((
                key,
                super().upload_entities(etype, df, index),
                getter
            ))
            self.pending_operations.append((
                'entitiy_types',
                None,
                self._entities_live_update
            ))
        if not (self.live or index):
            warnings.warn("Entity may not be present in cache until next online sync")
        if self.live:
            try:
                # Try to trigger an update
                self.get_entities(etype)
            except APIException:
                pass

    @_synchronized
    def update_entity_attributes(self, etype, attrs):
        """
        Create or update entity attributes

        attrs:
          pd.DataFrame: update entities x attributes
          pd.Series:    update attribute (attrs.name)
                        for multiple entities (attrs.index)

          To update multiple attributes for a single entity, use:
            pd.DataFrame(attr_dict, index=[entity_name]))

          To update a single attribute for a single entity, use:
            pd.Series({entity_name:attr_value}, name=attr_name)
        """
        if not isinstance(attrs, pd.DataFrame):
            return super().update_entity_attributes(etype, attrs)
        attrs = self.upload_entity_metadata(etype, attrs.copy())
        getter = partial(
            self.call_with_timeout,
            DEFAULT_LONG_TIMEOUT,
            super().get_entities,
            etype
        )
        key = 'entities:'+etype
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = attrs
        else:
            self.cache[key] = self.cache[key].append(
                attrs.loc[[k for k in attrs.index if k not in self.cache[key].index]]
            )
            self.cache[key].update(attrs)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')
        if self.live:
            try:
                self._df_upload_translation_layer(
                    super().update_entity_attributes,
                    etype,
                    attrs
                )
                self.cache[key] = getter()
                if key in self.dirty:
                    self.dirty.remove(key)
            except APIException:
                self.go_offline()
        if not self.live:
            self.pending_operations.append((
                key,
                partial(
                    self._df_upload_translation_layer,
                    super().update_entity_attributes,
                    etype,
                    attrs
                ),
                getter
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self._entities_live_update
            ))
        else:
            try:
                self._get_entities_internal(etype)
            except APIException:
                pass

    @_synchronized
    def update_entity_set(self, etype, set_id, member_ids):
        """Create or update an entity set"""
        key = 'entities:%s_set' % etype
        updates = pd.DataFrame(index=[set_id], data={etype+'s':[[*member_ids]]})
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = updates
        else:
            self.cache[key] = self.cache[key].append(
                updates.loc[[k for k in updates.index if k not in self.cache[key].index]],
                sort=True
            )
            self.cache[key].update(updates)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype+'_set'] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_set_id'
        }
        self.dirty.add('entity_types')
        setter = partial(
            super().update_entity_set,
            etype,
            set_id,
            member_ids
        )
        getter = partial(
            self.call_with_timeout,
            DEFAULT_LONG_TIMEOUT,
            super().get_entities,
            etype+'_set'
        )
        if self.live:
            try:
                result = setter()
                if not result:
                    raise APIException("Update appeared to fail")
            except APIException:
                # One of the update routes failed
                self.go_offline()
        if not self.live:
            # offline. Add operations
            self.pending_operations.append((
                key,
                setter,
                getter
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self._entities_live_update
            ))
        else:
            try:
                self._get_entities_internal(etype+'_set')
            except APIException:
                pass

    def get_attributes(self):
        """Get workspace attributes"""
        ws = self.firecloud_workspace
        if 'workspace' in ws and 'attributes' in ws['workspace']:
            return ws['workspace']['attributes']
        self.fail()

    @_synchronized
    def update_attributes(self, attr_dict=None, **kwargs):
        """
        Set or update workspace attributes. Wrapper for API 'set' call
        Accepts a dictionary of attribute:value pairs and/or keyword arguments.
        Updates workspace attributes using the combination of the attr_dict and any keyword arguments
        Any values which reference valid filepaths will be uploaded to the workspace
        """
        # First handle upload
        if attr_dict is None:
            attr_dict = {}
        attr_dict.update(kwargs)
        base_path = 'gs://{}/workspace'.format(self.get_bucket_id())
        uploads = []
        for key, value in attr_dict.items():
            if isinstance(value, str) and os.path.isfile(value):
                path = '{}/{}'.format(base_path, os.path.basename(value))
                uploads.append(upload_to_blob(value, path))
                if self.initialize_hound() is not None:
                    self.hound.write_log_entry(
                        'upload',
                        "Uploading new file to workspace: {} ({})".format(
                            os.path.basename(value),
                            byteSize(os.path.getsize(value))
                        ),
                        entities=['workspace.{}'.format(key)]
                    )
                attr_dict[key] = path
        if len(uploads):
            [callback() for callback in status_bar.iter(uploads, prepend="Uploading attributes ")]

        # now cache and post to firecloud
        if 'workspace' in self.cache:
            if self.cache['workspace'] is None:
                self.cache['workspace'] = {
                    'workspace': {
                        'attributes': {k:v for k,v in attr_dict.items()}
                    }
                }
            else:
                self.cache['workspace']['workspace']['attributes'].update(attr_dict)
            self.dirty.add('workspace')
        if self.live:
            try:
                super().update_attributes(attr_dict)
            except AssertionError:
                self.go_offline()
        if not self.live:
            self.pending_operations.append((
                'workspace',
                partial(super().update_attributes, attr_dict),
                partial(self.call_with_timeout, DEFAULT_LONG_TIMEOUT, firecloud.api.get_workspace, self.namespace, self.workspace)
            ))
        else:
            try:
                self.get_attributes()
            except AssertionError:
                pass
        return attr_dict

    @_synchronized
    def upload_participants(self, participant_ids):
        """Upload a list of participants IDs"""
        # First update cache
        offline_df = pd.DataFrame(index=np.unique(participant_ids))
        offline_df.index.name = 'participant_id'
        key = 'entities:participant'
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = offline_df
        else:
            self.cache[key] = self.cache[key].append(
                offline_df.loc[[k for k in offline_df.index if k not in self.cache[key].index]]
            )
            self.cache[key].update(offline_df)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')
        if self.live:
            try:
                super().upload_participants(participant_ids)
            except APIException:
                self.go_offline()
        if not self.live:
            self.pending_operations.append((
                key,
                partial(super().upload_participants, participant_ids),
                partial(self._get_entities_internal, 'participant')
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self._entities_live_update
            ))
        else:
            try:
                self._get_entities_internal('participant')
            except:
                pass

    # =============
    # Properties
    # =============

    bucket_id = property(get_bucket_id)

    samples = property(LegacyWorkspaceManager.get_samples)

    sample_sets = property(LegacyWorkspaceManager.get_sample_sets)

    pairs = property(LegacyWorkspaceManager.get_pairs)

    pair_sets = property(LegacyWorkspaceManager.get_pair_sets)

    participants = property(LegacyWorkspaceManager.get_participants)

    participant_sets = property(LegacyWorkspaceManager.get_participant_sets)

    attributes = property(get_attributes)
    attributes.setter(update_attributes)

    configs = property(list_configs)
    configurations = configs

    # =============
    # Other upgrades
    # =============

    def upload_entity_metadata(self, etype, df):
        """
        Uploads a DataFrame of new entities.
        Any DataFrame cells which are valid filepaths will be uploaded to the bucket
        """
        base_path = "gs://{}/{}s".format(self.get_bucket_id(), etype)
        uploads = []

        def scan_row(row):
            # Note: due to something stupid in pandas apply, this gets called
            # twice on the first row
            # Not a huge deal though, because the paths will already be uploading
            # and will not represent valid filepaths anymore
            for i, value in enumerate(row):
                if isinstance(value, str) and os.path.isfile(value):
                    path = "{}/{}/{}".format(
                        base_path,
                        row.name,
                        os.path.basename(value)
                    )
                    uploads.append(upload_to_blob(value, path))
                    if self.initialize_hound() is not None:
                        self.hound.write_log_entry(
                            'upload',
                            "Uploading new file to workspace: {} ({})".format(
                                os.path.basename(value),
                                byteSize(os.path.getsize(value))
                            ),
                            entities=['{}/{}.{}'.format(
                                etype,
                                row.name,
                                i
                            )]
                        )
                    row.iloc[i] = path
            return row

        # Scan the df for uploadable filepaths
        staged_df = df.copy().apply(scan_row, axis='columns')

        if len(uploads):
            # Wait for callbacks
            [callback() for callback in status_bar.iter(uploads, prepend="Uploading {}s ".format(etype))]

        # Now upload as normal
        return staged_df

    def populate_cache(self):
        """
        Preloads all data from the FireCloud workspace into the in-memory cache.
        Use in advance of switching offline so that the WorkspaceManager can run in
        offline mode without issue.

        Call `WorkspaceManager.go_offline()` after this function to switch
        the workspace into offline mode
        """
        if self.live:
            self.sync()
        self.get_attributes()
        for etype in self.entity_types:
            self._get_entities_internal(etype)
        for config in self.configs:
            self.get_config(config)
        self.sync()

    @_synchronized
    def evaluate_expression(self, etype, entity, expression):
        """
        Evaluate entity expressions in the context of this workspace
        IE: "this.samples.sample_id" or "workspace.gtf"
        This function works in both online and offline modes, but the offline
        mode requires that the cache be almost completely populated
        """
        if self.live:
            with self.timeout(DEFAULT_LONG_TIMEOUT):
                result = self.tentative_json(
                    # NAME MANGLING!!!!
                    getattr(firecloud.api, '__post')(
                        'workspaces/%s/%s/entities/%s/%s/evaluate' % (
                            self.namespace,
                            self.workspace,
                            etype,
                            entity
                        ),
                        data=expression
                    ),
                    400,
                    404
                )
                if result is not None:
                    return result
        entity_types = self.entity_types
        evaluator = Evaluator(entity_types)
        for _etype, data in entity_types.items():
            evaluator.add_entities(
                _etype,
                self._get_entities_internal(_etype)
            )
        if 'workspace' in expression:
            evaluator.add_attributes(
                self.attributes
            )
        return [
            item for item in evaluator(etype, entity, expression)
            if isinstance(item, str) or not np.isnan(item)
        ]

    def validate_config(self, namespace, name=None):
        """
        validates a method configuration.
        The combination and values of namespace and name can be any
        input accepted by get_config

        This method ignores APIExceptions. It is not designed to cause failures,
        so you should not rely on it to check that a configuration actually exists.

        In the event of a failure or cache miss, this just returns an empty validation
        object, indicating a valid configuration
        """
        try:
            return self._validate_config_internal(namespace, name)
        except APIException:
            print(
                crayons.red("WARNING:", bold=False),
                "This operator was unable to validate the config",
                file=sys.stderr
            )
            print("Assuming valid inputs and returning blank validation object")
            return {
                'invalidInputs': {},
                'missingInputs': []
            }

    def preflight(self, config_name, entity, expression=None, etype=None):
        """
        Verifies submission configuration.
        This is just a quick check that the entity type, name, and expression map to
        one or more valid entities of the same type as the config's rootEntityType

        Returns a namedtuple.
        If tuple.result is False, tuple.reason will explain why preflight failed
        If tuple.result is True, you can access the following attributes:
        * (.config): The method configuration object
        * (.entity): The submission entity
        * (.etype): The submission entity type (inferred from the configuration, if not provided)
        * (.workflow_entities): The list of entities for each workflow (from evaluating the expression, if provided)
        * (.invalid): A dictonary of input-name : error, for any invalid inputs in the configuration
        """
        config = self.get_config(config_name)
        if (expression is not None) ^ (etype is not None and etype != config['rootEntityType']):
            return PreflightFailure(False, "expression and etype must BOTH be None or a string value")
        if etype is None:
            etype = config['rootEntityType']
        entities = self._get_entities_internal(etype)
        if entity not in entities.index:
            return PreflightFailure(
                False,
                "No such %s '%s' in this workspace. Check your entity and entity type" % (
                    etype,
                    entity
                )
            )

        workflow_entities = self.evaluate_expression(
            etype,
            entity,
            (expression if expression is not None else 'this')+'.%s_id' % config['rootEntityType']
        )
        if isinstance(workflow_entities, dict) and 'statusCode' in workflow_entities and workflow_entities['statusCode'] >= 400:
            return PreflightFailure(False, workflow_entities['message'] if 'message' in workflow_entities else repr(workflow_entities))
        elif not len(workflow_entities):
            return PreflightFailure(False, "Expression evaluates to 0 entities")

        template = config['inputs']

        invalid_inputs = self.validate_config(
            config['namespace'],
            config['name']
        )
        invalid_inputs = {**invalid_inputs['invalidInputs'], **{k:'N/A' for k in invalid_inputs['missingInputs']}}

        return PreflightSuccess(True, config, entity, etype, workflow_entities, invalid_inputs)


    def create_submission(self, config, entity, etype=None, expression=None, use_callcache=True):
        """
        Validates config parameters then creates a submission in Firecloud.
        Returns the submission id.
        This function does not use the Lapdog Engine, and instead submits a job
        through the FireCloud Rawls API. Use `WorkspaceManager.execute` to run
        jobs through the Lapdog Engine
        Accepts the following argument types for config:
        1) config = {config JSON object} (will be uploaded if not present on the workspace)
        2) config = "config namespace"
        3) config = "config name" (Only if name is unique in the workspace)
        4) config = "config namespace/config name"
        """
        if not self.live:
            warnings.warn(
                "WorkspaceManager {}/{} is currently offline."
                " Preflight will take place offline, but submission will still connect to Firecloud".format(
                    self.namespace,
                    self.workspace
                )
            )
        if isinstance(config, dict):
            # Auto upload a method configuration if it doesn't exist
            try:
                # Bonus: Quick pre-check that the config exists
                # If we intercept a ConfigNotFound, then upload the new configuration
                cfg = self.get_config(config['namespace'], config['name'])
                if 'inputs' in config and 'outputs' in config != cfg:
                    # Extra bonus: Maybe the full JSON we're holding is different than
                    # the config retrieved, meaning they are different versions
                    # It's best that we halt the procedure in that case, because we can't
                    # tell which should supercede which
                    print("User provided a full method configuration, which did not match the configuration already present on the workspace", file=sys.stderr)
                    print("Either upload the provided configuration, or use a different reference type to fetch the online version", file=sys.stderr)
                    raise ConfigNotUnique("Provided configuration did not match live version {}/{}".format(cfg['namespace'], cfg['name']))
            except ConfigNotFound:
                self.update_config(config)
        preflight = self.preflight(config, entity, expression, etype)
        if not preflight.result:
            raise ValueError(preflight.reason)

        if len(preflight.invalid_inputs):
            raise ValueError("The following inputs are invalid on this configuation: %s" % repr(list(preflight.invalid_inputs)))

        return super().create_submission(
            preflight.config['namespace'],
            preflight.config['name'],
            preflight.entity,
            preflight.etype,
            expression,
            use_callcache
        )

    @_synchronized
    def update_participant_entities(self, etype, target_set=None):
        """
        Attach entities (samples or pairs) to participants.
        If target_set is not None, only perform the update for samples/pairs
        belonging to the given set
        Parallelized update to run on 5 entities in parallel
        """
        if etype=='sample':
            df = self.samples[['participant']]
        elif etype=='pair':
            df = self.pairs[['participant']]
        else:
            raise ValueError('Entity type {} not supported'.format(etype))

        if target_set is not None:
            df = df.loc[
                df.index.intersection(
                    self._get_entities_internal(etype+'_set')[etype+'s'][target_set]
                )
            ]

        entities_dict = {k:g.index.values for k,g in df.groupby('participant')}
        participant_ids = np.unique(df['participant'])

        column = "{}s_{}".format(
            etype,
            (target_set if target_set is not None else '')
        )

        offline_df = pd.DataFrame(
            {column: [
                entities_dict[pid] for pid in participant_ids
            ]},
            index=participant_ids
        )
        offline_df.index.name = 'participant_id'

        # We can't just run update_participant_attributes, because if that goes through,
        # then we'll have broken attributes in Firecloud
        key = 'entities:participant'
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = offline_df
        else:
            self.cache[key] = self.cache[key].append(
                offline_df.loc[[k for k in offline_df.index if k not in self.cache[key].index]]
            )
            self.cache[key].update(offline_df)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')

        # Now attempt to update participant entities live
        if self.live:
            try:
                self._update_participant_entities_internal(
                    etype,
                    column,
                    participant_ids,
                    entities_dict
                )
                if key in self.dirty:
                    self.dirty.remove(key)
            except APIException:
                self.go_offline()
        if not self.live:
            self.pending_operations.append((
                key,
                partial(
                    self._update_participant_entities_internal,
                    etype,
                    column,
                    participant_ids,
                    entities_dict
                ),
                partial(
                    self._get_entities_internal,
                    'participant'
                )
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self._entities_live_update
            ))
        else:
            try:
                self._get_entities_internal('participant')
            except APIException:
                pass

    @property
    def acl(self):
        """
        Returns the current FireCloud ACL settings for the workspace
        """
        with self.timeout(DEFAULT_LONG_TIMEOUT):
            result = self.tentative_json(
                firecloud.api.get_workspace_acl(self.namespace, self.workspace)
            )
            if result is not None:
                return result['acl']
        raise APIException("Failed to get the workspace ACL")

    def update_acl(self, acl):
        """
        Sets the ACL. Provide a dictionary of email -> access level
        ex: {email: "WRITER"}
        """
        with self.timeout(DEFAULT_LONG_TIMEOUT):
            result = self.tentative_json(
                firecloud.api.update_workspace_acl(
                    self.namespace,
                    self.workspace,
                    [
                        {
                            'email': email,
                            'accessLevel': (
                                level['accessLevel'] if isinstance(level, dict)
                                else level
                            ).upper()
                        }
                        for email, level in acl.items()
                    ]
                )
            )
            if result is not None:
                if self.initialize_hound() is not None:
                    self.hound.update_workspace_meta(
                        "Updated ACL: {}".format(repr(acl))
                    )
                return result
        raise APIException("Failed to update the workspace ACL")

    def patch_attributes(self, cnamespace, configuration=None, *args, dry_run=False, entity='sample'):
        """
        Patch attributes for all samples/tasks that run successfully but were not written to database.
        This includes outputs from successful tasks in workflows that failed.
        Takes cnamespace and configuration arguments in the following formats:
        1) cnamespace = {config JSON object} (must be present online)
        2) cnamespace = "config namespace", configuration = "config name"
        3) cnamespace = "config name"
        4) cnamespace = "config namespace/config name"
        """
        if len(args):
            raise TypeError("dry_run and entity arguments are keyword-only")
        if isinstance(cnamespace, dict):
            # given a dictionary, we just need to check that it's been uploaded, or
            # else this won't make any sense
            # this also provides us with the name which we need to provide to patch_attributes
            configuration = self.get_config(cnamespace['namespace'], cnamespace['name'])['name']
        return super().patch_attributes(cnamespace, configuration, dry_run=dry_run, entity=entity)

    def display_status(self, configuration, entity='sample', filter_active=True):
        """
        Display summary of task statuses
        Takes cnamespace and configuration arguments in the following formats:
        1) cnamespace = {config JSON object} (must be present online)
        2) cnamespace = "config name"
        3) cnamespace = "config namespace/config name"
        """
        if isinstance(cnamespace, dict):
            # given a dictionary, we just need to check that it's been uploaded, or
            # else this won't make any sense
            # this also provides us with the name which we need to provide to patch_attributes
            configuration = self.get_config(cnamespace['namespace'], cnamespace['name'])['name']
        else:
            namespace, configuration = self.get_config(configuration, decode_only=True)
        return super().display_status(configuration, entity=entity, filter_active=filter_active)

    def list_submissions(self, config=None):
        """
        List all submissions from workspace
        If config is provided, it must be in one of the following formats:
        1) config = {config JSON object}
        2) config = "config name"
        3) config = "config namespace/config name"
        """
        submissions = firecloud.api.list_submissions(self.namespace, self.workspace)
        if submissions.status_code != 200:
            raise APIException("Failed to list submissions", submissions)
        submissions = submissions.json()

        if config is not None:
            ns, name = self.get_config(config, decode_only=True)
            submissions = [
                s for s in submissions if s['methodConfigurationName'] == name and s['methodConfigurationNamespace'] == ns
            ]

        return submissions

    def get_entity_status(self, etype, config):
        """Get status of latest submission for the entity type in the workspace"""

        # filter submissions by configuration
        submissions = self.list_submissions(config=config)

        # get status of last run submission
        entity_dict = {}

        @parallelize(5)
        def get_status(s):
            for attempt in range(3):
                with set_timeout(DEFAULT_SHORT_TIMEOUT):
                    if s['submissionEntity']['entityType']!=etype:
                        print('\rIncompatible submission entity type: {}'.format(
                            s['submissionEntity']['entityType']))
                        print('\rSkipping : '+ s['submissionId'])
                        return
                    try:
                        r = self.get_submission(s['submissionId'])
                        ts = datetime.timestamp(iso8601.parse_date(s['submissionDate']))
                        for w in r['workflows']:
                            entity_id = w['workflowEntity']['entityName']
                            if entity_id not in entity_dict or entity_dict[entity_id]['timestamp']<ts:
                                entity_dict[entity_id] = {
                                    'status':w['status'],
                                    'timestamp':ts,
                                    'submission_id':s['submissionId'],
                                    'configuration':s['methodConfigurationName']
                                }
                                if 'workflowId' in w:
                                    entity_dict[entity_id]['workflow_id'] = w['workflowId']
                                else:
                                    entity_dict[entity_id]['workflow_id'] = 'NA'
                    except (APIException, requests.ReadTimeout):
                        if attempt >= 2:
                            raise

        for k,s in enumerate(get_status(submissions), 1):
            print('\rFetching submission {}/{}'.format(k, len(submissions)), end='')

        print()
        status_df = pd.DataFrame(entity_dict).T
        status_df.index.name = etype+'_id'

        return status_df[['status', 'timestamp', 'workflow_id', 'submission_id', 'configuration']]

    def get_stats(self, status_df, workflow_name=None):
        """
        For a list of submissions, calculate time, preemptions, etc
        """
        # for successful jobs, get metadata and count attempts
        status_df = status_df[status_df['status']=='Succeeded'].copy()
        metadata_dict = {}

        @parallelize(5)
        def get_metadata(i, row):
            for attempt in range(3):
                with set_timeout(DEFAULT_SHORT_TIMEOUT):
                    try:
                        return i, self.get_workflow_metadata(row['submission_id'], row['workflow_id'])
                    except (APIException, requests.ReadTimeout):
                        if attempt >= 2:
                            raise

        for k,(i,data) in enumerate(get_metadata(*splice(status_df.iterrows())), 1):
            print('\rFetching metadata {}/{}'.format(k,status_df.shape[0]), end='')
            metadata_dict[i] = data
        print()

        # if workflow_name is None:
            # split output by workflow
        workflows = np.array([metadata_dict[k]['workflowName'] for k in metadata_dict])
        # else:
            # workflows = np.array([workflow_name])

        # get tasks for each workflow
        for w in np.unique(workflows):
            workflow_status_df = status_df[workflows==w]
            tasks = np.sort(list(metadata_dict[workflow_status_df.index[0]]['calls'].keys()))
            task_names = [t.rsplit('.')[-1] for t in tasks]

            task_dfs = {}
            for t in tasks:
                task_name = t.rsplit('.')[-1]
                task_dfs[task_name] = pd.DataFrame(index=workflow_status_df.index,
                    columns=[
                        'time_h',
                        'total_time_h',
                        'max_preempt_time_h',
                        'machine_type',
                        'attempts',
                        'start_time',
                        'est_cost',
                        'job_ids'])
                for i in workflow_status_df.index:
                    successes = {}
                    preemptions = []

                    if 'shardIndex' in metadata_dict[i]['calls'][t][0]:
                        scatter = True
                        for j in metadata_dict[i]['calls'][t]:
                            if j['shardIndex'] in successes:
                                preemptions.append(j)
                            # last shard (assume success follows preemptions)
                            successes[j['shardIndex']] = j
                    else:
                        scatter = False
                        successes[0] = metadata_dict[i]['calls'][t][-1]
                        preemptions = metadata_dict[i]['calls'][t][:-1]

                    task_dfs[task_name].loc[i, 'time_h'] = np.sum([workflow_time(j)/3600 for j in successes.values()])

                    # subtract time spent waiting for quota
                    quota_time = [e for m in successes.values() for e in m['executionEvents'] if e['description']=='waiting for quota']
                    quota_time = [(convert_time(q['endTime']) - convert_time(q['startTime']))/3600 for q in quota_time]
                    task_dfs[task_name].loc[i, 'time_h'] -= np.sum(quota_time)

                    total_time_h = [workflow_time(t_attempt)/3600 for t_attempt in metadata_dict[i]['calls'][t]]
                    task_dfs[task_name].loc[i, 'total_time_h'] = np.sum(total_time_h) - np.sum(quota_time)

                    if not np.any(['hit' in j['callCaching'] and j['callCaching']['hit'] for j in metadata_dict[i]['calls'][t]]):
                        was_preemptible = [j['preemptible'] for j in metadata_dict[i]['calls'][t]]
                        if len(preemptions)>0:
                            assert was_preemptible[0]
                            task_dfs[task_name].loc[i, 'max_preempt_time_h'] = np.max([workflow_time(t_attempt) for t_attempt in preemptions])/3600
                        task_dfs[task_name].loc[i, 'attempts'] = len(metadata_dict[i]['calls'][t])

                        task_dfs[task_name].loc[i, 'start_time'] = iso8601.parse_date(metadata_dict[i]['calls'][t][0]['start']).astimezone(pytz.timezone(self.timezone)).strftime('%H:%M')

                        machine_types = [j['jes']['machineType'].rsplit('/')[-1] for j in metadata_dict[i]['calls'][t]]
                        task_dfs[task_name].loc[i, 'machine_type'] = machine_types[-1]  # use last instance

                        task_dfs[task_name].loc[i, 'est_cost'] = np.sum([get_vm_cost(m,p)*h for h,m,p in zip(total_time_h, machine_types, was_preemptible)])

                        task_dfs[task_name].loc[i, 'job_ids'] = ','.join([j['jobId'] for j in successes.values()])

            # add overall cost
            workflow_status_df['est_cost'] = pd.concat([task_dfs[t.rsplit('.')[-1]]['est_cost'] for t in tasks], axis=1).sum(axis=1)
            workflow_status_df['time_h'] = [workflow_time(metadata_dict[i])/3600 for i in workflow_status_df.index]
            workflow_status_df['cpu_hours'] = pd.concat([task_dfs[t.rsplit('.')[-1]]['total_time_h'] * task_dfs[t.rsplit('.')[-1]]['machine_type'].apply(lambda i: int(i.rsplit('-',1)[-1]) if (pd.notnull(i) and '-small' not in i and '-micro' not in i) else 1) for t in tasks], axis=1).sum(axis=1)
            workflow_status_df['start_time'] = [iso8601.parse_date(metadata_dict[i]['start']).astimezone(pytz.timezone(self.timezone)).strftime('%H:%M') for i in workflow_status_df.index]

        return workflow_status_df, task_dfs

    def attribute_provenance(self):
        """
        Returns a provenance dictionary for the workspace
        { attributeName: Provenance }
        If Hound cannot find an entry for a given attribute, it will be recorded
        as None in the dictionary
        """
        self.initialize_hound()
        return {
            attr: self._check_conflicting_value(
                value,
                self.hound.latest(
                    self.hound.get_entries(os.path.join('hound', 'workspace', attr)),
                    'updates'
                )
            )
            for attr, value in self.attributes.items()
        }

    def entity_provenance(self, etype, df):
        """
        Maps the given df/series to contain provenance objects
        etype: The entity type
        df: pd.DataFrame or pd.Series of entity metadata.
        For a series: Name must be entity_id, index must contain desired attributes
        For a dataframe: index must contain entity_ids, columns must contain desired attributes
        Return value will be of the same type and shape as the input df
        If hound cannot find an entry for a given attribute, it will be set to
        None in the output
        """
        self.initialize_hound()
        if isinstance(df, pd.DataFrame):
            return df.copy().apply(
                lambda row: pd.Series(data=self._build_provenance_series_internal(etype, row), index=row.index.copy(), name=row.name),
                axis='columns'
            )
        elif isinstance(df, pd.Series):
            return pd.Series(
                data=self._build_provenance_series_internal(etype, df),
                index=df.index.copy(),
                name=df.name
            )
        raise TypeError("df must be a pd.Series or pd.DataFrame")

    def synchronize_hound_records(self):
        """
        Slow operation
        Updates conflicting or missing attributes in hound to match their current
        live value.
        Updates workspace attributes and all entity attributes
        """
        with self.initialize_hound().with_reason("<Automated> Synchronizing hound with Firecloud"):
            self.hound.write_log_entry(
                'other',
                "Starting database sync with FireCloud"
            )
            print("Checking workspace attributes")
            updates = {
                attr: self.attributes[attr]
                for attr, prov in self.attribute_provenance().items()
                if prov is None or isinstance(prov, ProvenanceConflict)
            }
            if len(updates):
                print("Updating", len(updates), "hound attribute records")
                self.hound.update_workspace_meta(
                    "Updating {} workspace attributes: {}".format(
                        len(updates),
                        ', '.join(updates)
                    )
                )
                for k,v in updates.items():
                    self.hound.update_workspace_attribute(k, v)
            for etype in self.entity_types:
                print("Checking", etype, "attributes")
                live_df = self._get_entities_internal(etype)
                for eid, data in self.entity_provenance(etype, live_df).iterrows():
                    updates = {
                        attr for attr, val in data.items()
                        if val is None or isinstance(val, ProvenanceConflict)
                    }
                    if len(updates):
                        print("Updating", len(updates), "attributes for", etype, eid)
                        self.hound.update_entity_meta(
                            etype,
                            eid,
                            "Updating {} attributes: {}".format(
                                len(updates),
                                ', '.join(updates)
                            )
                        )
                        for attr in updates:
                            self.hound.update_entity_attribute(
                                etype,
                                eid,
                                attr,
                                live_df[attr][eid]
                            )
