from __future__ import print_function
import pandas as pd
import numpy as np
import subprocess
import os
import sys
import io
from collections import defaultdict
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
from agutil import status_bar
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
# Operator Cache Helper Decorators
# =============

def _synchronized(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            with self.lock:
                return func(self, *args, **kwargs)
        return wrapper

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
    """

    def decorator(func):

        @wraps(func)
        def call(self, *args, **kwargs):
            if callable(key):
                _key = key(self, *args, **kwargs)
            else:
                _key = key
            if self.live:
                with self.timeout(_key):
                    result = func(self, *args, **kwargs)
                if result is not None:
                    self.cache[_key] = result
            if _key in self.cache and self.cache[_key] is not None:
                return self.cache[_key]
            self.fail(message)

        return call

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

    def __init__(self, namespace, workspace=None, timezone='America/New_York'):
        super().__init__(namespace, workspace, timezone)
        self.pending_operations = []
        self.cache = {}
        self.dirty = set()
        self.live = True
        self.lock = RLock()
        self._last_result = None

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
                print('Successfully updated configuration {}/{}'.format(json_body['namespace'], json_body['name']))
                return True
            else:
                raise APIException("Failed to update existing configuration", r)
        return False # Shouldn't be a control path to get here

    @_synchronized
    def _entities_live_update(self):
        """
        Internal Use. Property: Fetches the list of entity types, but forces
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
                translations = True
        return translations

    def _get_entities_internal(self, etype):
        return getattr(self, 'get_{}s'.format(etype))()

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

    @_synchronized
    @_read_from_cache(lambda self, cnamespace, config=None: 'config:{}'.format(self.fetch_config_name(cnamespace, config)))
    def get_config(self, cnamespace, config=None):
        """
        Get workspace configuration JSON
        Accepts the following formats:
        1) cnamespace = namespace; config = name
        2) cnamespace = "namespace/name"; config = None
        3) cnamespace = name; config = None
        """
        canonical = self.fetch_config_name(cnamespace, config)
        cnamespace, config = canonical.split('/')
        return self.tentative_json(firecloud.api.get_workspace_config(
            self.namespace,
            self.workspace,
            cnamespace,
            config
        ))

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
        mnamespace = confog['methodRepoMethod']['methodNamespace']
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
                update_method(mnamespace, mname, synopsis, wdl)
                time.sleep(5)
        if config['methodRepoMethod']['methodVersion'] == 'latest':
            config['methodRepoMethod']['methodVersion'] = get_method_version(
                mnamespace,
                mname
            )
        identifier = '{}/{}'.format(config['namespace'], config['name'])
        key = 'config:' + identifier
        self.cache[key] = config # add full config object to cache
        if identifier not in {'{}/{}'.format(c['namespace'], c['name']) for c in self.configs}:
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
        self.pending.append((
            None,
            partial(self._upload_config, config),
            None
        ))
        return False

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
            except ValueError: # Also accepts APIExceptions
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
            except ValueError:
                self.go_offline()
        if not self.live:
            self.pending.append((
                key,
                partial(
                    self._df_upload_translation_layer,
                    super().update_entity_attributes,
                    etype,
                    attrs
                ),
                getter
            ))
            self.pending.append((
                'entity_types',
                None,
                lambda x=None:self._entities_live_update
            ))
        if self.live:
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
            self.pending.append((
                key,
                setter,
                getter
            ))
            self.pending.append((
                'entity_types',
                None,
                lambda x=None:self._entities_live_update
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
        else:
            self.pending.append((
                'workspace',
                partial(super().update_attributes, attr_dict),
                partial(self.call_with_timeout, DEFAULT_LONG_TIMEOUT, firecloud.api.get_workspace, self.namespace, self.workspace)
            ))
        if self.live:
            try:
                self.get_attributes()
            except AssertionError:
                pass
        return attr_dict

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
                    row.iloc[i] = path
            return row

        # Scan the df for uploadable filepaths
        staged_df = df.copy().apply(scan_row, axis='columns')

        if len(uploads):
            # Wait for callbacks
            [callback() for callback in status_bar.iter(uploads, prepend="Uploading {}s ".format(etype))]

        # Now upload as normal
        return staged_df

    def fetch_config_name(self, config_slug, config_name=None):
        """
        Fetches a configuration by the provided slug (method_config_namespace/method_config_name).
        If the slug is just the config name, this returns a config
        with a matching name IFF the name is unique. If another config
        exists with the same name, this will fail.
        If the slug is a full slug (namespace/name) this will always return
        a matching config (slug uniqueness is enforced by firecloud)
        """
        if config_name is not None:
            config_slug = '{}/{}'.format(config_slug, config_name)
        if isinstance(config_slug, dict):
            config_slug = '{}/{}'.format(config_slug['namespace'], config_slug['name'])
        configs = self.list_configs()
        candidates = [] # For configs just matching name
        for config in configs:
            if config_slug == '%s/%s' % (config['namespace'], config['name']):
                return '{}/{}'.format(config['namespace'], config['name'])
            elif config_slug == config['name']:
                candidates.append(config)
        if len(candidates) == 1:
            return '{}/{}'.format(candidates[0]['namespace'], candidates[0]['name'])
        elif len(candidates) > 1:
            raise ConfigNotUnique('%d configs matching name "%s". Use a full config slug' % (len(candidates), config_slug))
        raise ConfigNotFound('No such config "%s"' % config_slug)

    def populate_cache(self):
        """
        Preloads all data from the FireCloud workspace into the in-memory cache.
        Use in advance of switching offline so that the WorkspaceManager can run in
        offline mode without issue.

        Call `WorkspaceManager.operator.go_offline()` after this function to switch
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
