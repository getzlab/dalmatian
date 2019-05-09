# Author: Francois Aguet
from __future__ import print_function
import os, sys, json
import subprocess
from datetime import datetime
from collections import Iterable
import pandas as pd
import numpy as np
import firecloud.api
import iso8601
import binascii, base64
import argparse
import multiprocessing as mp
from functools import lru_cache
from agutil.parallel import parallelize2
from google.cloud import storage
import requests

# Collection of high-level wrapper functions for FireCloud API

class APIException(ValueError):
    """
    Class for generic FireCloud errors
    """
    def __init__(self, *args, **kwargs):
        if len(args)==2 and isinstance(args[0], str) and isinstance(args[1], requests.Response):
            return super().__init__(
                "{}: ({}) : {}".format(args[0], args[1].status_code, args[1].text),
                **kwargs
            )
        elif len(args)==1 and isinstance(args[0], requests.Response):
            return super().__init__(
                "({}) : {}".format(args[0].status_code, args[0].text),
                **kwargs
            )
        return super().__init__(
            *args,
            **kwargs
        )

def assert_status_code(response, condition, message=None):
    """
    Shorthand for checking status code
    response: a requests.Response object
    condition: boolean indicating if the response is acceptable or not
    message: (optional) string message indicating what failed
    """
    if not condition:
        if message is not None:
            raise APIException(message, response)
        raise APIException(response)

# =============
# Reference utilities for decoding method and config references
# =============
class ConfigNotFound(KeyError):
    pass

class ConfigNotUnique(KeyError):
    pass

class MethodNotFound(KeyError):
    pass

class MethodNotUnique(KeyError):
    pass

#------------------------------------------------------------------------------
#  Helper functions for processing timestamps
#------------------------------------------------------------------------------
def convert_time(x):
    return datetime.timestamp(iso8601.parse_date(x))


def workflow_time(workflow):
    """
    Convert API output to timestamp difference
    """
    if 'end' in workflow:
        return convert_time(workflow['end']) - convert_time(workflow['start'])
    else:
        return np.NaN


#------------------------------------------------------------------------------
#  Wrapper functions for gsutil calls
#------------------------------------------------------------------------------
def gs_list_bucket_files(bucket_id, path=None, ext=None):
    """Get list of all files stored in bucket"""
    if path is None:
        s = subprocess.check_output('gsutil ls gs://{}/**'.format(bucket_id), shell=True)
    else:
        s = subprocess.check_output(os.path.join('gsutil ls gs://{}'.format(bucket_id), path, '**'), shell=True)
    s = s.decode().strip().split('\n')
    if ext is not None:
        s = [i for i in s if i.endswith(ext)]
    return s


def gs_delete(file_list, chunk_size=500):
    """Delete list of files (paths starting with gs://)"""
    # number of calls is limited by command line size limit
    n = int(np.ceil(len(file_list)/chunk_size))
    for i in range(n):
        x = file_list[chunk_size*i:chunk_size*(i+1)]
        cmd = 'echo -e "{}" | gsutil -m rm -I'.format('\n'.join(x))
        subprocess.call(cmd, shell=True)


def gs_copy(file_list, dest_dir, chunk_size=500):
    """Copy list of files (paths starting with gs://)"""
    n = int(np.ceil(len(file_list)/chunk_size))
    for i in range(n):
        x = file_list[chunk_size*i:chunk_size*(i+1)]
        cmd = 'echo -e "{}" | gsutil -m cp -I {}'.format('\n'.join(x), dest_dir)
        subprocess.check_call(cmd, shell=True)


def gs_move(file_list, dest_dir, chunk_size=500):
    """Move list of files (paths starting with gs://)"""
    n = int(np.ceil(len(file_list)/chunk_size))
    for i in range(n):
        x = file_list[chunk_size*i:chunk_size*(i+1)]
        cmd = 'echo -e "{}" | gsutil -m mv -I {}'.format('\n'.join(x), dest_dir)
        subprocess.check_call(cmd, shell=True)


def _gsutil_cp_wrapper(args):
    """gsutil cp wrapper for gs_par_upload"""
    source_path = args[0]
    dest_path = args[1]
    filename = os.path.basename(dest_path)
    print('Starting copy: '+filename, flush=True)
    st = datetime.now()
    subprocess.check_call('gsutil cp {} {}'.format(source_path, dest_path), shell=True)
    et = datetime.now()
    time_min = (et-st).total_seconds()/60
    size_gb = os.path.getsize(source_path)/1024**3
    print('Finished copy: {}. size: {:.2f} GB, time elapsed: {:.2f} min.'.format(filename, size_gb, time_min), flush=True)
    # return time_min, size_gb


def gs_copy_par(source_paths, dest_paths, num_threads=10):
    """Parallel gsutil cp"""
    if len(source_paths) != len(dest_paths):
        raise ValueError("list of source and destination paths must be of equal length")
    res_list = []
    print('Starting cp pool ({} threads)'.format(num_threads), flush=True)
    with mp.Pool(processes=int(num_threads)) as pool:
        for i in range(len(source_paths)):
            res = pool.map_async(_gsutil_cp_wrapper, ((source_paths[i], dest_paths[i], ),))
            res_list.append(res)
        pool.close()
        pool.join()
    print('Finished upload.', flush=True)
    # res = np.array([i.get()[0] for i in res_list])
    # return res[:,0], res[:,1]


def gs_exists(file_list_s):
    """
    Check whether files exist

    file_list_s: pd.Series
    """
    status_s = pd.Series(False, index=file_list_s.index, name='file_exists')
    for k,(i,p) in enumerate(zip(file_list_s.index, file_list_s), 1):
        print('\rChecking {}/{} files'.format(k, len(file_list_s)), end='')
        try:
            s = subprocess.check_output('gsutil -q stat {}'.format(p), shell=True)
            status_s[i] = True
        except subprocess.CalledProcessError as e:
            s = e.stdout.decode()
            print('{}: {}'.format(i, s))
    return status_s


def gs_size(file_list_s):
    """
    Get file sizes (in bytes)

    file_list_s: pd.Series
    """
    prefix = os.path.commonprefix(file_list_s.tolist())
    s = subprocess.check_output('gsutil ls -l {}**'.format(prefix), shell=True)
    gs_sizes = s.decode().strip().split('\n')[:-1]
    gs_sizes = pd.Series([np.int64(i.split()[0]) for i in gs_sizes],
        index=[i.split()[-1] for i in gs_sizes])
    gs_sizes.index.name = 'path'
    return pd.Series(gs_sizes[file_list_s].values, index=file_list_s.index, name='size_bytes')


def _parse_stat_entry(se):
    """parse output from 'gsutil stat'"""
    se = se.split('\n')
    filename = se[0].strip(':')
    md5 = [i for i in se if 'md5' in i][0].split()[-1]
    md5 = binascii.hexlify(base64.b64decode(md5)).decode()
    sample_id = os.path.basename(filename).split('.')[0]
    return sample_id, md5


def get_md5_hashes(wildcard_path):
    s = subprocess.check_output('gsutil stat '+wildcard_path, shell=True).decode().strip()
    s = ['gs://'+i for i in s.split('gs://')[1:]]
    sample_ids, md5 = np.array([_parse_stat_entry(i) for i in s]).T
    return pd.Series(md5, index=sample_ids, name='md5').sort_index()


def get_md5hash(file_path):
    """Calculate MD5 hash using gsutil or md5sum, depending on location"""
    if file_path.startswith('gs://'):
        s = subprocess.check_output('gsutil hash -m -h '+file_path, shell=True).decode()
        s = s.strip().split('\n')
        s = [i for i in s if 'md5' in i][0]
        return s.split()[-1]
    else:
        return subprocess.check_output('md5sum '+file_path, shell=True).decode().split()[0]


def get_md5hashes(file_list_s, num_threads=10):
    """Parallelized get_md5hash()"""
    md5_hashes = []
    with mp.Pool(processes=num_threads) as pool:
        for k,r in enumerate(pool.imap(get_md5hash, [i for i in file_list_s]), 1):
            print('\rCalculating MD5 hash for file {}/{}'.format(k,len(file_list_s)), end='')
            md5_hashes.append(r)
    return md5_hashes


#------------------------------------------------------------------------------
# Functions for parsing Google metadata
#------------------------------------------------------------------------------
def get_google_metadata(job_id):
    """
    jobid: operations ID
    """
    if isinstance(job_id, str):
        s = subprocess.check_output('gcloud alpha genomics operations describe '+job_id+' --format json', shell=True)
        return json.loads(s.decode())
    elif isinstance(job_id, Iterable):
        json_list = []
        for k,j in enumerate(job_id, 1):
            print('\rFetching metadata ({}/{})'.format(k,len(job_id)), end='')
            s = subprocess.check_output('gcloud alpha genomics operations describe '+j+' --format json', shell=True)
            json_list.append(json.loads(s.decode()))
        return json_list


def parse_google_stats(json_list):
    """
    Parse job start and end times, machine type, and preemption status from Google metadata
    """
    df = pd.DataFrame(index=[j['name'] for j in json_list], columns=['time_h', 'machine_type', 'preemptible', 'preempted'])
    for j in json_list:
        event_dict = {k['description']:convert_time(k['startTime']) for k in j['metadata']['events'] if 'copied' not in k}
        event_times = [convert_time(k['startTime']) for k in j['metadata']['events'] if 'copied' not in k]
        time_delta = np.max(event_times) - np.min(event_times)
        # if 'ok' in event_dict:
        #     time_delta = convert_time(event_dict['ok']) - convert_time(event_dict['start'])
        # elif 'start-shutdown' in event_dict:
        #     time_delta = convert_time(event_dict['start-shutdown']) - convert_time(event_dict['start'])
        # else:
        #     raise ValueError('unknown event')
        mt = j['metadata']['runtimeMetadata']['computeEngine']['machineType'].split('/')[-1]
        p = j['metadata']['request']['ephemeralPipeline']['resources']['preemptible']
        # j[0]['metadata']['request']['pipelineArgs']['resources']['preemptible']
        df.loc[j['name'], ['time_h', 'machine_type', 'preemptible', 'preempted']] = [time_delta/3600, mt, p, 'ok' not in event_dict]
    return df


def calculate_google_cost(jobid, jobid_lookup_df):
    """
    Calculate cost
    """
    r = jobid_lookup_df.loc[jobid]
    if r['preempted'] and r['time_h']<1/6:
        return 0
    else:
        return r['time_h']*get_vm_cost(r['machine_type'], preemptible=r['preemptible'])


#------------------------------------------------------------------------------
# Functions for managing methods and configuration in the repository
#------------------------------------------------------------------------------
def list_workspaces():
    """List all workspaces"""
    r = firecloud.api.list_workspaces()
    if r.status_code==200:
        return r.json()
    else:
        print(r.text)

def fetch_method(reference, name=None, version=None, *args, decode_only=False):
    """
    Fetches a single method JSON object given a variety of possible inputs:
    1) reference = {method configuration JSON}
    2) reference = {method JSON}
    3) reference = "method namespace", name = "method name", (optional) verison = "method version"
    4) reference = "method namespace/method name"
    5) reference = "method namespace/method name/method version"
    """
    if len(args):
        raise TypeError("decode_only is a keyword-only argument")
    if isinstance(reference, dict):
        # it's a config or method object
        if "name" in reference and "snapshotId" in reference:
            if decode_only:
                return (reference['namespace'], reference['name'], reference['snapshotId'])
            if 'managers' in reference and 'public' in reference and 'payload' in reference:
                return reference #because it's already a method
            else:
                return fetch_method(reference['namespace'], reference['name'], reference['snapshotId'])
        if "methodRepoMethod" in reference:
            reference = reference['methodRepoMethod']
        version = (
            reference['methodVersion']
            if reference['methodVersion'] != 'latest'
            else None
        )
        namespace = reference['methodNamespace']
        name = reference['methodName']
    elif name is None:
        # Just one argument
        data = reference.split('/')
        if len(data) == 1:
            # Just a name
            methods = list_methods().query('name == "{}"'.format(data[0]))
            try:
                methods = methods.loc[methods.groupby('namespace').idxmax('snapshotId').snapshotId]
            except AttributeError as e:
                raise MethodNotFound("No method by name '{}'".format(data[0])) from e
            if len(methods) > 1:
                raise MethodNotUnique("Found more than one method with name '{}'. Must provide a namespace".format(data[0]))
            if len(methods) == 0:
                raise MethodNotFound("No method by name '{}'".format(data[0]))
            methods = methods.iloc[0]
            namespace = methods['namespace']
            name = methods['name']
            # Even though a version was not provided by the user
            # we found the latest snapshot as a byproduct of the search
            version = methods['snapshotId']
        elif len(data) >= 2:
            # namespace / name
            namespace = data[0]
            name = data[1]
            if len(data) == 3:
                version = data[2]
            elif len(data) > 3:
                raise TypeError("Unable to determine argument configuration from {}".format(reference))
    else:
        namespace = reference
    if decode_only:
        return (namespace, name, version)
    if version is None:
        version = _get_method_version_internal(namespace, name)
    response = firecloud.api.get_repository_method(namespace, name, version)
    if response.status_code == 404:
        raise MethodNotFound("No such method {}/{}/{}".format(namespace, name, version))
    elif response.status_code >= 400:
        raise APIException("Failed to get method", response)
    return response.json()

def list_methods(namespace=None, name=None):
    """List all methods in the repository"""
    r = firecloud.api.list_repository_methods()
    if r.status_code != 200:
        raise APIException(r)
    r = r.json()

    if namespace is not None:
        r = [m for m in r if m['namespace']==namespace]
    if name is not None:
        r = [m for m in r if m['name']==name]

    return pd.DataFrame(r).sort_values(['name', 'snapshotId'])


def _get_method_internal(namespace, name):
    """Get all available versions of a method from the repository"""
    r = firecloud.api.list_repository_methods()
    if r.status_code != 200:
        raise APIException(r)
    r = r.json()
    r = [m for m in r if m['name']==name and m['namespace']==namespace]
    return r

def get_method(namespace, name=None):
    """
    Get all available versions of a method from the repository
    Takes arguments in any of the following formats:
    1) namespace = {method configuration JSON}
    2) namespace = {method JSON}
    3) namespace = "method namespace", name = "method name"
    4) namespace = "method namespace/method name"
    """
    # Decode only because we're going to list methods, so we just need it to parse the arguments
    namespace, name, version = fetch_method(namespace, name, decode_only=True)
    return _get_method_internal(namespace, name)



def _get_method_version_internal(namespace, name):
    """Get latest method version"""
    r = _get_method_internal(namespace, name)
    if len(r) == 0:
        raise MethodNotFound("No such method {}/{}".format(namespace, name))
    return int(np.max([m['snapshotId'] for m in r]))

def get_method_version(namespace, name=None):
    """
    Gets the latest method version
    Takes arguments in any of the following formats:
    1) namespace = {method configuration JSON}
    2) namespace = {method JSON}
    3) namespace = "method namespace", name = "method name"
    4) namespace = "method namespace/method name"
    """
    # Decode only because we're going to list methods, so we just need it to parse the arguments
    namespace, name, version = fetch_method(namespace, name, decode_only=True)
    return _get_method_version_internal(namespace, name)


def fetch_config(reference, name=None, version=None, *args, decode_only=False):
    """
    fetches a config json given a variety of reference formats
    Note: This method only fetches public configurations. You must use a workspacemanager
    to fetch configurations from a workspace
    1) reference = {method configuration JSON}
    2) reference = "config namespace", name = "config name", (optional) verison = "config version"
    3) reference = "config name"
    4) reference = "config namespace/config name"
    5) reference = "config namespace/config name/config version"
    """
    if len(args):
        raise TypeError("decode_only is a keyword-only argument")
    if isinstance(reference, dict):
        namespace = reference['namespace']
        name = reference['name']
        if 'methodConfigVersion' in reference:
            version = reference['methodConfigVersion']
    elif name is None:
        # Just one argument
        data = reference.split('/')
        if len(data) == 1:
            # Just a name
            configs = list_configs().query('name == "{}"'.format(data[0]))
            try:
                configs = configs.loc[configs.groupby('namespace').idxmax('snapshotId').snapshotId]
            except AttributeError as e:
                raise ConfigNotFound("No config by name '{}'".format(data[0])) from e
            if len(configs) > 1:
                raise ConfigNotUnique("Found more than one config with name '{}'. Must provide a namespace".format(data[0]))
            if len(configs) == 0:
                raise ConfigNotFound("No config by name '{}'".format(data[0]))
            configs = configs.iloc[0]
            namespace = configs['namespace']
            name = configs['name']
            # Even though a version was not provided by the user
            # we found the latest snapshot as a byproduct of the search
            version = configs['snapshotId']
        elif len(data) >= 2:
            # namespace / name
            namespace = data[0]
            name = data[1]
            if len(data) == 3:
                version = data[2]
            elif len(data) > 3:
                raise TypeError("Unable to determine argument configuration from {}".format(reference))
    else:
        namespace = reference
    if decode_only:
        return (namespace, name, version)
    if version is None:
        version = _get_config_version_internal(namespace, name)
    response = firecloud.api.get_repository_config(namespace, name, version)
    if response.status_code == 404:
        raise ConfigNotFound("No such method {}/{}/{}".format(namespace, name, version))
    elif response.status_code >= 400:
        raise APIException("Failed to get method", response)
    return response.json()


def list_configs(namespace=None):
    """List all configurations in the repository"""
    r = firecloud.api.list_repository_configs()
    if r.status_code != 200:
        raise APIException(r)
    r = r.json()

    if namespace is not None:
        r = [m for m in r if m['namespace']==namespace]

    return pd.DataFrame(r).sort_values(['name', 'snapshotId'])


def _get_config_internal(namespace, name):
    """Get all versions of a configuration from the repository"""
    r = firecloud.api.list_repository_configs()
    if r.status_code != 200:
        raise APIException("Failed to list repository configurations", r)
    r = r.json()
    if len(r) == 0:
        raise ConfigNotFound("No visible configurations")
    r = [m for m in r if m['name']==name and m['namespace']==namespace]
    return r

def get_config(namespace, name=None):
    """
    Get all versions of a configuration from the repository
    Takes arguments in any of the following formats:
    1) namespace = {method configuration JSON}
    2) namespace = "config namespace", name = "config name"
    3) namespace = "config name"
    4) namespace = "config namespace/config name"
    """
    namespace, name, version = fetch_config(namespace, name, decode_only=True)
    return _get_config_internal(namespace, name)

def _get_config_version_internal(namespace, name):
    """Get latest configuration version"""
    r = _get_config_internal(namespace, name)
    if len(r) == 0:
        raise ConfigNotFound("No such config {}/{}".format(namespace, name))
    return int(np.max([m['snapshotId'] for m in r]))

def get_config_version(namespace, name=None):
    """
    Get latest configuration version
    Takes arguments in any of the following formats:
    1) namespace = {method configuration JSON}
    2) namespace = "config namespace", name = "config name"
    3) namespace = "config name"
    4) namespace = "config namespace/config name"
    """
    namespace, name, version = fetch_config(namespace, name, decode_only=True)
    return _get_config_version_internal(namespace, name)

def get_config_json(namespace, name=None, snapshot_id=None):
    """
    Get configuration JSON from repository
    1) namespace = "config namespace", name = "config name", (optional) verison = "config version"
    2) namespace = "config name"
    3) namespace = "config namespace/config name"
    4) namespace = "config namespace/config name/config version"
    """
    return fetch_config(namespace, name, snapshot_id)


def get_config_template(namespace, method=None, version=None):
    """
    Get configuration template for method
    Takes arguments in any of the following formats:
    1) namespace = {method configuration JSON}
    2) namespace = {method JSON}
    3) namespace = "method namespace", method = "method name", (optional) verison = "method version"
    4) namespace = "method namespace/method name"
    5) namespace = "method namespace/method name/method version"
    """
    method = fetch_method(namespace, method, version) # decode user inputs into a method object
    r = firecloud.api.get_config_template(
        method['namespace'],
        method['name'],
        method['snapshotId']
    )
    if r.status_code != 200:
        raise APIException("Failed to get method template", r)
    return r.json()


def autofill_config_template(namespace, method=None, *args, version=None, workflow_inputs=None):
    """
    Fill configuration template for workflow based on dependent tasks
    Namespace and method arguments can be in any of the following formats
    1) namespace = {method configuration JSON}
    2) namespace = {method JSON}
    3) namespace = "method namespace", method = "method name", (optional) verison = "method version"
    4) namespace = "method namespace/method name"
    5) namespace = "method namespace/method name/method version"
    workflow_inputs must always be a keyword argument
    """
    if len(args):
        raise TypeError("version and workflow_inputs are keyword-only arguments")
    if workflow_inputs is None:
        raise TypeError("workflow_inputs is required but must be passed as a keyword argument")
    method = fetch_method(namespace, method, version)
    attr = get_config_template(method) # Now we're passing the full method object

    # get dependent configurations
    wdl = method['payload']
    wdls = [i.split()[1].replace('"','') for i in wdl.split('\n') if i.startswith('import')]
    assert [i.startswith('https://api.firecloud.org/ga4gh/v1/tools/') for i in wdls]
    methods = [i.split(':')[-1].split('/')[0] for i in wdls]
    # attempt to get configurations for all methods
    configs = {}
    for k,m in enumerate(methods,1):
        print('\r  * importing configuration {}/{}'.format(k, len(methods)), end='')
        try:
            configs[m] = get_config_json(method['namespace'], m+'_cfg')
        except:
            raise ValueError('No configuration found for {} ({} not found)'.format(m, m+'_cfg'))
    print()

    # parse out inputs/outputs
    inputs = {}
    for c in configs:
        for i in configs[c]['inputs']:
            inputs['.'.join(i.split('.')[1:])] = configs[c]['inputs'][i]

    outputs = {}
    for c in configs:
        for i in configs[c]['outputs']:
            outputs['.'.join(i.split('.')[1:])] = configs[c]['outputs'][i]

    # populate template
    for i in attr['inputs']:
        k = '.'.join(i.split('.')[1:])
        if k in inputs:
            attr['inputs'][i] = inputs[k]

    for i in attr['outputs']:
        k = '.'.join(i.split('.')[1:])
        if k in outputs:
            attr['outputs'][i] = outputs[k]

    # add workflow inputs
    workflow_name = np.unique([i.split('.')[0] for i in attr['inputs']])
    assert len(workflow_name)==1
    workflow_name = workflow_name[0]
    for i in workflow_inputs:
        j = workflow_name+'.'+i
        assert j in attr['inputs']
        attr['inputs'][j] = workflow_inputs[i]

    return attr


def print_methods(namespace):
    """Print all methods in a namespace"""
    r = firecloud.api.list_repository_methods()
    if r.status_code != 200:
        raise APIException(r)
    r = r.json()
    r = [m for m in r if m['namespace']==namespace]
    methods = np.unique([m['name'] for m in r])
    for k in methods:
        print('{}: {}'.format(k, np.max([m['snapshotId'] for m in r if m['name']==k])))


def print_configs(namespace):
    """Print all configurations in a namespace"""
    r = firecloud.api.list_repository_configs()
    if r.status_code != 200:
        raise APIException(r)
    r = r.json()
    r = [m for m in r if m['namespace']==namespace]
    configs = np.unique([m['name'] for m in r])
    for k in configs:
        print('{}: {}'.format(k, np.max([m['snapshotId'] for m in r if m['name']==k])))


def get_wdl(method_namespace, method_name=None, snapshot_id=None):
    """
    Get WDL from repository
    Takes arguments in any of the following formats:
    1) method_namespace = {method configuration JSON}
    2) method_namespace = {method JSON}
    3) method_namespace = "method namespace", method_name = "method name", (optional) snapshot_id = "method version"
    4) method_namespace = "method namespace/method name"
    5) method_namespace = "method namespace/method name/method version"
    """
    return fetch_method(method_namespace, method_name, snapshot_id)['payload']


def compare_wdls(mnamespace1, mname1, mnamespace2, mname2):
    """Compare WDLs from two methods"""
    # (internal method is faster)
    v1 = _get_method_version_internal(mnamespace1, mname1)
    v2 = _get_method_version_internal(mnamespace2, mname2)
    wdl1 = get_wdl(mnamespace1, mname1, v1)
    wdl2 = get_wdl(mnamespace2, mname2, v2)
    print('Comparing:')
    print('< {}:{}.v{}'.format(mnamespace1, mname1, v1))
    print('> {}:{}.v{}'.format(mnamespace2, mname2, v2))
    cmd = 'diff <(echo \''+wdl1+'\') <(echo \''+wdl2+'\')'
    d = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, executable='/bin/bash')
    print(d.stdout.decode())


def compare_wdl(mnamespace, mname, wdl_path):
    """Compare method WDL to file"""
    # (internal method is faster)
    v = _get_method_version_internal(mnamespace, mname)
    wdl1 = get_wdl(mnamespace, mname, v)
    with open(wdl_path) as f:
        wdl2 = f.read()
    print('Comparing:')
    print('< {}'.format(wdl_path))
    print('> {}:{}.v{}'.format(mnamespace, mname, v))
    cmd = 'diff <(echo \''+wdl1+'\') <(echo \''+wdl2+'\')'
    d = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, executable='/bin/bash')
    print(d.stdout.decode())


def redact_method(method_namespace, method_name=None, *args, mode='outdated'):
    """
    Redact method in repository

    mode: 'outdated', 'latest', 'all'
    """
    if len(args):
        raise TypeError("mode is a keyword-only argument")
    assert mode in ['outdated', 'latest', 'all']
    # just decode user inputs
    method_namespace, method_name, version = fetch_method(method_namespace, method_name, decode_only=True)
    r = firecloud.api.list_repository_methods()
    if r.status_code != 200:
        raise APIException(r)
    r = r.json()
    r = [m for m in r if m['name']==method_name and m['namespace']==method_namespace]
    versions = np.sort([m['snapshotId'] for m in r])
    print('Versions: {}'.format(', '.join(map(str, versions))))
    if mode == 'outdated':
        versions = versions[:-1]
    elif mode == 'latest':
        versions = versions[-1:]
    for i in versions:
        print('  * deleting version {}'.format(i))
        r = firecloud.api.delete_repository_method(method_namespace, method_name, i)
        if r.status_code != 200:
            raise APIException(r)


def update_method(namespace, method, synopsis, wdl_file, public=False, delete_old=True):
    """
    push new version, then redact previous version(s)
    """
    # check whether prior version exists
    r = get_method(namespace, method)
    old_version = None
    if r:
        old_version = np.max([m['snapshotId'] for m in r])
        print('Method {}/{} exists. SnapshotID: {}'.format(
            namespace, method, old_version))

    # push new version
    r = firecloud.api.update_repository_method(namespace, method, synopsis, wdl_file)
    if r.status_code==201:
        print("Successfully pushed {}/{}. New SnapshotID: {}".format(namespace, method, r.json()['snapshotId']))
    else:
        raise APIException('Update failed', r)

    if public:
        print('  * setting public read access.')
        r = firecloud.api.update_repository_method_acl(namespace, method, r.json()['snapshotId'], [{'role': 'READER', 'user': 'public'}])

    # delete old version
    if old_version is not None and delete_old:
        r = firecloud.api.delete_repository_method(namespace, method, old_version)
        if r.status_code != 200:
            raise APIException("Delete failed", r)
        print("Successfully deleted SnapshotID {}.".format(old_version))



#------------------------------------------------------------------------------
# VM costs
#------------------------------------------------------------------------------
def get_vm_cost(machine_type, preemptible=True):
    """
    Cost per hour
    """
    preemptible_dict = {
        'n1-standard-1': 0.0100,  # 3.75 GB
        'n1-standard-2': 0.0200,  # 7.5 GB
        'n1-standard-4': 0.0400,  # 15  GB
        'n1-standard-8': 0.0800,  # 30  GB
        'n1-standard-16':0.1600,  # 60  GB
        'n1-standard-32':0.3200,  # 120 GB
        'n1-standard-64':0.6400,  # 240 GB
        'n1-highmem-2':  0.0250,  # 13  GB
        'n1-highmem-4':  0.0500,  # 26  GB
        'n1-highmem-8':  0.1000,  # 52  GB
        'n1-highmem-16': 0.2000,  # 104 GB
        'n1-highmem-32': 0.4000,  # 208 GB
        'n1-highmem-64': 0.8000,  # 416 GB
        'n1-highcpu-2':  0.0150,  # 1.80 GB
        'n1-highcpu-4':  0.0300,  # 3.60 GB
        'n1-highcpu-8':  0.0600,  # 7.20 GB
        'n1-highcpu-16': 0.1200,  # 14.40 GB
        'n1-highcpu-32': 0.2400,  # 28.80 GB
        'n1-highcpu-64': 0.4800,  # 57.6 GB
        'f1-micro':      0.0035,  # 0.6 GB
        'g1-small':      0.0070,  # 1.7 GB
    }

    standard_dict = {
        'n1-standard-1': 0.0475,
        'n1-standard-2': 0.0950,
        'n1-standard-4': 0.1900,
        'n1-standard-8': 0.3800,
        'n1-standard-16': 0.7600,
        'n1-standard-32': 1.5200,
        'n1-standard-64': 3.0400,
        'n1-highmem-2':  0.1184,
        'n1-highmem-4':  0.2368,
        'n1-highmem-8':  0.4736,
        'n1-highmem-16': 0.9472,
        'n1-highmem-32': 1.8944,
        'n1-highmem-64': 3.7888,
        'n1-highcpu-2':  0.0709,
        'n1-highcpu-4':  0.1418,
        'n1-highcpu-8':  0.2836,
        'n1-highcpu-16': 0.5672,
        'n1-highcpu-32': 1.1344,
        'n1-highcpu-64': 2.2688,
        'f1-micro':      0.0076,
        'g1-small':      0.0257,
    }

    if preemptible:
        return preemptible_dict[machine_type]
    else:
        return standard_dict[machine_type]

# Lapdog Utilities

# Using the LRU cache allows us to use one common Client
# throughout the entire program
@lru_cache()
def _getblob_client(credentials):
    return storage.Client(credentials=credentials)

def getblob(gs_path, credentials=None, user_project=None):
    """
    Return a GCP "blob" object for a given gs:// path.
    Path must start with "gs://".
    By default, uses the current application default credentials for authentication.
    Alternatively, you may provide a `google.auth.Credentials` object.
    When interacting with a requester pays bucket, you must set `user_project` to
    be the name of the project to bill for the data transfer fees
    """
    if not gs_path.startswith('gs://'):
        raise ValueError("Getblob path must start with gs://")
    bucket_id = gs_path[5:].split('/')[0]
    bucket_path = '/'.join(gs_path[5:].split('/')[1:])
    return storage.Blob(
        bucket_path,
        _getblob_client(credentials).bucket(bucket_id, user_project)
    )

def strict_getblob(gs_path, credentials=None, user_project=None):
    """
    Like getblob(), but raises a FileNotFound error if the path does
    not already exist
    """
    blob = getblob(gs_path, credentials, user_project)
    if not blob.exists():
        raise FileNotFoundError("No such blob: "+gs_path)
    return blob

def copyblob(src, dest, credentials=None, user_project=None):
    """
    Copy blob from src -> dest
    src and dest may either be a gs:// path or a premade blob object
    """
    if isinstance(src, str):
        src = getblob(src, credentials, user_project)
    if isinstance(dest, str):
        dest = getblob(dest, credentials, user_project)
    src.bucket.copy_blob(src, dest.bucket, dest.name)
    return dest.exists()

def moveblob(src, dest, credentials=None, user_project=None):
    """
    Move blob from src -> dest
    src and dest may either be a gs:// path or a premade blob object
    """
    if isinstance(src, str):
        src = getblob(src, credentials, user_project)
    if isinstance(dest, str):
        dest = getblob(dest, credentials, user_project)
    if copyblob(src, dest):
        src.delete()
        return True
    return False

@parallelize2(5)
def upload_to_blob(source, dest, allow_composite=True):
    """
    Uploads {source} to google cloud.
    Result google cloud path is {dest}.
    If the file to upload is larger than 4Gib, the file will be uploaded via
    a composite upload.

    WARNING: You MUST set allow_composite to False if you are uploading to a nearline
    or coldline bucket. The composite upload will incur large fees from deleting
    the temporary objects

    This function starts the upload on a background thread and returns a callable
    object which can be used to wait for the upload to complete. Calling the object
    blocks until the upload finishes, and will raise any exceptions encountered
    by the background thread. This function allows up to 5 concurrent uploads, beyond
    which workers will be queued until there is an empty execution slot.
    """
    if isinstance(dest, str):
        dest = getblob(dest)
    if allow_composite and os.path.isfile(source) and os.path.getsize(source) >= 2865470566: #2.7gib
        dest.chunk_size = 104857600 # ~100mb
    dest.upload_from_filename(source)
    return dest

def main(argv=None):
    if not argv:
        argv = sys.argv

    # Initialize core parser
    descrip  = 'dalmatian [OPTIONS] CMD [arg ...]\n'
    descrip += '       dalmatian [ --help | -v | --version ]'
    parser = argparse.ArgumentParser(description='dalmatian: the loyal companion to FISS. Only currently useful in a REPL.')

    parser.add_argument("-v", "--version", action='version', version=__version__)

    args = parser.parse_args()

    sys.exit()

if __name__ == '__main__':
    sys.exit(main())
