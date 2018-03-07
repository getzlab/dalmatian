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
import argparse
import multiprocessing as mp
from .__about__ import __version__

# Collection of high-level wrapper functions for FireCloud API

class FirecloudError(AssertionError):
    pass

def check_response_status(response, code):
    if response.status_code != code:
        try:
            data = response.json()
            if 'message' in data:
                raise FirecloudError(
                    "Status %d : %s" % (
                        response.status_code,
                        data['message'].strip()
                    )
                )
        except FirecloudError:
            raise
        except:
            pass
        raise FirecloudError(
            "Status %d : %s" % (
                response.status_code,
                response.text.strip()
            )
        )
    #for chaining
    return response

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

def gs_list_bucket_files(bucket_id):
    """Get list of all files stored in bucket"""
    s = subprocess.check_output('gsutil ls gs://{}/**'.format(bucket_id), shell=True)
    return s.decode().strip().split('\n')


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


def gs_exists(file_list_s):
    """
    Check whether files exist

    file_list_s: pd.Series
    """
    status_s = pd.Series(False, index=file_list_s.index, name='file_exists')
    for k,(i,p) in enumerate(zip(file_list_s.index, file_list_s)):
        print('\rChecking {}/{} files'.format(k+1, len(file_list_s)), end='')
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
        for k,r in enumerate(pool.imap(get_md5hash, [i for i in file_list_s])):
            print('\rCalculating MD5 hash for file {}/{}'.format(k+1,len(file_list_s)), end='')
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
        for k,j in enumerate(job_id):
            print('\rFetching metadata ({}/{})'.format(k+1,len(job_id)), end='')
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
def list_methods(namespace=None):
    """
    List all methods in the repository
    """
    r = firecloud.api.list_repository_methods()
    check_response_status(r,200)
    r = r.json()

    if namespace is not None:
        r = [m for m in r if m['namespace']==namespace]

    return pd.DataFrame(r).sort_values(['name', 'snapshotId'])


def get_method(namespace, name):
    """
    Get all available versions of a method from the repository
    """
    r = firecloud.api.list_repository_methods()
    check_response_status(r,200)
    r = r.json()
    r = [m for m in r if m['name']==name and m['namespace']==namespace]
    return r


def get_method_version(namespace, name):
    """
    Get latest method version
    """
    r = get_method(namespace, name)
    return np.max([m['snapshotId'] for m in r])


def list_configs(namespace=None):
    """
    List all configurations in the repository
    """
    r = firecloud.api.list_repository_configs()
    check_response_status(r,200)
    r = r.json()

    if namespace is not None:
        r = [m for m in r if m['namespace']==namespace]

    return pd.DataFrame(r).sort_values(['name', 'snapshotId'])


def get_config(namespace, name):
    """
    Get all versions of a configuration from the repository
    """
    r = firecloud.api.list_repository_configs()
    check_response_status(r,200)
    r = r.json()
    r = [m for m in r if m['name']==name and m['namespace']==namespace]
    return r


def get_config_version(namespace, name):
    """
    Get latest config version
    """
    r = get_config(namespace, name)
    return np.max([m['snapshotId'] for m in r])


def print_methods(namespace):
    """
    Print all methods in a namespace
    """
    r = firecloud.api.list_repository_methods()
    check_response_status(r,200)
    r = r.json()
    r = [m for m in r if m['namespace']==namespace]
    methods = np.unique([m['name'] for m in r])
    for k in methods:
        print('{}: {}'.format(k, np.max([m['snapshotId'] for m in r if m['name']==k])))


def print_configs(namespace):
    """
    Print all configurations in a namespace
    """
    r = firecloud.api.list_repository_configs()
    check_response_status(r,200)
    r = r.json()
    r = [m for m in r if m['namespace']==namespace]
    configs = np.unique([m['name'] for m in r])
    for k in configs:
        print('{}: {}'.format(k, np.max([m['snapshotId'] for m in r if m['name']==k])))


def get_wdl(method_namespace, method_name, snapshot_id=None):
    """
    Get WDL from repository
    """
    if snapshot_id is None:
        snapshot_id = get_method_version(method_namespace, method_name)

    r = firecloud.api.get_repository_method(method_namespace, method_name, snapshot_id)
    check_response_status(r,200)
    return r.json()['payload']


def compare_wdls(mnamespace1, mname1, mnamespace2, mname2):
    """
    Compare WDLs from two methods
    """
    v1 = get_method_version(mnamespace1, mname1)
    v2 = get_method_version(mnamespace2, mname2)
    wdl1 = get_wdl(mnamespace1, mname1, v1)
    wdl2 = get_wdl(mnamespace2, mname2, v2)
    print('Comparing:')
    print('< {}:{}.v{}'.format(mnamespace1, mname1, v1))
    print('> {}:{}.v{}'.format(mnamespace2, mname2, v2))
    cmd = 'diff <(echo \''+wdl1+'\') <(echo \''+wdl2+'\')'
    d = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, executable='/bin/bash')
    print(d.stdout.decode())


def compare_wdl(mnamespace, mname, wdl_path):
    """
    Compare method WDL to file
    """
    v = get_method_version(mnamespace, mname)
    wdl1 = get_wdl(mnamespace, mname, v)
    with open(wdl_path) as f:
        wdl2 = f.read()
    print('Comparing:')
    print('< {}'.format(wdl_path))
    print('> {}:{}.v{}'.format(mnamespace, mname, v))
    cmd = 'diff <(echo \''+wdl1+'\') <(echo \''+wdl2+'\')'
    d = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, executable='/bin/bash')
    print(d.stdout.decode())


def redact_method(method_namespace, method_name, mode='outdated'):
    """
    Redact method in repository

    mode: 'outdated', 'latest', 'all'
    """
    assert mode in ['outdated', 'latest', 'all']
    r = firecloud.api.list_repository_methods()
    check_response_status(r,200)
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
        check_response_status(r,200)


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
    try:
        check_response_status(r,201)
        print("Successfully pushed {}/{}. New SnapshotID: {}".format(namespace, method, r.json()['snapshotId']))
    except FirecloudError as e:
        raise ValueError("Update failed.") from e

    if public:
        print('  * setting public read access.')
        r = firecloud.api.update_repository_method_acl(namespace, method, r.json()['snapshotId'], [{'role': 'READER', 'user': 'public'}])

    # delete old version
    if old_version is not None and delete_old:
        r = firecloud.api.delete_repository_method(namespace, method, old_version)
        check_response_status(r,200)
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
