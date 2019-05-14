
from __future__ import print_function
import pandas as pd
import numpy as np
import subprocess
import os
import io
from collections import defaultdict
import firecloud.api
from firecloud import fiss
import iso8601
import pytz
from datetime import datetime
from .core import *
from hound import HoundClient
import warnings
from contextlib import ExitStack
import traceback

#------------------------------------------------------------------------------
#  Extension of firecloud.api functionality using the rawls (internal) API
#------------------------------------------------------------------------------
def _batch_update_entities(namespace, workspace, json_body):
    """ Batch update entity attributes in a workspace.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        json_body (list(dict)):
        [{
            "name": "string",
            "entityType": "string",
            "operations": (list(dict))
        }]

        operations:
        [{
          "op": "AddUpdateAttribute",
          "attributeName": "string",
          "addUpdateAttribute": "string"
        },
        {
          "op": "RemoveAttribute",
          "attributeName": "string"
        },
        {
          "op": "AddListMember",
          "attributeListName": "string",
          "newMember": "string"
        },
        {
          "op": "RemoveListMember",
          "attributeListName": "string",
          "removeMember": "string"
        },
        {
          "op": "CreateAttributeEntityReferenceList",
          "attributeListName": "string"
        },
        {
          "op": "CreateAttributeValueList",
          "attributeListName": "string"
        }]

    Swagger:
        https://rawls.dsde-prod.broadinstitute.org/#!/entities/batch_update_entities
    """
    headers = firecloud.api._fiss_agent_header({"Content-type":  "application/json"})
    uri = "{0}workspaces/{1}/{2}/entities/batchUpdate".format(
        'https://rawls.dsde-prod.broadinstitute.org/api/', namespace, workspace)

    return firecloud.api.__post(uri, headers=headers, json=json_body)

class LegacyWorkspaceManager(object):
    def __init__(self, namespace, workspace=None, timezone='America/New_York', credentials=None, user_project=None):
        if workspace is None:
            self.namespace, self.workspace = namespace.split('/')
        else:
            self.namespace = namespace
            self.workspace = workspace
        self.timezone  = timezone
        self.__credentials = credentials
        self.__user_project = user_project
        self.hound = None

    def initialize_hound(self, credentials=None, user_project=None):
        """
        Initializes the HoundClient for the workspace, if it is None
        credentials: (optional) google.cloud.auth.Credentials to use when
        interacting with bucket, if not using default credentials
        user_project: (optional) name of project to bill, if bucket is requester pays
        """
        if credentials is None:
            credentials = self.__credentials
        if user_project is None:
            user_project = self.__user_project
        if self.hound is not None and credentials is None and user_project is None:
            return self.hound
        try:
            self.hound = HoundClient(self.get_bucket_id(), credentials=credentials, user_project=user_project)
        except:
            traceback.print_exc()
        return self.hound

    def create_workspace(self, wm=None):
        """Create the workspace, or clone from another"""
        if wm is None:
            r = firecloud.api.create_workspace(self.namespace, self.workspace)
            if r.status_code==201:
                print('Workspace {}/{} successfully created.'.format(self.namespace, self.workspace))
                if self.initialize_hound() is not None:
                        self.hound.update_workspace_meta("Created Empty Workspace")
                return True
            else:
                raise APIException("Failed to create Workspace", r)
        else:  # clone workspace
            r = firecloud.api.clone_workspace(wm.namespace, wm.workspace, self.namespace, self.workspace)
            if r.status_code == 201:
                print('Workspace {}/{} successfully cloned from {}/{}.'.format(
                    self.namespace, self.workspace, wm.namespace, wm.workspace))
                if self.initialize_hound() is not None:
                        self.hound.update_workspace_meta("Cloned workspace from {}/{}".format(wm.namespace, wm.workspace))
                return True
            else:
                raise APIException("Failed to clone Workspace", r)
        return False


    def delete_workspace(self):
        """Delete the workspace"""
        r = firecloud.api.delete_workspace(self.namespace, self.workspace)
        if r.status_code == 202:
            print('Workspace {}/{} successfully deleted.'.format(self.namespace, self.workspace))
            print('  * '+r.json()['message'])
        else:
            raise APIException("Failed to delete workspace", r)


    def get_bucket_id(self):
        """Get the GCS bucket ID associated with the workspace"""
        r = firecloud.api.get_workspace(self.namespace, self.workspace)
        if r.status_code != 200:
            raise APIException("Unable to get workspace metadata", r)
        r = r.json()
        bucket_id = r['workspace']['bucketName']
        return bucket_id


    def upload_entities(self, etype, df, index=True):
        """
        index: True if DataFrame index corresponds to ID
        """
        buf = io.StringIO()
        df.to_csv(buf, sep='\t', index=index)
        s = firecloud.api.upload_entities(self.namespace, self.workspace, buf.getvalue())
        buf.close()
        et = etype.replace('_set', ' set')
        if s.status_code == 200:
            if 'set' in etype:
                if index:
                    sets = df.index
                else:
                    sets = df[df.columns[0]]
                print('Successfully imported {} {}s:'.format(len(np.unique(sets)), et))
                for s in np.unique(sets):
                    print('  * {} ({} {}s)'.format(s, np.sum(sets==s), et.replace(' set','')))
            else:
                print('Successfully imported {} {}s.'.format(df.shape[0], et))
            if self.initialize_hound() is not None:
                idx = df.index if index else (df[etype+'_id'] if etype+'_id' in df.columns else None)
                self.hound.write_log_entry(
                    "upload",
                    "Uploaded {} {}s".format(
                        len(df),
                        etype
                    ),
                    entities=(
                        [os.path.join(etype, eid) for eid in idx]
                        if idx is not None
                        else None
                    )
                )

                if idx is not None:
                    for eid, data in df.iterrows():
                        self.hound.update_entity_meta(
                            etype,
                            eid,
                            "User uploaded new entity"
                        )
                        for attr, val in data.items():
                            self.hound.update_entity_attribute(
                                etype,
                                eid,
                                attr,
                                val
                            )
        else:
            raise APIException('{} import failed.'.format(et.capitalize()), s)


    def upload_participants(self, participant_ids):
        """Upload a list of participants IDs"""
        participant_df = pd.DataFrame(
            data=np.unique(participant_ids),
            columns=['entity:participant_id']
        )
        self.upload_entities('participant', participant_df, index=False)
        if self.initialize_hound() is not None:
            self.hound.write_log_entry(
                "upload",
                "Uploaded {} participants".format(
                    len(participant_df),
                ),
                entities=[
                    os.path.join('participant', eid) for eid in df['entity:participant_id']
                ]
            )


    def upload_samples(self, df, participant_df=None, add_participant_samples=False):
        """
        Upload samples stored in a pandas DataFrame, and populate the required
        participant, sample, and sample_set attributes

        df columns: sample_id (index), participant[_id], {sample_set_id,}, additional attributes
        """
        assert df.index.name=='sample_id' and ('participant' in df.columns or 'participant_id' in df.columns)
        if 'participant' in df.columns:
            participant_col = 'participant'
        else:
            participant_col = 'participant_id'

        # 1) upload participant IDs (without additional attributes)
        if participant_df is None:
            self.upload_participants(np.unique(df[participant_col]))
        else:
            assert (participant_df.index.name=='entity:participant_id'
                or participant_df.columns[0]=='entity:participant_id')
            self.upload_entities('participant', participant_df,
                index=participant_df.index.name=='entity:participant_id')

        # 2) upload samples
        sample_df = df[df.columns[df.columns!='sample_set_id']].copy()
        sample_df.index.name = 'entity:sample_id'
        self.upload_entities('sample', sample_df)

        # 3) upload sample sets
        if 'sample_set_id' in df.columns:
            set_df = pd.DataFrame(data=sample_df.index.values, index=df['sample_set_id'], columns=['sample_id'])
            set_df.index.name = 'membership:sample_set_id'
            self.upload_entities('sample_set', set_df)

        if add_participant_samples:
            # 4) add participant.samples_
            print('  * The FireCloud data model currently does not provide participant.samples\n',
                  '    Adding "participant.samples_" as an explicit attribute.', sep='')
            self.update_participant_entities('sample')


    def update_participant_entities(self, etype):
        """Attach entities (samples or pairs) to participants"""

        # get etype -> participant mapping
        if etype=='sample':
            df = self.get_samples()[['participant']]
        elif etype=='pair':
            df = self.get_pairs()[['participant']]
        else:
            raise ValueError('Entity type {} not supported'.format(etype))

        entitites_dict = {k:g.index.values for k,g in df.groupby('participant')}
        participant_ids = np.unique(df['participant'])

        for n,k in enumerate(participant_ids, 1):
            print('\r    Updating {}s for participant {}/{}'.format(etype, n, len(participant_ids)), end='')
            attr_dict = {
                "{}s_".format(etype): {
                    "itemsType": "EntityReference",
                    "items": [{"entityType": etype, "entityName": i} for i in entitites_dict[k]]
                }
            }
            attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
            r = firecloud.api.update_entity(self.namespace, self.workspace, 'participant', k, attrs)
            if self.initialize_hound() is not None:
                self.hound.update_entity_attribute(
                    'participant',
                    k,
                    '{}s_'.format(etype),
                    list(entitites_dict[k]),
                    "<Automated> Populating attribute from entity references"
                )
            if r.status_code != 200:
                raise APIException(r)
        print('\n    Finished attaching {}s to {} participants'.format(etype, len(participant_ids)))
        if self.initialize_hound() is not None:
            for pid in participant_ids:
                self.hound.update_entity_meta(
                    'participant',
                    pid,
                    "Updated {}s_ membership".format(etype)
                )


    def update_participant_samples(self):
        """Attach samples to participants"""
        self.update_participant_entities('sample')


    def update_participant_samples_and_pairs(self):
        """Attach samples and pairs to participants"""
        self.update_participant_entities('sample')
        self.update_participant_entities('pair')


    def make_pairs(self, sample_set_id=None):
        """
        Make all possible pairs from participants (all or a specified set)
        Requires sample_type sample level annotation 'Normal' or 'Tumor'
        """
        # get data from sample set or all samples
        if sample_set_id is None:
            df = self.get_samples()
        else:
            df = self.get_sample_attributes_in_set(sample_set_id)

        normal_samples = list(df[df['sample_type'] == 'Normal'].index)
        participants = list(df['participant'])
        # generate pairs
        pair_tumors = list()
        pair_normals = list()
        pair_ids = list()
        participant_pair_ids = list()
        for s in normal_samples:
            patient = df['participant'][df.index == s][0]
            idx = [i for i, x in enumerate(participants) if x == patient]
            patient_sample_tsv = df.iloc[idx]
            for i, row in patient_sample_tsv.iterrows():
                if not row['sample_type'] == 'Normal':
                    pair_tumors.append(i)
                    pair_normals.append(s)
                    pair_ids.append(i + '-' + s)
                    participant_pair_ids.append(patient)
        pair_df = pd.DataFrame(
            np.array([pair_tumors, pair_normals, participant_pair_ids]).T,
            columns=['case_sample', 'control_sample', 'participant'],
            index=pair_ids
        )
        pair_df.index.name = 'entity:pair_id'
        self.upload_entities('pair', pair_df)


    def update_sample_attributes(self, attrs, sample_id=None):
        """Set or update attributes in attrs (pd.Series or pd.DataFrame)"""
        if sample_id is not None and isinstance(attrs, dict):
            attrs = pd.DataFrame(attrs, index=[sample_id])
        self.update_entity_attributes('sample', attrs)


    def update_sample_set_attributes(self, sample_set_id, attrs):
        """
        Set or update attributes in attrs (pd.Series or pd.DataFrame)
        """
        self.update_entity_attributes('sample_set', attrs)


    def delete_sample_set_attributes(self, sample_set_id, attrs):
        """Delete attributes"""
        self.delete_entity_attributes(self, 'sample_set', sample_set_id, attrs)


    def update_attributes(self, attr_dict):
        """
        Set or update workspace attributes. Wrapper for API 'set' call
        """
        # attrs must be list:
        attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
        r = firecloud.api.update_workspace_attributes(self.namespace, self.workspace, attrs)
        if self.initialize_hound() is not None:
            self.hound.update_workspace_meta(
                "Updating {} workspace attributes: {}".format(
                    len(attr_dict),
                    ', '.join(attr_dict)
                )
            )
            for k,v in attr_dict.items():
                self.hound.update_workspace_attribute(k, v)
        if r.status_code != 200:
            raise APIException(r)
        print('Successfully updated workspace attributes in {}/{}'.format(self.namespace, self.workspace))


    def get_attributes(self):
        """Get workspace attributes"""
        r = firecloud.api.get_workspace(self.namespace, self.workspace)
        if r.status_code != 200:
            raise APIException(r)
        attr = r.json()['workspace']['attributes']
        for k in [k for k in attr if 'library:' in k]:
            attr.pop(k)
        return attr


    def get_sample_attributes_in_set(self, set):
        """Get sample attributes of samples in a set"""
        samples = self.get_sample_sets().loc[set]['samples']
        all_samples = self.get_samples().index
        idx = np.zeros(len(all_samples), dtype=bool)
        for s in samples:
            idx[all_samples == s] = True
        return self.get_samples()[idx]


    def get_submission_status(self, config=None, filter_active=True, show_namespaces=False):
        """
        Get status of all submissions in the workspace (replicates UI Monitor)
        """
        # filter submissions by configuration
        submissions = self.list_submissions(config=config)

        statuses = ['Succeeded', 'Running', 'Failed', 'Aborted', 'Aborting', 'Submitted', 'Queued']
        df = []
        for s in submissions:
            d = {
                'entity_id':s['submissionEntity']['entityName'],
                'status':s['status'],
                'submission_id':s['submissionId'],
                'date':iso8601.parse_date(s['submissionDate']).strftime('%H:%M:%S %m/%d/%Y'),
            }
            d.update({i:s['workflowStatuses'].get(i,0) for i in statuses})
            if show_namespaces:
                d['configuration'] = s['methodConfigurationNamespace']+'/'+s['methodConfigurationName']
            else:
                d['configuration'] = s['methodConfigurationName']
            df.append(d)
        df = pd.DataFrame(df)
        df.set_index('entity_id', inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df = df[['configuration', 'status']+statuses+['date', 'submission_id']]
        if filter_active:
            df = df[(df['Running']!=0) | (df['Submitted']!=0)]
        return df.sort_values('date')[::-1]


    def get_workflow_metadata(self, submission_id, workflow_id):
        """Get metadata JSON for a specific workflow"""
        metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace,
            submission_id, workflow_id)
        if metadata.status_code != 200:
            raise APIException(metadata)
        return metadata.json()


    def get_submission(self, submission_id):
        """Get submission metadata"""
        r = firecloud.api.get_submission(self.namespace, self.workspace, submission_id)
        if r.status_code != 200:
            raise APIException(r)
        return r.json()


    def list_submissions(self, config=None):
        """List all submissions from workspace"""
        submissions = firecloud.api.list_submissions(self.namespace, self.workspace)
        if submissions.status_code != 200:
            raise APIException(submissions)
        submissions = submissions.json()

        if config is not None:
            submissions = [s for s in submissions if config in s['methodConfigurationName']]

        return submissions


    def print_scatter_status(self, submission_id, workflow_id=None):
        """Print status for a specific scatter job"""
        if workflow_id is None:
            s = self.get_submission(submission_id)
            for w in s['workflows']:
                if 'workflowId' in w and w['status']!='Succeeded':
                    print('\n{} ({}):'.format(w['workflowEntity']['entityName'], w['workflowId']))
                    self.print_scatter_status(submission_id, workflow_id=w['workflowId'])
        else:
            metadata = self.get_workflow_metadata(submission_id, workflow_id)
            if metadata['status']!='Succeeded':
                for task_name in metadata['calls']:
                    if np.all(['shardIndex' in i for i in metadata['calls'][task_name]]):
                        print('Submission status ({}): {}'.format(task_name.split('.')[-1], metadata['status']))
                        s = pd.Series([s['backendStatus'] if 'backendStatus' in s else 'NA' for s in metadata['calls'][task_name]])
                        print(s.value_counts().to_string())


    def get_entity_status(self, etype, config):
        """Get status of latest submission for the entity type in the workspace"""

        # filter submissions by configuration
        submissions = self.list_submissions(config=config)

        # get status of last run submission
        entity_dict = {}
        for k,s in enumerate(submissions, 1):
            print('\rFetching submission {}/{}'.format(k, len(submissions)), end='')
            if s['submissionEntity']['entityType']!=etype:
                print('\rIncompatible submission entity type: {}'.format(
                    s['submissionEntity']['entityType']))
                print('\rSkipping : '+ s['submissionId'])
                continue
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
        print()
        status_df = pd.DataFrame(entity_dict).T
        status_df.index.name = etype+'_id'

        return status_df[['status', 'timestamp', 'workflow_id', 'submission_id', 'configuration']]


    def get_sample_status(self, configuration):
        """Get status of lastest submission for samples in the workspace"""
        return self.get_entity_status('sample', configuration)


    def get_sample_set_status(self, configuration):
        """Get status of lastest submission for sample sets in the workspace"""
        return self.get_entity_status('sample_set', configuration)


    def get_pair_status(self, configuration):
        """Get status of lastest submission for pairs in the workspace"""
        return self.get_entity_status('pair', configuration)


    def get_pair_set_status(self, configuration):
        """Get status of lastest submission for pair sets in the workspace"""
        return self.get_entity_status('pair_set', configuration)


    def patch_attributes(self, cnamespace, configuration, dry_run=False, entity='sample'):
        """
        Patch attributes for all samples/tasks that run successfully but were not written to database.
        This includes outputs from successful tasks in workflows that failed.
        """

        # get list of expected outputs
        r = self.get_config(cnamespace, configuration)
        output_map = {i.split('.')[-1]:j.split('this.')[-1] for i,j in r['outputs'].items()}
        columns = list(output_map.values())

        if entity=='sample':
            # get list of all samples in workspace
            print('Fetching sample status ...')
            samples_df = self.get_samples()
            if len(np.intersect1d(columns, samples_df.columns))>0:
                incomplete_df = samples_df[samples_df[columns].isnull().any(axis=1)]
            else:
                incomplete_df = pd.DataFrame(index=samples_df.index, columns=columns)

            # get workflow status for all submissions
            sample_status_df = self.get_sample_status(configuration)

            # make sure successful workflows were all written to database
            error_ix = incomplete_df.loc[sample_status_df.loc[incomplete_df.index, 'status']=='Succeeded'].index
            if np.any(error_ix):
                print('Attributes from {} successful jobs were not written to database.'.format(len(error_ix)))

            # for remainder, assume that if attributes exists, status is successful.
            # this doesn't work when multiple successful runs of the same task exist --> need to add this

            # for incomplete samples, go through submissions and assign outputs of completed tasks
            task_counts = defaultdict(int)
            for n,sample_id in enumerate(incomplete_df.index, 1):
                print('\rPatching attributes for sample {}/{}'.format(n, incomplete_df.shape[0]), end='')

                try:
                    metadata = self.get_workflow_metadata(sample_status_df.loc[sample_id, 'submission_id'], sample_status_df.loc[sample_id, 'workflow_id'])
                    if 'outputs' in metadata and len(metadata['outputs'])!=0 and not dry_run:
                        attr = {output_map[k.split('.')[-1]]:t for k,t in metadata['outputs'].items()}
                        self.update_sample_attributes(sample_id, attr)
                    else:
                        for task in metadata['calls']:
                            if 'outputs' in metadata['calls'][task][-1]:
                                if np.all([k in output_map for k in metadata['calls'][task][-1]['outputs'].keys()]):
                                    # only update if attributes are empty
                                    if incomplete_df.loc[sample_id, [output_map[k] for k in metadata['calls'][task][-1]['outputs']]].isnull().any():
                                        # write to attributes
                                        if not dry_run:
                                            attr = {output_map[i]:j for i,j in metadata['calls'][task][-1]['outputs'].items()}
                                            self.update_sample_attributes(sample_id, attr)
                                        task_counts[task.split('.')[-1]] += 1
                except:
                    print('Metadata call failed for sample {}'.format(sample_id))
                    print(metadata.json())
            print()
            for i,j in task_counts.items():
                print('Samples patched for "{}": {}'.format(i,j))

        elif entity=='sample_set':
            print('Fetching sample set status ...')
            sample_set_df = self.get_sample_sets()
            # get workflow status for all submissions
            sample_set_status_df = self.get_sample_set_status(configuration)

            # any sample sets with empty attributes for configuration
            incomplete_df = sample_set_df.loc[sample_set_status_df.index, columns]
            incomplete_df = incomplete_df[incomplete_df.isnull().any(axis=1)]

            # sample sets with successful jobs
            error_ix = incomplete_df[sample_set_status_df.loc[incomplete_df.index, 'status']=='Succeeded'].index
            if np.any(error_ix):
                print('Attributes from {} successful jobs were not written to database.'.format(len(error_ix)))
                print('Patching attributes with outputs from latest successful run.')
                for n,sample_set_id in enumerate(incomplete_df.index, 1):
                    print('\r  * Patching sample set {}/{}'.format(n, incomplete_df.shape[0]), end='')
                    metadata = self.get_workflow_metadata(sample_set_status_df.loc[sample_set_id, 'submission_id'], sample_set_status_df.loc[sample_set_id, 'workflow_id'])
                    if 'outputs' in metadata and len(metadata['outputs'])!=0 and not dry_run:
                        attr = {output_map[k.split('.')[-1]]:t for k,t in metadata['outputs'].items()}
                        self.update_sample_set_attributes(sample_set_id, attr)
                print()
        print('Completed patching {} attributes in {}/{}'.format(entity, self.namespace, self.workspace))


    def display_status(self, configuration, entity='sample', filter_active=True):
        """
        Display summary of task statuses
        """
        # workflow status for each sample (from latest/current run)
        status_df = self.get_sample_status(configuration)

        # get workflow details from 1st submission
        metadata = self.get_workflow_metadata(status_df['submission_id'][0], status_df['workflow_id'][0])

        workflow_tasks = list(metadata['calls'].keys())

        print(status_df['status'].value_counts())
        if filter_active:
            ix = status_df[status_df['status']!='Succeeded'].index
        else:
            ix = status_df.index

        state_df = pd.DataFrame(0, index=ix, columns=workflow_tasks)
        for k,i in enumerate(ix, 1):
            print('\rFetching metadata for sample {}/{}'.format(k, len(ix)), end='')
            metadata = self.get_workflow_metadata(status_df.loc[i, 'submission_id'], status_df.loc[i, 'workflow_id'])
            state_df.loc[i] = [metadata['calls'][t][-1]['executionStatus'] if t in metadata['calls'] else 'Waiting' for t in workflow_tasks]
        print()
        state_df.rename(columns={i:i.split('.')[1] for i in state_df.columns}, inplace=True)
        summary_df = state_df.apply(lambda x : x.value_counts(), axis = 0).fillna(0).astype(int)
        print(summary_df)
        state_df[['workflow_id', 'submission_id']] = status_df.loc[ix, ['workflow_id', 'submission_id']]

        return state_df, summary_df


    def get_stderr(self, state_df, task_name):
        """
        Fetch stderrs from bucket (returns list of str)
        """
        df = state_df[state_df[task_name]==-1]
        fail_idx = df.index
        stderrs = []
        for n,i in enumerate(fail_idx, 1):
            print('\rFetching stderr for task {}/{}'.format(n, len(fail_idx)), end='\r')
            metadata = self.get_workflow_metadata(state_df.loc[i, 'submission_id'], state_df.loc[i, 'workflow_id'])
            stderr_path = metadata['calls'][[i for i in metadata['calls'].keys() if i.split('.')[1]==task_name][0]][-1]['stderr']
            s = subprocess.check_output('gsutil cat '+stderr_path, shell=True).decode()
            stderrs.append(s)
        return stderrs


    def get_submission_history(self, sample_id, config=None):
        """
        Currently only supports samples
        """

        # filter submissions by configuration
        submissions = self.list_submissions(config=config)

        # filter by sample
        submissions = [s for s in submissions
            if s['submissionEntity']['entityName']==sample_id
            and 'Succeeded' in list(s['workflowStatuses'].keys())
        ]

        outputs_df = []
        for s in submissions:
            r = self.get_submission(s['submissionId'])

            metadata = self.get_workflow_metadata(s['submissionId'], r['workflows'][0]['workflowId'])

            outputs_s = pd.Series(metadata['outputs'])
            outputs_s.index = [i.split('.',1)[1].replace('.','_') for i in outputs_s.index]
            outputs_s['submission_date'] = iso8601.parse_date(s['submissionDate']).strftime('%H:%M:%S %m/%d/%Y')
            outputs_df.append(outputs_s)

        outputs_df = pd.concat(outputs_df, axis=1).T
        # sort by most recent first
        outputs_df = outputs_df.iloc[np.argsort([datetime.timestamp(iso8601.parse_date(s['submissionDate'])) for s in submissions])[::-1]]
        outputs_df.index = ['run_{}'.format(str(i)) for i in np.arange(outputs_df.shape[0],0,-1)]

        return outputs_df


    def get_storage(self):
        """
        Get total amount of storage used, in TB

        Pricing: $0.026/GB/month (multi-regional)
                 $0.02/GB/month (regional)
        """
        bucket_id = self.get_bucket_id()
        s = subprocess.check_output('gsutil du -s gs://'+bucket_id, shell=True)
        return np.float64(s.decode().split()[0])/1024**4


    def get_stats(self, status_df, workflow_name=None):
        """
        For a list of submissions, calculate time, preemptions, etc
        """
        # for successful jobs, get metadata and count attempts
        status_df = status_df[status_df['status']=='Succeeded'].copy()
        metadata_dict = {}
        for k,(i,row) in enumerate(status_df.iterrows(), 1):
            print('\rFetching metadata {}/{}'.format(k,status_df.shape[0]), end='')
            fetch = True
            while fetch:  # improperly dealing with 500s here...
                try:
                    metadata = self.get_workflow_metadata(row['submission_id'], row['workflow_id'])
                    metadata_dict[i] = metadata.json()
                    fetch = False
                except:
                    pass

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

    #-------------------------------------------------------------------------
    #  Methods for manipulating configurations
    #-------------------------------------------------------------------------
    def list_configs(self):
        """List configurations in workspace"""
        r = firecloud.api.list_workspace_configs(self.namespace, self.workspace)
        if r.status_code != 200:
            raise APIException(r)
        return r.json()


    def get_config(self, cnamespace, config):
        """Get workspace configuration JSON"""
        r = firecloud.api.get_workspace_config(self.namespace, self.workspace, cnamespace, config)
        if r.status_code != 200:
            raise APIException(r)
        return r.json()


    def get_configs(self, latest_only=False):
        """Get all configurations in the workspace"""
        r = self.list_configs()
        df = pd.io.json.json_normalize(r)
        df.rename(columns={c:c.split('methodRepoMethod.')[-1] for c in df.columns}, inplace=True)
        if latest_only:
            df = df.sort_values(['methodName','methodVersion'], ascending=False).groupby('methodName').head(1)
            # .sort_values('methodName')
            # reverse sort
            return df[::-1]
        return df


    def import_config(self, cnamespace, cname=None):
        """Import configuration from repository"""
        # get latest snapshot
        c = get_config(cnamespace, cname)
        if len(c)==0:
            raise ValueError('Configuration "{}/{}" not found (name must match exactly).'.format(cnamespace, cname))
        c = c[np.argmax([i['snapshotId'] for i in c])]
        r = firecloud.api.copy_config_from_repo(self.namespace, self.workspace,
            cnamespace, cname, c['snapshotId'], cnamespace, cname)
        if r.status_code == 201:
            print('Successfully imported configuration "{}/{}" (SnapshotId {})'.format(cnamespace, cname, c['snapshotId']))
        else:
            raise APIException(r)

    def update_config(self, json_body):
        """
        Create or update a method configuration (separate API calls)

        json_body = {
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

        """
        configs = self.list_configs()
        if self.initialize_hound() is not None:
            self.hound.write_log_entry(
                'other',
                "Uploaded/Updated method configuration: {}/{}".format(
                    json_body['namespace'],
                    json_body['name']
                )
            )
        if json_body['name'] not in [m['name'] for m in configs]:
            # configuration doesn't exist -> name, namespace specified in json_body
            r = firecloud.api.create_workspace_config(self.namespace, self.workspace, json_body)
            if r.status_code == 201:
                print('Successfully added configuration: {}'.format(json_body['name']))
            elif r.status_code >= 400:
                raise APIException(r)
            else:
                print(r.text)
        else:
            r = firecloud.api.update_workspace_config(self.namespace, self.workspace,
                    json_body['namespace'], json_body['name'], json_body)
            if r.status_code == 200:
                print('Successfully updated configuration {}/{}'.format(json_body['namespace'], json_body['name']))
            elif r.status_code >= 400:
                raise APIException(r)
            else:
                print(r.text)


    def copy_config(self, wm, cnamespace, config=None):
        """Copy configuration from another workspace"""
        self.update_config(wm.get_config(cnamespace, config))


    def publish_config(self, from_cnamespace, from_config, to_cnamespace=None, to_config=None, public=False):
        """Copy configuration to repository"""
        if to_cnamespace is None:
            to_cnamespace = from_cnamespace
        if to_config is None:
            to_config = from_config

        # check whether prior version exists
        r = get_config(to_cnamespace, to_config)
        old_version = None
        if r:
            old_version = np.max([m['snapshotId'] for m in r])
            print('Configuration {}/{} exists. SnapshotID: {}'.format(
                to_cnamespace, to_config, old_version))

        # copy config to repo
        r = firecloud.api.copy_config_to_repo(self.namespace, self.workspace,
                from_cnamespace, from_config, to_cnamespace, to_config)
        if r.status_code == 200:
            print("Successfully copied {}/{}. New SnapshotID: {}".format(to_cnamespace, to_config, r.json()['snapshotId']))
        elif r.status_code >= 400:
            raise APIException(r)
        else:
            print(r.text)

        # make configuration public
        if public:
            print('  * setting public read access.')
            r = firecloud.api.update_repository_config_acl(to_cnamespace, to_config,
                    r.json()['snapshotId'], [{'role': 'READER', 'user': 'public'}])

        # delete old version
        if old_version is not None:
            r = firecloud.api.delete_repository_config(to_cnamespace, to_config, old_version)
            if r.status_code == 200:
                print("Successfully deleted SnapshotID {}.".format(old_version))
            elif r.status_code >= 400:
                raise APIException(r)
            else:
                print(r.text)


    def check_config(self, config_name):
        """
        Get version of a configuration and compare to latest available in repository
        """
        r = self.list_configs()
        r = [i for i in r if i['name']==config_name][0]['methodRepoMethod']
        # method repo version
        mrversion = get_method_version(r['methodNamespace'], r['methodName'])
        print('Method for config. {}: {} version {} (latest: {})'.format(config_name, r['methodName'], r['methodVersion'], mrversion))
        return r['methodVersion']


    def delete_config(self, cnamespace, config):
        """Delete workspace configuration"""
        r = firecloud.api.delete_workspace_config(self.namespace, self.workspace, cnamespace, config)
        if r.status_code == 204:
            if self.initialize_hound() is not None:
                self.hound.write_log_entry(
                    'other',
                    "Deleted method configuration: {}/{}".format(
                        cnamespace,
                        config
                    )
                )
            print('Successfully deleted configuration {}/{}'.format(cnamespace, config))
        elif r.status_code >= 400:
            raise APIException(r)
        else:
            print(r.text)

    #-------------------------------------------------------------------------
    #  Methods for querying entities
    #-------------------------------------------------------------------------
    def _get_entities_query(self, etype, page, page_size=1000):
        """Wrapper for firecloud.api.get_entities_query"""
        r = firecloud.api.get_entities_query(self.namespace, self.workspace,
                etype, page=page, page_size=page_size)
        if r.status_code == 200:
            return r.json()
        else:
            raise APIException(r)

    def get_entities(self, etype, page_size=1000):
        """Paginated query replacing get_entities_tsv()"""
        # get first page
        r = self._get_entities_query(etype, 1, page_size=page_size)

        # get additional pages
        total_pages = r['resultMetadata']['filteredPageCount']
        all_entities = r['results']
        for page in range(2,total_pages+1):
            r = self._get_entities_query(etype, page, page_size=page_size)
            all_entities.extend(r['results'])

        # convert to DataFrame
        df = pd.DataFrame({i['name']:i['attributes'] for i in all_entities}).T
        df.index.name = etype+'_id'
        # convert JSON to lists; assumes that values are stored in 'items'
        df = df.applymap(lambda x: x['items'] if isinstance(x, dict) and 'items' in x else x)
        return df


    def get_samples(self):
        """Get DataFrame with samples and their attributes"""
        df = self.get_entities('sample')
        df['participant'] = df['participant'].apply(lambda x: x['entityName'] if isinstance(x, dict) else x)
        return df


    def get_pairs(self):
        """Get DataFrame with pairs and their attributes"""
        df = self.get_entities('pair')
        df['participant'] = df['participant'].apply(lambda x: x['entityName'] if isinstance(x, dict) else x)
        df['case_sample'] = df['case_sample'].apply(lambda  x: x['entityName'] if isinstance(x, dict) else x)
        df['control_sample'] = df['control_sample'].apply(lambda x: x['entityName'] if isinstance(x, dict) else x)
        return df


    def get_participants(self):
        """Get DataFrame with participants and their attributes"""
        df = self.get_entities('participant')
        # convert sample lists from JSON
        df = df.applymap(lambda x: [i['entityName'] if 'entityName' in i else i for i in x]
                            if isinstance(x, list) and np.all(pd.notnull(x)) else x)
        return df


    def get_sample_sets(self):
        """Get DataFrame with sample sets and their attributes"""
        df = self.get_entities('sample_set')
        # convert sample lists from JSON
        df = df.applymap(lambda x: [i['entityName'] if 'entityName' in i else i for i in x]
                            if isinstance(x, list) and np.all(pd.notnull(x)) else x)
        return df


    def get_participant_sets(self):
        """Get DataFrame with sample sets and their attributes"""
        df = self.get_entities('participant_set')
        # convert sample lists from JSON
        df = df.applymap(lambda x: [i['entityName'] if 'entityName' in i else i for i in x]
                            if isinstance(x, list) and np.all(pd.notnull(x)) else x)
        return df


    def get_pair_sets(self):
        """Get DataFrame with sample sets and their attributes"""
        df = self.get_entities('pair_set')
        df['pairs'] = df['pairs'].apply(lambda x: [i['entityName'] for i in x] if isinstance(x, list) and np.all(pd.notnull(x)) else x)

        # # convert JSON to table
        # columns = np.unique([k for s in df for k in s['attributes'].keys()])
        # df = pd.DataFrame(index=df.index, columns=columns)
        # for s in df:
        #     for c in columns:
        #         if c in s['attributes']:
        #             if isinstance(s['attributes'][c], dict):
        #                 df.loc[s['name'], c] = [i['entityName'] if 'entityName' in i else i for i in
        #                                         s['attributes'][c]['items']]
        #             else:
        #                 df.loc[s['name'], c] = s['attributes'][c]
        return df


    def get_pairs_in_pair_set(self, pair_set):
        """Get DataFrame with pairs belonging to pair_set"""
        # return df[df.index.isin(self.get_pair_sets().loc[pair_set, 'pairs'])]
        df = self.get_pairs()
        df = df[
            np.in1d(df.index.values, self.get_pair_sets().loc[pair_set]['pairs'])]
        return df

    #-------------------------------------------------------------------------
    #  Methods for updating entity sets
    #-------------------------------------------------------------------------
    def update_entity_set(self, etype, set_id, entity_ids):
        """Update or create an entity set"""
        assert etype in ['sample', 'pair', 'participant']
        r = firecloud.api.get_entity(self.namespace, self.workspace, etype+'_set', set_id)
        if r.status_code==200:  # exists -> update
            r = r.json()
            items_dict = r['attributes']['{}s'.format(etype)]
            items_dict['items'] = [{'entityName': i, 'entityType': etype} for i in entity_ids]
            attrs = [{
                'addUpdateAttribute': items_dict,
                'attributeName': '{}s'.format(etype),
                'op': 'AddUpdateAttribute'
            }]
            r = firecloud.api.update_entity(self.namespace, self.workspace, etype+'_set', set_id, attrs)
            if r.status_code == 200:
                print('{} set "{}" ({} {}s) successfully updated.'.format(
                    etype.capitalize(), set_id, len(entity_ids), etype))
                if self.initialize_hound() is not None:
                    self.hound.update_entity_meta(
                        etype+'_set',
                        set_id,
                        "Updated set members"
                    )
                    self.hound.update_entity_attribute(
                        etype+'_set',
                        set_id,
                        etype+'s',
                        [*entity_ids]
                    )
                return True
            elif r.status_code >= 400:
                raise APIException(r)
            else:
                print(r.text)
        else:
            set_df = pd.DataFrame(
                data=np.c_[[set_id]*len(entity_ids), entity_ids],
                columns=['membership:{}_set_id'.format(etype), '{}_id'.format(etype)]
            )
            self.upload_entities('{}_set'.format(etype), set_df, index=False)
            return True
        return False


    def update_sample_set(self, sample_set_id, sample_ids):
        """Update or create a sample set"""
        self.update_entity_set('sample', sample_set_id, sample_ids)


    def update_pair_set(self, pair_set_id, pair_ids):
        """Update or create a pair set"""
        self.update_entity_set('pair', pair_set_id, pair_ids)


    def update_participant_set(self, participant_set_id, participant_ids):
        """Update or create a participant set"""
        self.update_entity_set('participant', participant_set_id, participant_ids)


    def update_super_set(self, super_set_id, sample_set_ids, sample_ids):
        """
        Update (or create) a set of sample sets

        Defines the attribute "sample_sets_"

        sample_ids: at least one 'dummy' sample is needed
        """
        if isinstance(sample_ids, str):
            self.update_sample_set(super_set_id, [sample_ids])
        else:
            self.update_sample_set(super_set_id, sample_ids)

        attr_dict = {
            "sample_sets_": {
                "itemsType": "EntityReference",
                "items": [{"entityType": "sample_set", "entityName": i} for i in sample_set_ids]
            }
        }
        attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
        r = firecloud.api.update_entity(self.namespace, self.workspace, 'sample_set', super_set_id, attrs)
        if r.status_code == 200:
            print('Set of sample sets "{}" successfully created.'.format(super_set_id))
            if self.initialize_hound() is not None:
                self.hound.update_entity_meta(
                    'sample_set',
                    super_set_id,
                    "Updated super_set members"
                )
                self.hound.update_entity_attribute(
                    'sample_set',
                    super_set_id,
                    'sample_sets_',
                    [*sample_set_ids]
                )
        elif r.status_code >= 400:
            raise APIException(r)
        else:
            print(r.text)


    #-------------------------------------------------------------------------
    #  Methods for deleting entities and attributes
    #-------------------------------------------------------------------------
    def delete_entity_attributes(self, etype, attrs, entity_id=None, delete_files=False, dry_run=False):
        """
        Delete entity attributes and (optionally) their associated data

        Examples

          To delete an attribute for all samples:
            samples_df = wm.get_samples()
            wm.delete_entity_attributes('sample', samples_df[attr_name])

          To delete multiple attributes a single sample:
            wm.delete_entity_attributes('sample', attributes_list, entity_id=sample_id)

        WARNING: This action is not reversible. Be careful!
        """
        assert isinstance(attrs, (list, pd.Series, pd.DataFrame))
        et = etype.replace('_set', ' set')

        if delete_files:
            assert isinstance(attrs, (pd.DataFrame, pd.Series))
            file_list = [x for x in attrs.values.flatten() if type(x) is str and x.startswith('gs://')]
            if dry_run:
                print('[dry-run] the following files will be deleted:')
                print('\n'.join(file_list))
                return
            else:
                gs_delete(file_list)

        if isinstance(attrs, pd.DataFrame):  # delete index x column combinations
            attr_list = [{
                'name':i,
                'entityType':etype,
                'operations':[{'attributeName':c, 'op':'RemoveAttribute'} for c in attrs]
            } for i in attrs.index]
            msg = "Successfully deleted attributes {} for {} {}s.".format(attrs.columns, attrs.shape[0], et)
        elif isinstance(attrs, pd.Series) and attrs.name is not None:  # delete index x attr.name
            # assume attrs.name is attribute name
            attr_list = [{
                'name':i,
                'entityType':etype,
                'operations':[{'attributeName':attrs.name, 'op':'RemoveAttribute'}]
            } for i in attrs.index]
            msg = "Successfully deleted attribute {} for {} {}s.".format(attrs.name, attrs.shape[0], et)
        elif isinstance(attrs, list) and entity_id is not None:
            attr_list = [{
                'name':entity_id,
                'entityType':etype,
                'operations':[{'attributeName':i, 'op':'RemoveAttribute'} for i in attrs]
            }]
            msg = "Successfully deleted attributes {} for {} {}.".format(attrs, et, entity_id)
        else:
            raise ValueError('Input type is not supported.')

        # TODO: try
        r = _batch_update_entities(self.namespace, self.workspace, attr_list)
        if r.status_code == 204:
            print(msg)
            if self.initialize_hound() is not None:
                for obj in attr_list:
                    self.hound.update_entity_meta(
                        etype,
                        obj['name'],
                        "Deleting {} attributes: {}".format(
                            len(obj['operations']),
                            ', '.join(attr['attributeName'] for attr in obj['operations'])
                        )
                    )
                    for attr in obj['operations']:
                        self.hound.update_entity_attribute(
                            etype,
                            obj['name'],
                            attr['attributeName'],
                            None
                        )
        elif r.status_code >= 400:
            raise APIException(r)
        else:
            print(r.text)
        # except:  # rawls API not available
        #     if isinstance(attrs, str):
        #         rm_list = [{"op": "RemoveAttribute", "attributeName": attrs}]
        #     elif isinstance(attrs, Iterable):
        #         rm_list = [{"op": "RemoveAttribute", "attributeName": i} for i in attrs]
        #     r = firecloud.api.update_entity(self.namespace, self.workspace, etype, ename, rm_list)
        #         assert r.status_code==200


    def delete_sample_attributes(self, attrs, entity_id=None, delete_files=False, dry_run=False):
        """Delete sample attributes and (optionally) their associated data"""
        self.delete_entity_attributes('sample', attrs,
                entity_id=entity_id, delete_files=delete_files, dry_run=dry_run)


    def delete_sample_set_attributes(self, attrs, entity_id=None, delete_files=False, dry_run=False):
        """Delete sample set attributes and (optionally) their associated data"""
        self.delete_entity_attributes('sample_set', attrs,
                entity_id=entity_id, delete_files=delete_files, dry_run=dry_run)


    def delete_participant_attributes(self, attrs, entity_id=None, delete_files=False, dry_run=False):
        """Delete participant attributes and (optionally) their associated data"""
        self.delete_entity_attributes('participant', attrs,
                entity_id=entity_id, delete_files=delete_files, dry_run=dry_run)


    def delete_entity(self, etype, entity_ids):
        """Delete entity or list of entities"""
        r = firecloud.api.delete_entity_type(self.namespace, self.workspace, etype, entity_ids)
        if r.status_code == 204:
            print('{}(s) {} successfully deleted.'.format(etype.replace('_set', ' set').capitalize(), entity_ids))
            if self.initialize_hound() is not None:
                self.hound.write_log_entry(
                    'upload',
                    "Deleting {} {}s".format(
                        len(entity_ids),
                        etype
                    ),
                    entities=[
                        os.path.join(etype, eid) for eid in entity_ids
                    ]
                )
                for eid in entity_ids:
                    self.hound.update_entity_meta(
                        etype,
                        eid,
                        "Deleted entity"
                    )
        elif r.status_code >= 400:
            raise APIException(r)
        else:
            print(r.text)


    def delete_sample(self, sample_ids, delete_dependencies=True):
        """Delete sample or list of samples"""
        if isinstance(sample_ids, str):
            sample_id_set = set([sample_ids])
        else:
            sample_id_set = set(sample_ids)
        etype = 'sample'
        r = firecloud.api.delete_entity_type(self.namespace, self.workspace, 'sample', sample_ids)
        if r.status_code == 204:
            print('Sample(s) {} successfully deleted.'.format(sample_ids))
            if self.initialize_hound() is not None:
                self.hound.write_log_entry(
                    'upload',
                    "Deleting {} samples".format(
                        len(sample_ids),
                    ),
                    entities=[
                        os.path.join('sample', eid) for eid in sample_ids
                    ]
                )
                for eid in sample_ids:
                    self.hound.update_entity_meta(
                        'sample',
                        eid,
                        "Deleted entity"
                    )
        elif r.status_code >= 400:
            raise APIException(r)

        elif r.status_code==409 and delete_dependencies:
            # delete participant dependencies
            participant_df = self.get_participants()
            if 'samples_' in participant_df.columns:
                participant_df = participant_df[participant_df['samples_'].apply(lambda x: np.any([i in sample_id_set for i in x]))]
                entitites_dict = participant_df['samples_'].apply(lambda x: np.array([i for i in x if i not in sample_id_set])).to_dict()
                participant_ids = np.unique(participant_df.index)
                for n,k in enumerate(participant_ids, 1):
                    print('\r  * removing {}s for participant {}/{}'.format(etype, n, len(participant_ids)), end='')
                    attr_dict = {
                        "{}s_".format(etype): {
                            "itemsType": "EntityReference",
                            "items": [{"entityType": etype, "entityName": i} for i in entitites_dict[k]]
                        }
                    }
                    attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
                    r = firecloud.api.update_entity(self.namespace, self.workspace, 'participant', k, attrs)
                    if r.status_code != 200:
                        raise APIException(r)
                    if self.initialize_hound() is not None:
                        self.hound.update_entity_attribute(
                            'participant',
                            k,
                            'samples_',
                            entities_dict[k],
                            "<Automated> Removing samples from participant prior to sample deletion"
                        )
                print()

            # delete sample set dependencies
            set_df = self.get_sample_sets()
            for i,s in set_df['samples'].items():
                if np.any([i in sample_id_set for i in s]):
                    with ExitStack() as stack:
                        if self.initialize_hound() is not None:
                            stack.enter_context(
                                self.hound.with_reason("<Automated> Removing samples from sample_set prior to sample deletion")
                            )
                        self.update_sample_set(i, np.setdiff1d(s, list(sample_id_set)))

            # try again
            r = firecloud.api.delete_entity_type(self.namespace, self.workspace, 'sample', sample_ids)
            if r.status_code == 204:
                print('Sample(s) {} successfully deleted.'.format(sample_ids))
                if self.initialize_hound() is not None:
                    self.hound.write_log_entry(
                        'upload',
                        "Deleting {} samples".format(
                            len(sample_ids),
                        ),
                        entities=[
                            os.path.join('sample', eid) for eid in sample_ids
                        ]
                    )
                    for eid in sample_ids:
                        self.hound.update_entity_meta(
                            'sample',
                            eid,
                            "Deleted entity"
                        )
            elif r.status_code >= 400:
                raise APIException(r)
            else:
                print(r.text)
        else:
            print(r.text)


    def delete_sample_set(self, sample_set_id):
        """Delete sample set(s)"""
        self.delete_entity('sample_set', sample_set_id)


    def delete_participant(self, participant_ids, delete_dependencies=False):
        """Delete participant or list of participants"""
        r = firecloud.api.delete_entity_type(self.namespace, self.workspace, 'participant', participant_ids)
        if r.status_code == 204:
            print('Participant(s) {} successfully deleted.'.format(participant_ids))
            if self.initialize_hound() is not None:
                for pid in participant_ids:
                    self.hound.update_entity_meta(
                        'participant',
                        pid,
                        "Deleted entity"
                    )
        elif r.status_code == 409:
            if delete_dependencies:
                r2 = firecloud.api.delete_entities(self.namespace, self.workspace, r.json())
                if r2.status_code == 204:
                    print('Participant(s) {} and dependent entities successfully deleted.'.format(participant_ids))
                    if self.initialize_hound() is not None:
                        for entity in r.json():
                            self.hound.update_entity_meta(
                                entity['entityType'],
                                entity['entityName'],
                                "Deleted entity",
                                (
                                    None
                                    if entity['entityType'] == 'participant'
                                    else "<Automated> Removing {}s from participant prior to sample deletion".format(
                                        entity['entityType']
                                    )
                                )
                            )
                elif r2.status_code >= 400:
                    raise APIException(r2)
                else:
                    print(r2.text)
            else:
                print('The following entities must be deleted before the participant(s) can be deleted:')
                print(r.text)
        else:
            print(r.text)


    def delete_pair_set(self, pair_id):
        """Delete pair(s)"""
        self.delete_entity('pair', pair_id)


    def delete_pair_set(self, pair_set_id):
        """Delete pair set(s)"""
        self.delete_entity('pair_set', pair_set_id)


    #-------------------------------------------------------------------------
    #
    #-------------------------------------------------------------------------
    def find_sample_set(self, sample_id, sample_set_df=None):
        """Find sample set(s) containing sample"""
        if sample_set_df is None:
            sample_set_df = self.get_sample_sets()
        return sample_set_df[sample_set_df['samples'].apply(lambda x: sample_id in x)].index.tolist()


    def purge_unassigned(self, attribute=None, bucket_files=None, entities_df=None, ext=None):
        """
        Delete any files that don't match attributes in the data model (e.g., from prior/outdated runs)
        """
        if bucket_files is None:
            bucket_files = gs_list_bucket_files(self.get_bucket_id())

        # exclude FireCloud logs etc
        bucket_files = [i for i in bucket_files if not i.endswith(('exec.sh', 'stderr.log', 'stdout.log'))]

        if entities_df is None:  # fetch all entities
            found_attrs = self.get_samples().values.flatten().tolist() \
                        + self.get_sample_sets().values.flatten().tolist() \
                        + self.get_participants().values.flatten().tolist()
            # missing: get_pairs, get_pair_sets --> resolve participant vs participant_id issue first
        else:
            found_attrs = entities_df.values.flatten().tolist()

        # flatten
        assigned = []
        for i in found_attrs:
            if isinstance(i, str) and i.startswith('gs://'):
                assigned.append(i)
            elif isinstance(i, list) and np.all([j.startswith('gs://') for j in i]):
                assigned.extend(i)

        # remove assigned from list
        assigned_set = set([os.path.split(i)[-1] for i in assigned])
        # a = [i for i in np.setdiff1d(bucket_files, assigned) if os.path.split(i)[-1] in assigned_set]
        # [os.path.split(i)[-1] for i in a]
        #
        # try:
        #     assert attribute in samples_df.columns
        # except:
        #     raise ValueError('Sample attribute "{}" does not exist'.format(attribute))
        #
        # # make sure all samples have attribute set
        # assert samples_df[attribute].isnull().sum()==0
        #
        # if ext is None:
        #     ext = np.unique([os.path.split(i)[1].split('.',1)[1] for i in samples_df[attribute]])
        #     assert len(ext)==1
        #     ext = ext[0]
        #
        # purge_paths = [i for i in bucket_files if i.endswith(ext) and i not in set(samples_df[attribute])]
        # if len(purge_paths)==0:
        #     print('No outdated files to purge.')
        # else:
        #     bucket_id = self.get_bucket_id()
        #     assert np.all([i.startswith('gs://'+bucket_id) for i in purge_paths])
        #
        #     while True:
        #         s = input('{} outdated files found. Delete? [y/n] '.format(len(purge_paths))).lower()
        #         if s=='n' or s=='y':
        #             break
        #
        #     if s=='y':
        #         print('Purging {} outdated files.'.format(len(purge_paths)))
        #         gs_delete(purge_paths, chunk_size=500)


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
        if isinstance(attrs, pd.DataFrame):
            attr_list = []
            for i,row in attrs.iterrows():
                attr_list.extend([{
                    'name':row.name,
                    'entityType':etype,
                    'operations': [{"op": "AddUpdateAttribute", "attributeName": i, "addUpdateAttribute":str(j)} for i,j in row.iteritems() if not pd.isnull(j)]
                }])
        elif isinstance(attrs, pd.Series):
            attr_list = [{
                'name':i,
                'entityType':etype,
                'operations': [{"op": "AddUpdateAttribute", "attributeName":attrs.name, "addUpdateAttribute":str(j)}]
            } for i,j in attrs.iteritems() if not pd.isnull(j)]
        else:
            raise ValueError('Unsupported input format.')

        # try rawls batch call if available
        r = _batch_update_entities(self.namespace, self.workspace, attr_list)
        # try:  # TODO
        if r.status_code == 204:
            if isinstance(attrs, pd.DataFrame):
                print("Successfully updated attributes '{}' for {} {}s.".format(attrs.columns.tolist(), attrs.shape[0], etype))
            elif isinstance(attrs, pd.Series):
                print("Successfully updated attribute '{}' for {} {}s.".format(attrs.name, len(attrs), etype))
            else:
                print("Successfully updated attribute '{}' for {} {}s.".format(attrs.name, len(attrs), etype))
            if self.initialize_hound() is not None:
                for obj in attr_list:
                    self.hound.update_entity_meta(
                        etype,
                        obj['name'],
                        "Updating {} attributes: {}".format(
                            len(obj['operations']),
                            ', '.join(attr['attributeName'] for attr in obj['operations'])
                        )
                    )
                    for attr in obj['operations']:
                        self.hound.update_entity_attribute(
                            etype,
                            obj['name'],
                            attr['attributeName'],
                            attr['addUpdateAttribute']
                        )
        elif r.status_code >= 400:
            raise APIException(r)
        else:
            print(r.text)
        # except:  # revert to public API
        #     attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
        #     r = firecloud.api.update_entity(self.namespace, self.workspace, etype, ename, attrs)
        #     if r.status_code==200:
        #         print('Successfully updated {}.'.format(ename))
        #     else:
        #         print(r.text)


    def create_submission(self, cnamespace, config, entity, etype, expression=None, use_callcache=True):
        """Create submission"""
        r = firecloud.api.create_submission(self.namespace, self.workspace,
            cnamespace, config, entity, etype, expression=expression, use_callcache=use_callcache)
        if r.status_code == 201:
            submission_id = r.json()['submissionId']
            print('Successfully created submission {}.'.format(submission_id))
            if self.initialize_hound() is not None:
                self.hound.write_log_entry(
                    'job',
                    (
                        "User started FireCloud submission {};"
                        " Job results will not be visible to Hound."
                        " Configuration: {}/{}, Entity: {}/{}, Expression: {}"
                    ).format(
                        submission_id,
                        cnamespace,
                        config,
                        etype,
                        entity,
                        'null' if expression is None else expression
                    ),
                    entities=[
                        os.path.join(etype, entity)
                    ]
                )
            return submission_id
        else:
            raise APIException(r)
