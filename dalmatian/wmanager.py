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
from agutil import status_bar
from google.cloud import storage
from .core import *
from .base import LegacyWorkspaceManager

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

class WorkspaceManager(LegacyWorkspaceManager):
    # This abstraction provides 2 benefits
    # 1) The code is now split between 2 files
    # 2) Base workspace manager functions are now segregated from higher level operator/caching functions


    def upload_entities(self, etype, df, index=True):
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
        return super().upload_entities(
            etype,
            staged_df,
            index
        )

    def update_attributes(self, attr_dict=None, **kwargs):
        """
        Set or update workspace attributes. Wrapper for API 'set' call
        Accepts a dictionary of attribute:value pairs and/or keyword arguments.
        Updates workspace attributes using the combination of the attr_dict and any keyword arguments
        Any values which reference valid filepaths will be uploaded to the workspace
        """
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
        super().update_attributes(attr_dict)
        return attr_dict
