import unittest
import unittest.mock
import json
from collections import namedtuple
from hashlib import md5
import pandas as pd
import contextlib
import os
import requests as reqs

import firecloud.api

# to avoid a credentials issue during online unittesting

def _fiss_agent_header(headers=None):
    """ Return request headers for fiss.
        Inserts FISS as the User-Agent.
        Initializes __SESSION if it hasn't been set.

    Args:
        headers (dict): Include additional headers as key-value pairs

    """

    if firecloud.api.__SESSION is None:
        firecloud.api.__SESSION = reqs.session()

    fiss_headers = {"User-Agent" : 'FISS/0.16.21'}
    if headers is not None:
        fiss_headers.update(headers)
    return fiss_headers

firecloud.api._fiss_agent_header = _fiss_agent_header

import dalmatian


relpath = os.path.dirname(__file__)

mockResponse = namedtuple(
    'MockResponse',
    [
        'status_code',
        'text',
        'content',
        'headers',
        'json'
    ]
)

def makekey(method, url, data=None, headers=None, kwargs=None):
    if kwargs is None:
        kwargs = {}
    return md5(
        ('+'.join(
            [
                repr(method),
                repr(url),
                repr(data),
                repr(headers),
                repr(kwargs)
            ]
        )).encode()
    ).hexdigest()

with open(os.path.join(relpath, 'request.log')) as r:
    requests = {}
    for request in json.load(r):
        key = makekey(**request['inputs'])
        if key in requests:
            requests[key].append(request['outputs'])
        else:
            requests[key] = [request['outputs']]

def callable_json(text):

    def get_json():
        return json.loads(text)

    return get_json

@contextlib.contextmanager
def no_op_ctx(*args, **kwargs):
    yield

def mocked_request(method, url, data=None, headers=None, **kwargs):
    # if 'Content-type' not in headers:
    #     if 'json' in kwargs:
    #         headers['Content-type'] =  'application/json'
    #     if url.endswith('importEntities'):
    #         headers['Content-type'] = 'application/x-www-form-urlencoded'
    key = makekey(method=method, url=url, data=data, headers=headers, kwargs=kwargs)
    if key in requests and len(requests[key]):
        response = requests[key].pop(0)
        return mockResponse(
            response['status_code'],
            response['content'],
            response['content'].encode(),
            response['headers'],
            callable_json(response['content'])
        )
    print(method, url, data, headers, kwargs)
    raise KeyError(key)


# Monkey Patch the wrapped request method
getattr(dalmatian.firecloud.api, "__SESSION").request = mocked_request

class TestDalmatian(unittest.TestCase):

    def test_simple_interactions(self):
        ws = dalmatian.WorkspaceManager('broad-firecloud-gtex/unit_testing')
        ws._LegacyWorkspaceManager__hound = unittest.mock.Mock(dalmatian.base.HoundClient)
        ws.hound.configure_mock(**{
            'with_reason.side_effect': no_op_ctx,
            'batch.side_effect': no_op_ctx
        })
        self.assertTrue(ws.create_workspace())
        self.assertEqual(ws.bucket_id, 'fc-3ce4e797-82a8-46fe-8eee-66422dd92ed0')
        ws.upload_samples(pd.read_csv(os.path.join(relpath, 'test_samples.tsv'), sep='\t', index_col=0))
        ws.update_participant_samples()
        with open(os.path.join(relpath, 'rnaseqc_counts.config.json')) as r:
            ws.update_config(json.load(r))
        ws.update_sample_set('all_samples', ws.samples.index)
        self.assertEqual(ws.create_submission('rnaseqc_v2_cfg', 'all_samples', 'sample_set', 'this.samples'), '88c08eea-c10d-4f8c-b757-504ae59f86c5')
        ws.delete_workspace()
        # Check that the request buffer has been played out
        for buf in requests.values():
            self.assertEqual(len(buf), 0)
