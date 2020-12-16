# Copyright 2012 Grid Dynamics
# Copyright 2013 Inktank Storage, Inc.
# Copyright 2014 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

#    This module is a thin wrapper around curvefs.
#    It currently provides all the synchronous methods of curvefs that do
#    not use callbacks.
#
#    Error codes from curvefs are turned into exceptions that subclass
#    :class:`Error`. Almost all methods may raise :class:`Error`
#    (the base class of all curvefs exceptions)

import abc
import errno
import http.client
import time
import urllib

from oslo_log import log as logging
from oslo_utils import excutils
from oslo_serialization import jsonutils as json

from cinder import exception

try:
    import curvefs
except ImportError:
    curvefs = None

LOG = logging.getLogger(__name__)


class BaseEnum(abc.ABCMeta):
    @abc.abstractmethod
    def values(cls):
        cls_dict = cls.__dict__

        return [cls_dict[key] for key in cls_dict.keys()
                if not key.startswith('_') and key.isupper()]


class SnapshotStatus(BaseEnum):
    Done = 0
    Pending = 1
    Deleting = 2
    ErrorDeleting = 3
    Canceling = 4
    Error = 5


class CloneTaskStatus(BaseEnum):
    Done = 0
    Cloning = 1
    Recovering = 2
    Cleaning = 3
    ErrorCleaning = 4
    Error = 5


class Error(Exception):
    pass


class ImageExists(Error):
    pass


class OperationFailed(Error):
    pass


class IODisbaled(Error):
    pass


class AuthFailed(Error):
    pass


class UnderDeleting(Error):
    pass


class ImageNotFound(Error):
    pass


class UnderSnapshoting(Error):
    pass


class NotUnderSnapshoting(Error):
    pass


class ErrorDelete(Error):
    pass


class SegmentNotAllocated(Error):
    pass


class NotSupported(Error):
    pass


class DirNotEmpty(Error):
    pass


class ResizeDownError(Error):
    pass


class SessionNotExists(Error):
    pass


class ImageBusy(Error):
    pass


class InvalidArgument(Error):
    pass


class InternalError(Error):
    pass


class CRCError(Error):
    pass


class InvalidRequest(Error):
    pass


class DiskError(Error):
    pass


class NoSpace(Error):
    pass


class NotAligned(Error):
    pass


class UnknownError(Error):
    pass


class CBDClientException(Exception):
    def __init__(self, code, message=None, body=None, request_id=None):
        self.code = code
        self.message = message
        self.body = body
        self.request_id = request_id

        formatted_string = "%s (HTTP %s)" % (self.message, self.code)
        if self.request_id:
            formatted_string += " (Request-ID: %s)" % self.request_id

        if self.body:
            LOG.error(("%(formatted_string)s %(body)s"),
                      {"formatted_string": formatted_string,
                       "body": self.body})

    def __str__(self):
        msg = "%s (HTTP %s)" % (self.message, self.code)
        if self.request_id:
            msg += " (Request-ID: %s)" % self.request_id

        return msg


def make_ex(ret, msg):
    """
    Translate a librbd return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """

    errors = {
        1: ImageExists,
        2: OperationFailed,
        3: IODisbaled,
        4: AuthFailed,
        5: UnderDeleting,
        6: ImageNotFound,
        7: UnderSnapshoting,
        8: NotUnderSnapshoting,
        9: ErrorDelete,
        10: SegmentNotAllocated,
        11: NotSupported,
        12: DirNotEmpty,
        13: ResizeDownError,
        14: SessionNotExists,
        15: ImageBusy,
        16: InvalidArgument,
        17: InternalError,
        18: CRCError,
        19: InvalidRequest,
        20: DiskError,
        21: NoSpace,
        22: NotAligned,
        100: UnknownError,
    }

    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
    else:
        return Error(msg + (": error code %d" % ret))


class CurveFS(object):
    def __init__(self, conf):
        self.client = curvefs.CBDClient()
        ret = self.client.Init(str(conf.curve_client_conf))
        if ret < 0:
            raise make_ex(ret, 'error init curvefs')

        self.user = curvefs.UserInfo_t()
        self.user.owner = conf.curve_user

    def create(self, volume_path, size):
        ret = self.client.Create(str(volume_path), self.user, size)
        # NOTE(lyh): return -LIBCURVE_ERROR::EXISTS (-1) means curve MDS
        #            already has the image when libcurve retry create file
        if ret < 0 and ret != -1:
            raise make_ex(ret, 'error creating image')

    def delete(self, volume_path):
        ret = self.client.Unlink(str(volume_path), self.user)
        if ret < 0:
            try:
                raise make_ex(ret, 'error deleting image')
            except ImageBusy:
                raise exception.VolumeIsBusy(volume_name=volume_path)

    def extend(self, volume_path, size):
        ret = self.client.Extend(str(volume_path), self.user, size)
        if ret < 0:
            raise make_ex(ret, 'error deleting image')

    def mkdir(self, directory):
        ret = self.client.Mkdir(str(directory), self.user)
        if ret < 0:
            raise make_ex(ret, 'error creating directory')

    def get_fsid(self):
        return self.client.GetClusterId()

    def __exit__(self):
        self.client.UnInit()


class CBDClient(object):
    def __init__(self, conf):
        self.conf = conf
        self.host, self.port = conf.cbd_snapshot_api_server.split(':')
        self.prefix_url = "/" + conf.cbd_snapshot_prefix_url
        self.default_api_version = conf.cbd_snapshot_api_version
        self.curve_user = conf.curve_user

    def _do_request(self, action, **kwargs):
        queryset = urllib.parse.urlencode(kwargs)
        url = ("%(prefix)s?Action=%(action)s&%(queryset)s" %
               {'prefix': self.prefix_url,
                'action': action,
                'queryset': queryset})

        #NOTE(lyh): "/" will be encode as "%2f" by urlencode,
        #           but curvefs can't parse it. so replace it.
        url = url.replace("%2F", "/")

        headers = {"Content-type": "application/json",
                   "Accept": "application/json"}

        return self._http_request('GET', url, headers, None)

    def _http_request(self, method, url, headers, body):
        LOG.debug(("Send request to %(url)s, method: %(method)s, "
                   "body: %(body)s, headers: %(headers)s") %
                  {"url": url, "method": method,
                   "body": body, "headers": headers})

        http_conn = http.client.HTTPConnection(self.host, self.port)
        http_conn.request(method, url, body, headers)
        resp = http_conn.getresponse()

        if resp.status == 200 or resp.status == 500:
            code = None
            try:
                data = json.loads(resp.read())
                code = int(data['Code'])
            except ValueError:
                LOG.warn("Response body not a valid json: %s" % resp.read())
                data = None
                request_id = None
            else:
                request_id = data["RequestId"]

            LOG.debug(("Response %(url)s with: "
                       "request-id: %(request_id)s, body: %(body)s") %
                      {"url": url, "request_id": request_id, "body": data})

            if code == 0:
                return data
            elif code == -16:
                raise ImageBusy("error deleting image.")
            else:
                raise CBDClientException(resp.status, resp.reason, resp.read())
        else:
            raise CBDClientException(resp.status, resp.reason, resp.read())

    def _wait_task_for_expect_status(self, task_uuid, expect_status, user,
                                     size, api_version=None):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            UUID=task_uuid
        )

        start = time.time()
        timeout = int(int(size) * 1024 / 10 + self.conf.wait_cbd_task_timeout)
        LOG.debug("set timeout to %d for snapshot size %d" % (timeout, size))
        while True:
            task_status = None
            try:
                response = self._do_request('GetCloneTasks', **kwargs)
                task_status = response['TaskInfos'][0]['TaskStatus']
            except Exception as ex:
                LOG.error("unexpect response when get clone tasks. %s"
                          % str(ex))
                raise exception.CurveCloneFailed(task_uuid)

            if task_status in expect_status:
                return
            if task_status == CloneTaskStatus.Error:
                raise exception.CurveCloneFailed(task_uuid)
            if time.time() - start > timeout:
                raise exception.CurveCloneTimeOut(task_uuid)

            time.sleep(10)

    def _wait_snapshot_for_available(self, volume, snapshot_uuid, user, size,
                                     api_version=None):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            UUID=snapshot_uuid,
            File=volume
        )

        start = time.time()
        timeout = int(int(size) * 1024 / 10 + self.conf.wait_cbd_task_timeout)
        LOG.debug("set timeout to %d for volume size %d" % (timeout, size))

        while True:
            task_status = None
            try:
                response = self._do_request('GetFileSnapshotInfo', **kwargs)
                task_status = response['Snapshots'][0]['Status']
            except Exception as ex:
                # NOTE(lyh): If http request failed, just retry
                LOG.error("unexpect response when get clone tasks. %s"
                          % str(ex))

            if task_status == SnapshotStatus.Done:
                return
            if task_status == SnapshotStatus.Error:
                raise exception.CurveSnapshotCreateFailed(snapshot_uuid)
            if time.time() - start > timeout:
                raise exception.CurveSnapshotCreateTimeOut(snapshot_uuid)

            time.sleep(10)

    def clone(self, source, dest, user, size, api_version=None, lazy="false"):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            Source=source,
            Destination=dest,
            Lazy=lazy
        )

        response = self._do_request('Clone', **kwargs)
        task_uuid = response['UUID']
        expect_status = [CloneTaskStatus.Done]

        try:
            return self._wait_task_for_expect_status(task_uuid, expect_status,
                                                     user, size)
        except exception.CurveCloneTimeOut:
            with excutils.save_and_reraise_exception():
                self.clean_clone_task(task_uuid, user)

    def snapshot(self, volume, snapshot, user, api_version=None):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            File=volume,
            Name=snapshot['name']
        )

        response = self._do_request('CreateSnapshot', **kwargs)
        snapshot_uuid = response['UUID']
        expect_status = [SnapshotStatus.Done]

        try:
            self._wait_snapshot_for_available(volume, snapshot_uuid, user,
                                              snapshot['volume_size'])
        except exception.CurveSnapshotCreateTimeOut:
            with excutils.save_and_reraise_exception():
                # NOTE(lyh): If snapshot failed, we must delete the snapshot
                self.delete_snapshot(volume, snapshot_uuid, user)

        return response

    def delete_snapshot(self, volume, snapshot_uuid, user, api_version=None):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            File=volume,
            UUID=snapshot_uuid
        )

        try:
            return self._do_request('DeleteSnapshot', **kwargs)
        except ImageBusy:
            raise exception.SnapshotIsBusy(snapshot_name=snapshot_uuid)

    def clean_clone_task(self, task_uuid, user, api_version=None):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            UUID=task_uuid
        )

        try:
            return self._do_request('CleanCloneTask', **kwargs)
        except Exception:
            pass

    def revert_to_snapshot(self, volume_path, snapshot_uuid, size, user,
                           api_version=None, lazy="false"):
        if api_version is None:
            api_version = self.default_api_version

        kwargs = dict(
            Version=api_version,
            User=user,
            Source=snapshot_uuid,
            Destination=volume_path,
            Lazy=lazy
        )

        response = self._do_request('Recover', **kwargs)
        task_uuid = response['UUID']
        expect_status = [CloneTaskStatus.Done]

        # NOTE(lyh): curve recover is similar with clone
        try:
            return self._wait_task_for_expect_status(task_uuid, expect_status,
                                                     user, size)
        except exception.CurveCloneTimeOut:
            with excutils.save_and_reraise_exception():
                self.clean_clone_task(task_uuid, user)
