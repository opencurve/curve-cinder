#    Copyright 2019 NetEase, Inc.
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

import socket

from eventlet import tpool
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from cinder import exception
from cinder.volume import driver
from cinder.volume.drivers.curve import client as curvefs_client

LOG = logging.getLogger(__name__)


cbd_opts = [
    cfg.StrOpt('curve_pool',
               default='pool1',
               help='the storage pool in which volume images are stored'),
    cfg.StrOpt('volume_dir',
               default='/cinder',
               help='the curve directory in which volumes are created.'),
    cfg.StrOpt('curve_user',
               default='cinder',
               help='the user of curve images which created by cinder'),
    cfg.StrOpt('curve_client_conf',
               default='/etc/curve/client.conf',
               help='path to the curve configuration file to use'),
    cfg.StrOpt('cbd_snapshot_api_server',
               default="127.0.0.1:5555",
               help='The curvefs snapshot/clone server address.'),
    cfg.StrOpt('cbd_snapshot_api_version',
               default="2018-01-01",
               help="The version of cbd snapshot/clone api."),
    cfg.StrOpt('cbd_snapshot_prefix_url',
               default="SnapshotCloneService",
               help="The url prefix of curve snapshot api server."),
    cfg.IntOpt('wait_cbd_task_timeout',
               default=600,
               help="The timeout wait cbd task come into the expect status."),
]


class CBDDriver(driver.VolumeDriver):

    def __init__(self, *args, **kwargs):
        super(CBDDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(cbd_opts)
        self.hostname = socket.gethostname()
        self.cbd_client = curvefs_client.CBDClient(self.configuration)

    def check_for_setup_error(self):
        #NOTE(lyh): make sure we can create dir, which used to
        #           store cinder volumes, if create dir failed
        #           means curefs not ready and marked
        #           cinder-volume as down.
        curvefsclient = curvefs_client.CurveFS(self.configuration)
        try:
            curvefsclient.mkdir(self.configuration.volume_dir)
        except curvefs_client.ImageExists:
            pass

    def CBDProxy(self):
        return tpool.Proxy(curvefs_client.CurveFS(self.configuration))

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """
        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""

        LOG.debug("Updating volume stats of CBDDriver")
        data = {}
        backend_name = self.configuration.safe_get('volume_backend_name')
        data["volume_backend_name"] = backend_name or 'CURVE'
        data["storage_pool"] = self.configuration.curve_pool
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = '1.0'
        data["storage_protocol"] = 'curve'

        data['total_capacity_gb'] = 'infinite'
        data['free_capacity_gb'] = 'infinite'
        data['reserved_percentage'] = 0
        data['QoS_support'] = False
        self._stats = data

    def _get_fsid(self):
        return self.CBDProxy().get_fsid()

    def create_volume(self, volume):
        LOG.debug("creating volume '%s'" % (volume['name']))

        size = int(volume['size']) * units.Gi
        volume_path = "%s/%s" % (self.configuration.volume_dir, volume['name'])
        self.CBDProxy().create(volume_path, size)

        provider_location = "cbd://%s/%s" % (self._get_fsid(),
                                             self.configuration.curve_pool)
        return {'provider_location': provider_location}

    def _resize(self, volume):
        size = int(volume['size']) * units.Gi
        volume_path = "%s/%s" % (self.configuration.volume_dir, volume['name'])
        self.CBDProxy().extend(volume_path, size)
        LOG.debug("resize volume %(volume_path)s to %(new_size)s GB."
                  % dict(volume_path=volume_path, new_size=volume['size']))

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        user = self.configuration.curve_user

        snapshot_uuid = self._get_snapshot_uuid(snapshot)
        volume_path = "/%s/%s" % (user, volume['name'])
        if snapshot_uuid:
            self.cbd_client.clone(snapshot_uuid, volume_path, user,
                                  snapshot['volume_size'])

            if int(volume['size']) > int(snapshot['volume_size']):
                self._resize(volume)

            provider_location = "cbd://%s/%s" % (self._get_fsid(),
                                                 self.configuration.curve_pool)
            return {'provider_location': provider_location}

        msg = "snapshot %s does't has backend uuid" % snapshot_uuid
        raise exception.InvalidSnapshot(msg)

    def delete_volume(self, volume):
        volume_path = "%s/%s" % (self.configuration.volume_dir, volume['name'])
        try:
            self.CBDProxy().delete(volume_path)
        except curvefs_client.ImageNotFound:
            LOG.debug("curve volume %s doesn't existed" % volume_path)

    def create_snapshot(self, snapshot):
        user = self.configuration.curve_user
        volume_path = "/%s/%s" % (user, snapshot['volume_name'])
        response = self.cbd_client.snapshot(volume_path, snapshot, user)

        #NOTE(lyh): Curve allow snapshot's name duplicate, and snapshot's
        #           UUID is unique, so we need save its UUID and use the UUID
        #           to clone image from snapshot or delete snapshot.
        #           I save the snapshot's UUID into provider_location, this
        #           is a bad idea but also simplest.
        provider_location = "cbd://%s/%s/%s" % (self._get_fsid(),
                                                self.configuration.curve_pool,
                                                response['UUID'])
        return {'provider_location': provider_location}

    def _get_snapshot_uuid(self, snapshot):
        prefix = "cbd://"
        if (snapshot['provider_location'] is not None and
                snapshot['provider_location'].startswith(prefix)):
            try:
                pieces = snapshot['provider_location'][len(prefix):].split('/')
                return pieces[2]
            except Exception:
                return None

        return None

    def delete_snapshot(self, snapshot):
        user = self.configuration.curve_user
        volume_path = "/%s/%s" % (user, snapshot['volume_name'])

        # NOTE(lyh): extract Curve snapshot's UUID form provider_location
        snapshot_uuid = self._get_snapshot_uuid(snapshot)
        if snapshot_uuid is not None:
            self.cbd_client.delete_snapshot(volume_path, snapshot_uuid, user)

    def create_export(self, context, volume):
        pass

    def remove_export(self, context, volume):
        pass

    def ensure_export(self, context, volume):
        pass

    def initialize_connection(self, volume, connector):
        volume_path = "%s/%s" % (self.configuration.volume_dir, volume['name'])
        name = ('%(poolid)s/%(volume_path)s_%(user)s_:%(client_conf)s' %
                dict(poolid=self.configuration.curve_pool,
                     volume_path=volume_path,
                     user=self.configuration.curve_user,
                     client_conf=self.configuration.curve_client_conf))
        data = {
            'driver_volume_type': 'cbd',
            'data': {
                'name': name,
            }
        }
        LOG.debug('connection data: %s' % data)
        return data

    def terminate_connection(self, volume, connector, **kwargs):
        pass

    def extend_volume(self, volume, new_size):
        """Extend an existing volume."""
        old_size = volume['size']
        size = int(new_size) * units.Gi
        volume_path = "%s/%s" % (self.configuration.volume_dir, volume['name'])
        self.CBDProxy().extend(volume_path, size)
        LOG.debug("Extend volume from %(old_size)s GB to %(new_size)s GB.",
                  {'old_size': old_size, 'new_size': new_size})

    def revert_to_snapshot(self, context, volume, snapshot):
        user = self.configuration.curve_user
        volume_path = "%s/%s" % (self.configuration.volume_dir, volume['name'])
        snapshot_uuid = self._get_snapshot_uuid(snapshot)
        if snapshot_uuid is None:
            msg = "snapshot %s does't has backend uuid" % snapshot_uuid
            raise exception.InvalidSnapshot(msg)

        self.cbd_client.revert_to_snapshot(volume_path, snapshot_uuid,
                                           volume['size'], user)
