# Copyright(C) Facebook, Inc. and its affiliates.
from collections import defaultdict, OrderedDict

try:
    import boto3
    from botocore.exceptions import ClientError as BotoClientError
except ImportError:
    boto3 = None
    BotoClientError = Exception
from pathlib import Path
import json
import shutil
import subprocess
import time
from time import sleep

from benchmark.utils import Print, BenchError, progress_bar
from benchmark.settings import Settings, SettingsError


# ── Retry helper for transient AWS API failures ──────────────────
_AWS_RETRIES = 8
_AWS_RETRY_BASE_DELAY = 2  # seconds, exponential backoff


def _aws_retry(action_name, fn, *args, **kwargs):
    """Call *fn(...)* with up to _AWS_RETRIES attempts on BotoClientError.
    Exponential backoff caps at 60s. Raises AWSError wrapped in BenchError
    after all retries are exhausted."""
    last = None
    for attempt in range(1, _AWS_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except BotoClientError as e:
            last = e
            if attempt == _AWS_RETRIES:
                break
            delay = min(_AWS_RETRY_BASE_DELAY ** attempt, 60)
            Print.warn(
                f'{action_name} failed (attempt {attempt}/{_AWS_RETRIES}): '
                f'{e.response["Error"]["Code"]}. Retrying in {delay}s...'
            )
            sleep(delay)
    raise BenchError(
        f'{action_name} failed after {_AWS_RETRIES} attempts',
        AWSError(last),
    )


class AWSError(Exception):
    def __init__(self, error):
        assert isinstance(error, BotoClientError)
        self.message = error.response["Error"]["Message"]
        self.code = error.response["Error"]["Code"]
        super().__init__(self.message)


class AWSInstanceManager:
    INSTANCE_NAME = "dag-node"
    SECURITY_GROUP_NAME = "dag"

    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings
        self.clients = OrderedDict()
        for region in settings.aws_regions:
            self.clients[region] = boto3.client("ec2", region_name=region)

    @classmethod
    def make(cls, settings_file="settings.json"):
        try:
            return cls(Settings.load(settings_file))
        except SettingsError as e:
            raise BenchError("Failed to load settings", e)

    def _get(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        ids, ips = defaultdict(list), defaultdict(list)
        for region, client in self.clients.items():
            r = _aws_retry(
                f'describe_instances in {region}',
                client.describe_instances,
                Filters=[
                    {"Name": "tag:Name", "Values": [self.INSTANCE_NAME]},
                    {"Name": "instance-state-name", "Values": state},
                ],
            )
            instances = [y for x in r["Reservations"] for y in x["Instances"]]
            for x in instances:
                ids[region] += [x["InstanceId"]]
                if "PublicIpAddress" in x:
                    ips[region] += [x["PublicIpAddress"]]
        return ids, ips

    def _wait(self, state):
        # Possible states are: 'pending', 'running', 'shutting-down',
        # 'terminated', 'stopping', and 'stopped'.
        while True:
            sleep(1)
            ids, _ = self._get(state)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _create_security_group(self, client):
        client.create_security_group(
            Description="HotStuff node",
            GroupName=self.SECURITY_GROUP_NAME,
        )

        client.authorize_security_group_ingress(
            GroupName=self.SECURITY_GROUP_NAME,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0",
                            "Description": "Debug SSH access",
                        }
                    ],
                    "Ipv6Ranges": [
                        {
                            "CidrIpv6": "::/0",
                            "Description": "Debug SSH access",
                        }
                    ],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": self.settings.base_port,
                    "ToPort": self.settings.base_port + 2_000,
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0",
                            "Description": "Dag port",
                        }
                    ],
                    "Ipv6Ranges": [
                        {
                            "CidrIpv6": "::/0",
                            "Description": "Dag port",
                        }
                    ],
                },
            ],
        )

    def _get_ami(self, client):
        # The AMI changes with regions and over time, thus we select the latest
        # available Ubuntu 22.04 image published by Canonical.
        name_patterns = [
            "ubuntu/images/hvm-ssd-gp3/ubuntu-jammy-22.04-amd64-server-*",
            "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*",
            "ubuntu/images/hvm-ssd*/ubuntu-jammy-22.04-amd64-server-*",
        ]
        images = []
        for pattern in name_patterns:
            response = client.describe_images(
                Owners=["099720109477"],
                Filters=[
                    {
                        "Name": "name",
                        "Values": [pattern],
                    },
                    {"Name": "state", "Values": ["available"]},
                    {"Name": "architecture", "Values": ["x86_64"]},
                    {"Name": "root-device-type", "Values": ["ebs"]},
                    {"Name": "virtualization-type", "Values": ["hvm"]},
                ],
            )
            images.extend(response.get("Images", []))

        unique_images = {}
        for image in images:
            image_id = image.get("ImageId")
            if image_id:
                unique_images[image_id] = image
        images = list(unique_images.values())

        if not images:
            region = client.meta.region_name
            message = (
                "Failed to find a compatible Ubuntu 22.04 AMI in region "
                f"{region}. Check EC2 Allowed AMIs policy and region availability."
            )
            raise BenchError(
                message,
                RuntimeError(message),
            )

        images.sort(key=lambda x: x.get("CreationDate", ""), reverse=True)
        return images[0]["ImageId"]

    def create_instances(self, instances):
        assert isinstance(instances, int) and instances > 0

        # Create the security group in every region.
        for client in self.clients.values():
            try:
                self._create_security_group(client)
            except BotoClientError as e:
                error = AWSError(e)
                if error.code != "InvalidGroup.Duplicate":
                    raise BenchError("Failed to create security group", error)

        # Track created instance IDs per region so we can rollback on failure.
        created_ids = defaultdict(list)

        try:
            # Create all instances.
            size = instances * len(self.clients)
            progress = progress_bar(
                self.clients.values(), prefix=f"Creating {size} instances"
            )
            for client in progress:
                region = client.meta.region_name
                result = _aws_retry(
                    f'run_instances in {region}',
                    client.run_instances,
                    ImageId=self._get_ami(client),
                    InstanceType=self.settings.instance_type,
                    KeyName=self.settings.key_name,
                    MaxCount=instances,
                    MinCount=instances,
                    SecurityGroups=[self.SECURITY_GROUP_NAME],
                    TagSpecifications=[
                        {
                            "ResourceType": "instance",
                            "Tags": [{"Key": "Name", "Value": self.INSTANCE_NAME}],
                        }
                    ],
                    EbsOptimized=True,
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {
                                "VolumeType": "gp2",
                                "VolumeSize": 200,
                                "DeleteOnTermination": True,
                            },
                        }
                    ],
                )
                created_ids[region] = [
                    inst["InstanceId"] for inst in result["Instances"]
                ]

            # Wait for the instances to boot.
            Print.info("Waiting for all instances to boot...")
            self._wait(["pending"])
            Print.heading(f"Successfully created {size} new instances")

        except (BenchError, BotoClientError) as e:
            # Rollback: terminate any instances already created.
            total_created = sum(len(ids) for ids in created_ids.values())
            if total_created > 0:
                Print.warn(
                    f'Rolling back {total_created} already-created instances '
                    f'(original error: {e})'
                )
                for region, ids in created_ids.items():
                    if ids:
                        client = self.clients[region]
                        try:
                            client.terminate_instances(InstanceIds=ids)
                        except BotoClientError:
                            pass
                # Wait for rollback terminates to complete.
                try:
                    self._wait(["shutting-down"])
                except (BenchError, BotoClientError):
                    pass
            if isinstance(e, BotoClientError):
                raise BenchError("Failed to create AWS instances", AWSError(e))
            raise

    def terminate_instances(self):
        try:
            ids, _ = self._get(["pending", "running", "stopping", "stopped"])
            size = sum(len(x) for x in ids.values())
            if size == 0:
                Print.heading(f"All instances are shut down")
                return

            # Terminate instances.
            for region, client in self.clients.items():
                if ids[region]:
                    _aws_retry(
                        f'terminate_instances in {region}',
                        client.terminate_instances,
                        InstanceIds=ids[region],
                    )

            # Wait for all instances to properly shut down.
            Print.info("Waiting for all instances to shut down...")
            self._wait(["shutting-down"])
            for client in self.clients.values():
                try:
                    client.delete_security_group(GroupName=self.SECURITY_GROUP_NAME)
                except BotoClientError:
                    pass

            Print.heading(f"Testbed of {size} instances destroyed")
        except BotoClientError as e:
            raise BenchError("Failed to terminate instances", AWSError(e))

    def start_instances(self, max):
        size = 0
        try:
            ids, _ = self._get(["stopping", "stopped"])
            for region, client in self.clients.items():
                if ids[region]:
                    target = ids[region]
                    target = target if len(target) < max else target[:max]
                    size += len(target)
                    _aws_retry(
                        f'start_instances in {region}',
                        client.start_instances,
                        InstanceIds=target,
                    )
            Print.heading(f"Starting {size} instances")
        except BotoClientError as e:
            raise BenchError("Failed to start instances", AWSError(e))

    def stop_instances(self):
        try:
            ids, _ = self._get(["pending", "running"])
            for region, client in self.clients.items():
                if ids[region]:
                    _aws_retry(
                        f'stop_instances in {region}',
                        client.stop_instances,
                        InstanceIds=ids[region],
                    )
            size = sum(len(x) for x in ids.values())
            Print.heading(f"Stopping {size} instances")
        except BotoClientError as e:
            raise BenchError(AWSError(e))

    def hosts(self, flat=False):
        try:
            _, ips = self._get(["pending", "running"])
            return [x for y in ips.values() for x in y] if flat else ips
        except BotoClientError as e:
            raise BenchError("Failed to gather instances IPs", AWSError(e))

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ""
        for region, ips in hosts.items():
            text += f"\n Region: {region.upper()}\n"
            for i, ip in enumerate(ips):
                new_line = "\n" if (i + 1) % 6 == 0 else ""
                text += f"{new_line} {i}\tssh -i {key} ubuntu@{ip}\n"
        print(
            "\n"
            "----------------------------------------------------------------\n"
            " INFO:\n"
            "----------------------------------------------------------------\n"
            f" Available machines: {sum(len(x) for x in hosts.values())}\n"
            f"{text}"
            "----------------------------------------------------------------\n"
        )


class GCPInstanceManager:
    INSTANCE_NAME = 'dag-node'
    FIREWALL_RULE_NAME = 'dag'

    def __init__(self, settings):
        assert isinstance(settings, Settings)
        self.settings = settings

    def _gcloud(self, args, check=True):
        cmd = ['gcloud'] + args
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if check and proc.returncode != 0:
            message = (
                f"gcloud command failed: {' '.join(cmd)}\n"
                f"exit={proc.returncode}\n"
                f"STDOUT:\n{proc.stdout}\n"
                f"STDERR:\n{proc.stderr}"
            )
            raise BenchError('Failed to execute gcloud command', RuntimeError(message))
        return proc

    def _ensure_gcloud_cli(self):
        if shutil.which('gcloud') is None:
            raise BenchError(
                'gcloud CLI is not installed or not in PATH',
                RuntimeError('Install Google Cloud SDK and run: gcloud auth login'),
            )

    def _ensure_project(self):
        if not self.settings.gcp_project:
            raise BenchError(
                'Missing GCP project in settings',
                RuntimeError('Set instances.project or gcp.project in settings.json'),
            )

    def _zones(self):
        zones = list(self.settings.gcp_zones or [])
        if not zones:
            raise BenchError(
                'Missing GCP zones in settings',
                RuntimeError('Set instances.zones in settings.json'),
            )
        return zones

    def _read_public_key(self):
        key_path = Path(self.settings.key_path).expanduser()
        pub_path = Path(str(key_path) + '.pub')
        if not pub_path.exists():
            return ''
        content = pub_path.read_text(encoding='utf-8').strip()
        return content

    def _list_instances(self):
        self._ensure_project()
        proc = self._gcloud(
            [
                'compute',
                'instances',
                'list',
                '--project',
                self.settings.gcp_project,
                '--filter',
                f'labels.name={self.INSTANCE_NAME}',
                '--format',
                'json(name,id,zone,status,networkInterfaces[0].accessConfigs[0].natIP)',
            ]
        )
        try:
            data = json.loads(proc.stdout or '[]')
        except json.JSONDecodeError as e:
            raise BenchError('Failed to parse gcloud instance list output', e)
        return data

    def _get(self, states):
        state_set = {x.upper() for x in states}
        ids, ips = defaultdict(list), defaultdict(list)
        for item in self._list_instances():
            status = str(item.get('status', '')).upper()
            if status not in state_set:
                continue
            zone_uri = str(item.get('zone', ''))
            zone = zone_uri.split('/')[-1] if zone_uri else 'unknown'
            name = str(item.get('name', ''))
            if name:
                ids[zone].append(name)

            net_ifaces = item.get('networkInterfaces') or []
            if net_ifaces:
                access_cfgs = net_ifaces[0].get('accessConfigs') or []
                if access_cfgs and access_cfgs[0].get('natIP'):
                    ips[zone].append(str(access_cfgs[0]['natIP']))
        return ids, ips

    def _wait_until_absent(self, states):
        target = {x.upper() for x in states}
        while True:
            sleep(1)
            ids, _ = self._get(target)
            if sum(len(x) for x in ids.values()) == 0:
                break

    def _ensure_firewall_rule(self):
        describe = self._gcloud(
            [
                'compute',
                'firewall-rules',
                'describe',
                self.FIREWALL_RULE_NAME,
                '--project',
                self.settings.gcp_project,
            ],
            check=False,
        )
        if describe.returncode == 0:
            return

        self._gcloud(
            [
                'compute',
                'firewall-rules',
                'create',
                self.FIREWALL_RULE_NAME,
                '--project',
                self.settings.gcp_project,
                '--network',
                self.settings.gcp_network,
                '--allow',
                f'tcp:22,tcp:{self.settings.base_port}-{self.settings.base_port + 2000}',
                '--source-ranges',
                '0.0.0.0/0',
                '--target-tags',
                self.FIREWALL_RULE_NAME,
            ]
        )

    def create_instances(self, instances):
        assert isinstance(instances, int) and instances > 0
        self._ensure_gcloud_cli()
        self._ensure_project()
        zones = self._zones()
        self._ensure_firewall_rule()

        ssh_key = self._read_public_key()
        size = instances * len(zones)
        progress = progress_bar(zones, prefix=f'Creating {size} instances')
        for zone in progress:
            for i in range(instances):
                name = f'{self.INSTANCE_NAME}-{zone}-{int(time.time())}-{i}'
                cmd = [
                    'compute',
                    'instances',
                    'create',
                    name,
                    '--project',
                    self.settings.gcp_project,
                    '--zone',
                    zone,
                    '--machine-type',
                    self.settings.instance_type,
                    '--network',
                    self.settings.gcp_network,
                    '--tags',
                    self.FIREWALL_RULE_NAME,
                    '--labels',
                    f'name={self.INSTANCE_NAME}',
                    '--image-project',
                    self.settings.gcp_image_project,
                    '--image-family',
                    self.settings.gcp_image_family,
                    '--boot-disk-size',
                    f'{self.settings.gcp_disk_size_gb}GB',
                    '--boot-disk-type',
                    'pd-ssd',
                ]
                if self.settings.gcp_subnetwork:
                    cmd.extend(['--subnet', self.settings.gcp_subnetwork])
                if ssh_key:
                    cmd.extend(['--metadata', f'ssh-keys=ubuntu:{ssh_key}'])
                self._gcloud(cmd)

        Print.info('Waiting for all instances to boot...')
        self._wait_until_absent(['PROVISIONING', 'STAGING'])
        Print.heading(f'Successfully created {size} new instances')

    def terminate_instances(self):
        self._ensure_gcloud_cli()
        ids, _ = self._get(['PROVISIONING', 'STAGING', 'RUNNING', 'STOPPING', 'TERMINATED'])
        size = sum(len(x) for x in ids.values())
        if size == 0:
            Print.heading('All instances are shut down')
            return

        for zone, names in ids.items():
            for name in names:
                self._gcloud(
                    [
                        'compute',
                        'instances',
                        'delete',
                        name,
                        '--project',
                        self.settings.gcp_project,
                        '--zone',
                        zone,
                        '--quiet',
                    ]
                )

        Print.heading(f'Testbed of {size} instances destroyed')

    def start_instances(self, max):
        self._ensure_gcloud_cli()
        size = 0
        ids, _ = self._get(['STOPPING', 'TERMINATED'])
        for zone, names in ids.items():
            target = names if len(names) < max else names[:max]
            for name in target:
                self._gcloud(
                    [
                        'compute',
                        'instances',
                        'start',
                        name,
                        '--project',
                        self.settings.gcp_project,
                        '--zone',
                        zone,
                    ]
                )
                size += 1
        Print.heading(f'Starting {size} instances')

    def stop_instances(self):
        self._ensure_gcloud_cli()
        ids, _ = self._get(['PROVISIONING', 'STAGING', 'RUNNING'])
        size = 0
        for zone, names in ids.items():
            for name in names:
                self._gcloud(
                    [
                        'compute',
                        'instances',
                        'stop',
                        name,
                        '--project',
                        self.settings.gcp_project,
                        '--zone',
                        zone,
                    ]
                )
                size += 1
        Print.heading(f'Stopping {size} instances')

    def hosts(self, flat=False):
        _, ips = self._get(['PROVISIONING', 'STAGING', 'RUNNING'])
        return [x for y in ips.values() for x in y] if flat else ips

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.key_path
        text = ''
        for zone, ips in hosts.items():
            text += f"\n Zone: {zone}\n"
            for i, ip in enumerate(ips):
                new_line = '\n' if (i + 1) % 6 == 0 else ''
                text += f"{new_line} {i}\tssh -i {key} ubuntu@{ip}\n"
        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' INFO:\n'
            '----------------------------------------------------------------\n'
            f" Available machines: {sum(len(x) for x in hosts.values())}\n"
            f'{text}'
            '----------------------------------------------------------------\n'
        )


class InstanceManager:
    def __init__(self, backend):
        self.backend = backend
        self.settings = backend.settings

    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            settings = Settings.load(settings_file)
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

        if settings.cloud_provider == 'gcp':
            return cls(GCPInstanceManager(settings))
        return cls(AWSInstanceManager(settings))

    def create_instances(self, instances):
        return self.backend.create_instances(instances)

    def terminate_instances(self):
        return self.backend.terminate_instances()

    def start_instances(self, max):
        return self.backend.start_instances(max)

    def stop_instances(self):
        return self.backend.stop_instances()

    def hosts(self, flat=False):
        return self.backend.hosts(flat=flat)

    def print_info(self):
        return self.backend.print_info()
