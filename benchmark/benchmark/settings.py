# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError
import os
from pathlib import Path


class SettingsError(Exception):
    pass


class Settings:
    def __init__(self, key_name, key_path, base_port, repo_name, repo_url,
                 branch, instance_type, aws_regions, cloud_provider='aws',
                 gcp_project='', gcp_zones=None, gcp_network='default',
                 gcp_subnetwork='', gcp_image_project='ubuntu-os-cloud',
                 gcp_image_family='ubuntu-2204-lts', gcp_disk_size_gb=200,
                 ssh_user='ubuntu', gcp_regions=None,
                 gcp_instance_name='dag-node', gcp_firewall_rule='dag'):
        inputs_str = [
            key_name, key_path, repo_name, repo_url, branch, instance_type
        ]
        if isinstance(aws_regions, list):
            regions = aws_regions
        else:
            regions = [aws_regions]
        inputs_str += regions
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= isinstance(base_port, int)
        ok &= len(regions) > 0
        ok &= cloud_provider in ('aws', 'gcp')
        ok &= isinstance(gcp_project, str)
        ok &= isinstance(gcp_network, str)
        ok &= isinstance(gcp_subnetwork, str)
        ok &= isinstance(gcp_image_project, str)
        ok &= isinstance(gcp_image_family, str)
        ok &= isinstance(gcp_disk_size_gb, int) and gcp_disk_size_gb > 0
        ok &= isinstance(ssh_user, str) and bool(ssh_user)
        ok &= isinstance(gcp_instance_name, str) and bool(gcp_instance_name)
        ok &= isinstance(gcp_firewall_rule, str) and bool(gcp_firewall_rule)

        if gcp_zones is None:
            gcp_zones = []
        if gcp_regions is None:
            gcp_regions = []
        ok &= isinstance(gcp_zones, list)
        ok &= all(isinstance(x, str) for x in gcp_zones)
        ok &= isinstance(gcp_regions, list)
        ok &= all(isinstance(x, str) for x in gcp_regions)
        if not ok:
            raise SettingsError('Invalid settings types')

        self.key_name = key_name
        self.key_path = key_path
        self.ssh_user = ssh_user

        self.base_port = base_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance_type = instance_type
        self.aws_regions = regions
        self.cloud_provider = cloud_provider

        self.gcp_project = gcp_project
        self.gcp_regions = gcp_regions
        self.gcp_zones = gcp_zones
        self.gcp_network = gcp_network or 'default'
        self.gcp_subnetwork = gcp_subnetwork
        self.gcp_image_project = gcp_image_project or 'ubuntu-os-cloud'
        self.gcp_image_family = gcp_image_family or 'ubuntu-2204-lts'
        self.gcp_disk_size_gb = gcp_disk_size_gb
        self.gcp_instance_name = gcp_instance_name
        self.gcp_firewall_rule = gcp_firewall_rule

        self.cloud_locations = self.aws_regions if cloud_provider == 'aws' else (self.gcp_zones or self.gcp_regions)

    @classmethod
    def load(cls, filename):
        try:
            path = cls.resolve_path(os.environ.get('BENCHMARK_SETTINGS', filename))
            with open(path, 'r') as f:
                data = load(f)

            provider = str(data.get('provider', 'aws')).strip().lower()
            instances = data['instances']
            key = data['key']

            gcp = data.get('gcp', {}) if isinstance(data.get('gcp', {}), dict) else {}
            ssh_user = str(instances.get('ssh_user', key.get('user', gcp.get('ssh_user', 'ubuntu'))))
            gcp_project = str(instances.get('project', gcp.get('project', '')))
            gcp_regions = instances.get(
                'gcp_regions',
                gcp.get('regions', instances.get('regions', []) if provider == 'gcp' else []),
            )
            gcp_zones = instances.get('zones', gcp.get('zones', []))
            gcp_network = str(instances.get('network', gcp.get('network', 'default')))
            gcp_subnetwork = str(instances.get('subnetwork', gcp.get('subnetwork', '')))
            gcp_image_project = str(instances.get('image_project', gcp.get('image_project', 'ubuntu-os-cloud')))
            gcp_image_family = str(instances.get('image_family', gcp.get('image_family', 'ubuntu-2204-lts')))
            gcp_disk_size_gb = int(instances.get('disk_size_gb', gcp.get('disk_size_gb', 200)))
            gcp_instance_name = str(instances.get('name', gcp.get('instance_name', 'dag-node')))
            gcp_firewall_rule = str(instances.get('firewall_rule', gcp.get('firewall_rule', 'dag')))

            return cls(
                key['name'],
                key['path'],
                data['port'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                instances['type'],
                instances.get('regions', gcp_zones),
                provider,
                gcp_project,
                gcp_zones,
                gcp_network,
                gcp_subnetwork,
                gcp_image_project,
                gcp_image_family,
                gcp_disk_size_gb,
                ssh_user,
                gcp_regions,
                gcp_instance_name,
                gcp_firewall_rule,
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')

    @staticmethod
    def resolve_path(filename):
        path = Path(filename).expanduser()
        if path.is_absolute() and path.exists():
            return path
        if path.exists():
            return path

        benchmark_root = Path(__file__).resolve().parent.parent
        candidates = [
            benchmark_root / filename,
            benchmark_root / 'scripts' / filename,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate

        raise SettingsError(
            f'Settings file not found: {filename}. Looked in cwd, '
            f'{benchmark_root}, and {benchmark_root / "scripts"}'
        )
