# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError
from pathlib import Path


class SettingsError(Exception):
    pass


class Settings:
    def __init__(self, key_name, key_path, base_port, repo_name, repo_url,
                 branch, instance_type, aws_regions, cloud_provider='aws',
                 gcp_project='', gcp_zones=None, gcp_network='default',
                 gcp_subnetwork='', gcp_image_project='ubuntu-os-cloud',
                 gcp_image_family='ubuntu-2204-lts', gcp_disk_size_gb=200):
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

        if gcp_zones is None:
            gcp_zones = []
        ok &= isinstance(gcp_zones, list)
        ok &= all(isinstance(x, str) for x in gcp_zones)
        if not ok:
            raise SettingsError('Invalid settings types')

        self.key_name = key_name
        self.key_path = key_path

        self.base_port = base_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance_type = instance_type
        self.aws_regions = regions
        self.cloud_provider = cloud_provider

        self.gcp_project = gcp_project
        self.gcp_zones = gcp_zones
        self.gcp_network = gcp_network or 'default'
        self.gcp_subnetwork = gcp_subnetwork
        self.gcp_image_project = gcp_image_project or 'ubuntu-os-cloud'
        self.gcp_image_family = gcp_image_family or 'ubuntu-2204-lts'
        self.gcp_disk_size_gb = gcp_disk_size_gb

        self.cloud_locations = self.aws_regions if cloud_provider == 'aws' else self.gcp_zones

    @classmethod
    def load(cls, filename):
        try:
            path = cls.resolve_path(filename)
            with open(path, 'r') as f:
                data = load(f)

            provider = str(data.get('provider', 'aws')).strip().lower()
            instances = data['instances']

            gcp = data.get('gcp', {}) if isinstance(data.get('gcp', {}), dict) else {}
            gcp_project = str(instances.get('project', gcp.get('project', '')))
            gcp_zones = instances.get('zones', gcp.get('zones', instances.get('regions', [])))
            gcp_network = str(instances.get('network', gcp.get('network', 'default')))
            gcp_subnetwork = str(instances.get('subnetwork', gcp.get('subnetwork', '')))
            gcp_image_project = str(instances.get('image_project', gcp.get('image_project', 'ubuntu-os-cloud')))
            gcp_image_family = str(instances.get('image_family', gcp.get('image_family', 'ubuntu-2204-lts')))
            gcp_disk_size_gb = int(instances.get('disk_size_gb', gcp.get('disk_size_gb', 200)))

            return cls(
                data['key']['name'],
                data['key']['path'],
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
