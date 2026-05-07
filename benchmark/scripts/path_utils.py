"""ExperimentPaths: timestamped output directories for benchmark runs."""
from datetime import datetime
from pathlib import Path


class ExperimentPaths:
    """Auto-generates timestamped experiment directories under logs/ and csv_plots/."""

    def __init__(self, experiment_id=None, base_dir=None):
        if base_dir is None:
            base_dir = Path(__file__).resolve().parent.parent
        self.base_dir = Path(base_dir)
        if experiment_id is None:
            experiment_id = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        self.experiment_id = experiment_id

    @property
    def logs_dir(self):
        d = self.base_dir / 'logs' / self.experiment_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    @property
    def csv_plots_dir(self):
        d = self.base_dir / 'csv_plots' / self.experiment_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def rate_logs_dir(self, rate):
        d = self.logs_dir / f'rate-{rate}'
        d.mkdir(parents=True, exist_ok=True)
        return d

    def csv_path(self, name):
        return self.csv_plots_dir / f'{name}.csv'

    def plot_path(self, name, ext):
        return self.csv_plots_dir / f'{name}.{ext}'
