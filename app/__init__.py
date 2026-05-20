"""Latest Version Manager - PySide6 GUI Application package.

This package was split out of the original monolithic app.py module.
"""

from app._common import main, APP_NAME, APP_VERSION  # noqa: F401
from app.widgets import (  # noqa: F401
    VersionTreeWidget, FlowLayout, TagWidget, TagInputWidget,
    CollapsibleSection, SourceItemDelegate,
)
from app.workers import (  # noqa: F401
    PromoteWorker, ThumbnailWorker, UpdateCheckWorker, UpdateDownloadWorker,
    ScanWorker, StatusWorker, SyncNamesWorker, ProjectLoadWorker,
)
from app.dialogs.dry_run import DryRunDialog  # noqa: F401
from app.dialogs.source import SourceDialog  # noqa: F401
from app.dialogs.project_setup import ProjectSetupDialog  # noqa: F401
from app.dialogs.settings import ProjectSettingsDialog  # noqa: F401
from app.dialogs.latest_path import LatestPathDialog  # noqa: F401
from app.dialogs.naming_rule import NamingRuleDialog  # noqa: F401
from app.dialogs.discovery import DiscoveryWorker, DiscoveryDialog  # noqa: F401
from app.dialogs.manage_groups import ManageGroupsDialog  # noqa: F401
from app.dialogs.update import UpdateDialog  # noqa: F401
from app.dialogs.about import AboutDialog  # noqa: F401
from app.dialogs.batch_promote import BatchPromoteReviewDialog  # noqa: F401
from app.dialogs.obsolete_layer import ObsoleteLayerDialog  # noqa: F401
from app.main_window import MainWindow  # noqa: F401
