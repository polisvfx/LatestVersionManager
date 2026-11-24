"""Domain controller mixins composed into MainWindow."""

from app.controllers.project import ProjectControllerMixin
from app.controllers.scan import ScanControllerMixin
from app.controllers.promote import PromoteControllerMixin
from app.controllers.auto_promote import AutoPromoteControllerMixin
from app.controllers.nle_sync import NleSyncControllerMixin

__all__ = [
    "ProjectControllerMixin",
    "ScanControllerMixin",
    "PromoteControllerMixin",
    "AutoPromoteControllerMixin",
    "NleSyncControllerMixin",
]
