"""
Jano Plugin — EventListener
No DCS events are handled in this version.
This file exists to satisfy the DCSServerBot plugin structure.
"""
from core import EventListener
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .commands import Jano


class JanoEventListener(EventListener["Jano"]):
    """Placeholder listener. Extend here if DCS event integration is needed later."""
    pass
