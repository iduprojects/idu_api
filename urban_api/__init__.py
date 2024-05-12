"""
This is a Digital Territories Platform API to access and manipulate basic territories' data.
"""

__all__ = [
    "app",
    "__version__",
]

from .fastapi_init import app
from .version import VERSION as __version__
