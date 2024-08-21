"""Api routers are defined here.

It is needed to import files which use these routers to initialize handlers.
"""


from .territories import routers_list as territories_routers

routers_list = [
    *territories_routers,
]

__all__ = [
    "routers_list",
]
