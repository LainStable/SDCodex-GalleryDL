"""SDCodex-GalleryDL entrypoint (Stitch UI + slim JSON API)."""

import os
import sys

plugin_dir = os.path.dirname(os.path.abspath(__file__))
if plugin_dir not in sys.path:
    sys.path.insert(0, plugin_dir)


def init_plugin(app, db, plugin_info=None):
    """Register the /gdl/api blueprint. Static pages are served by core."""
    from plugin_backend import gdl_api

    if "gdl_api" not in app.blueprints:
        app.register_blueprint(gdl_api)
    return True
