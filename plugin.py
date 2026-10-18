"""SDCodex-GalleryDL entrypoint (Stitch UI + slim JSON API)."""

import importlib.util
import os
import sys

plugin_dir = os.path.dirname(os.path.abspath(__file__))
_BACKEND_NAME = "_sdcodex_gallerydl_backend"


def _load_backend():
    """Import ``plugin_backend.py`` under a plugin-private module name.

    Several plugins ship a module literally called ``plugin_backend`` (AI
    Renamer does). The core loads plugins alphabetically, and a plain
    ``from plugin_backend import gdl_api`` binds whichever copy reached
    ``sys.modules`` first — for AI Renamer that raised ImportError, the
    blueprint was never registered, and every ``/gdl/api`` call fell through
    to the SPA handler, so the tasks page showed "No tasks yet".
    """
    if _BACKEND_NAME in sys.modules:
        return sys.modules[_BACKEND_NAME]
    path = os.path.join(plugin_dir, "plugin_backend.py")
    spec = importlib.util.spec_from_file_location(_BACKEND_NAME, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load GalleryDL backend from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[_BACKEND_NAME] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(_BACKEND_NAME, None)
        raise
    return module


def init_plugin(app, db, plugin_info=None):
    """Register the /gdl/api blueprint. Static pages are served by core."""
    backend = _load_backend()

    if "gdl_api" not in app.blueprints:
        app.register_blueprint(backend.gdl_api)
    backend._init_scheduler()
    return True
