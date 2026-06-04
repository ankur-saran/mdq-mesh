"""Root conftest: make the project root a Python import root.

This allows tests to import from `harness.*` (which lives at the project root,
not under src/, per PRD §14).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root so `import harness` works without installing harness as a package
sys.path.insert(0, str(Path(__file__).parent))
