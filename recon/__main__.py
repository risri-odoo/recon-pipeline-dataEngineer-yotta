"""Entry point for `python -m recon`; delegates to recon.presentation.cli."""

import sys

from recon.presentation.cli import main

sys.exit(main())
