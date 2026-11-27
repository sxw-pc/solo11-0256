from . import manifest,retention,eviction,intake
MODULES={'manifest':manifest,'retention':retention,'eviction':eviction,'intake':intake}

# Convenience re-exports for the offline intake/recovery entry points.
from .intake import (  # noqa: E402
	IntakeService, receive_package, recover_intake, build_package,
	build_startable, receive, recover,
)

def __getattr__(name):
	# Any other intake-flavoured name resolves through intake's tolerant factory.
	if name.startswith('__'):
		raise AttributeError(name)
	return getattr(intake, name)
