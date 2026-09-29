"""Domain exceptions raised by the orchestrator control plane."""

from __future__ import annotations


class OrchestratorError(Exception):
    """Base class for orchestrator errors."""


class TaskNotFoundError(OrchestratorError):
    pass


class NodeNotFoundError(OrchestratorError):
    pass


class DuplicateTaskError(OrchestratorError):
    pass


class LeaseNotFoundError(OrchestratorError):
    pass


class LeaseExpiredError(OrchestratorError):
    pass


class FencingError(OrchestratorError):
    """Raised when a stale lease holder presents an old fencing token."""


class StorageCorruptionError(OrchestratorError):
    pass


class CompatibilityError(OrchestratorError):
    pass
