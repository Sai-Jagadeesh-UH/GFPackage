"""Pipeline registry — discover and register pipeline modules.

Pipelines register themselves by name. The top-level runner
looks up the appropriate pipeline runner from this registry.

Usage:
    from app.registry import registry

    # Registration (done in each pipeline's __init__.py):
    registry.register("enbridge", EnbridgeRunner)

    # Lookup:
    runner_cls = registry.get("enbridge")
"""

from __future__ import annotations

from typing import Any

from app.core.logging import logger


class PipelineRegistry:
    """Central registry mapping pipeline names to their runner classes."""

    def __init__(self) -> None:
        self._runners: dict[str, type] = {}

    def register(self, name: str, runner_cls: type) -> None:
        """Register a pipeline runner class by name.

        Args:
            name: Pipeline name (e.g. "enbridge"), case-insensitive.
            runner_cls: The runner class (e.g. EnbridgeRunner).
        """
        key = name.lower()
        if key in self._runners:
            logger.warning(f"Pipeline '{key}' already registered — overwriting")
        self._runners[key] = runner_cls
        logger.info(f"Pipeline '{key}' registered")

    def get(self, name: str) -> type | None:
        """Look up a pipeline runner class by name.

        Returns None if not registered.
        """
        return self._runners.get(name.lower())

    def get_or_raise(self, name: str) -> type:
        """Look up a pipeline runner class, raising if not found."""
        cls = self.get(name)
        if cls is None:
            available = ", ".join(sorted(self._runners.keys())) or "(none)"
            raise KeyError(
                f"Pipeline '{name}' not registered. Available: {available}"
            )
        return cls

    @property
    def available(self) -> list[str]:
        """List all registered pipeline names."""
        return sorted(self._runners.keys())

    def __contains__(self, name: str) -> bool:
        return name.lower() in self._runners

    def __len__(self) -> int:
        return len(self._runners)


# Singleton registry instance
registry = PipelineRegistry()


def _auto_register() -> None:
    """Auto-register known pipelines.

    Called on import. Each pipeline module is imported and its runner
    class is registered. Add new pipelines here as they are created.
    """
    try:
        from app.pipelines.enbridge import EnbridgeRunner
        registry.register("enbridge", EnbridgeRunner)
    except ImportError:
        logger.debug("Enbridge pipeline not available")


_auto_register()
