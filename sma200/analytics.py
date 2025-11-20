class Analytics:
    """Registry for analytics strategies."""

    def __init__(self, config: dict):
        self.config = config
        # map strategy names -> callables
        self._registry = {"sma": self.sma_with_threshold}

    def register(self, name: str, fn) -> None:
        """Register a strategy function."""
        self._registry[name] = fn

    def get(self, name: str):
        """Get a strategy by name, raises KeyError if not found."""
        try:
            return self._registry[name]
        except KeyError:
            raise KeyError(f"Strategy '{name}' not configured or loaded")

    def exists(self, name: str) -> bool:
        """Check if a strategy exists."""
        return name in self._registry

    def names(self) -> list[str]:
        """List registered strategy names."""
        return list(self._registry)

    def execute(self, name: str, df):
        """Execute a registered strategy."""
        return self.get(name)(df)

    # Example strategy implementation
    def sma_with_threshold(self, df):
        params = self.config.get("sma", {})
        return {"type": "sma", "params": params}
