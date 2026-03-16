"""Configuration for sample data generation."""

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class GenerationConfig:
    """Configuration for sample data generation."""

    num_members: int = 10000  # Base number of members in OutreachList
    relationship_density: float = 0.7  # % of optional relationships populated
    temporal_range_days: int = 365  # Date range for temporal fields
    null_percentage: dict[str, float] = field(default_factory=dict)

    # Random seed for reproducible generation (None = random)
    random_seed: int | None = 42

    # Key pool configuration for joinable foreign keys
    key_pool_size: int = 500  # Number of unique keys in the pool
    key_distribution_80_20: bool = True  # Use 80/20 distribution pattern
    high_frequency_key_ratio: float = 0.8  # Portion of references to top 20% of keys

    # Reference date for deterministic generation (None = auto-select based on seed)
    reference_date: datetime | None = None

    def get_reference_date(self) -> datetime:
        """Get reference date for deterministic generation.

        When random_seed is set, returns a fixed date for reproducibility.
        Otherwise returns the current datetime.
        """
        if self.reference_date is not None:
            return self.reference_date
        if self.random_seed is not None:
            # Fixed reference date for deterministic generation
            return datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
        return datetime.now(tz=UTC)


__all__ = ["GenerationConfig"]
