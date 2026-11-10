import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str = "miniAPM"
    environment: str = "local"
    host: str = "127.0.0.1"
    port: int = 8000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_name=os.getenv("MINIAPM_APP_NAME", "miniAPM"),
            environment=os.getenv("MINIAPM_ENVIRONMENT", "local"),
            host=os.getenv("MINIAPM_HOST", "127.0.0.1"),
            port=int(os.getenv("MINIAPM_PORT", "8000")),
        )
