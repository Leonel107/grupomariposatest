from pathlib import Path

from omegaconf import DictConfig, OmegaConf


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config(
    environment: str = "dev",
    tenant: str = "all",
) -> DictConfig:
    """
    Load hierarchical configuration.

    Configuration precedence:
        1. base.yaml
        2. env/<environment>.yaml
        3. tenants/<tenant>.yaml
    """

    base_path = (
        PROJECT_ROOT
        / "config"
        / "base.yaml"
    )

    env_path = (
        PROJECT_ROOT
        / "config"
        / "env"
        / f"{environment}.yaml"
    )

    if not base_path.exists():
        raise FileNotFoundError(
            f"Base configuration not found: {base_path}"
        )

    if not env_path.exists():
        raise FileNotFoundError(
            f"Environment configuration not found: {env_path}"
        )

    configs = [
        OmegaConf.load(base_path),
        OmegaConf.load(env_path),
    ]

    if tenant != "all":
        tenant_path = (
            PROJECT_ROOT
            / "config"
            / "tenants"
            / f"{tenant.lower()}.yaml"
        )

        if not tenant_path.exists():
            raise FileNotFoundError(
                f"Tenant configuration not found: "
                f"{tenant_path}"
            )

        configs.append(
            OmegaConf.load(tenant_path)
        )

    config = OmegaConf.merge(*configs)

    config.execution.tenant = tenant.lower()

    # Validate mandatory quality configuration.
    if not hasattr(config, "quality"):
        raise ValueError(
            "Missing 'quality' configuration."
        )

    if not hasattr(
        config.quality,
        "fail_on_critical",
    ):
        raise ValueError(
            "Missing 'quality.fail_on_critical'."
        )

    return config
