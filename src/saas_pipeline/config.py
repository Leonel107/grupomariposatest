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
        3. tenants/<tenant>.yaml when a specific tenant is requested

    When tenant == "all", all tenant configuration files are discovered
    from config/tenants/.
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

    tenants_path = (
        PROJECT_ROOT
        / "config"
        / "tenants"
    )

    if not base_path.exists():
        raise FileNotFoundError(
            f"Base configuration not found: {base_path}"
        )

    if not env_path.exists():
        raise FileNotFoundError(
            f"Environment configuration not found: {env_path}"
        )

    if not tenants_path.exists():
        raise FileNotFoundError(
            f"Tenant configuration directory not found: {tenants_path}"
        )

    configs = [
        OmegaConf.load(base_path),
        OmegaConf.load(env_path),
    ]

    tenant = tenant.lower()

    # ---------------------------------------------------------
    # Tenant configuration
    # ---------------------------------------------------------

    if tenant == "all":
        tenant_files = sorted(
            tenants_path.glob("*.yaml")
        )

        if not tenant_files:
            raise FileNotFoundError(
                f"No tenant configuration files found in: "
                f"{tenants_path}"
            )

        tenant_ids = []

        for tenant_file in tenant_files:
            tenant_config = OmegaConf.load(
                tenant_file
            )

            if not hasattr(
                tenant_config,
                "tenant",
            ):
                raise ValueError(
                    f"Missing 'tenant' section in: "
                    f"{tenant_file}"
                )

            if not hasattr(
                tenant_config.tenant,
                "id",
            ):
                raise ValueError(
                    f"Missing 'tenant.id' in: "
                    f"{tenant_file}"
                )

            tenant_id = str(
                tenant_config.tenant.id
            ).lower()

            tenant_ids.append(tenant_id)

        # Expose the available tenants to the rest
        # of the pipeline.
        config = OmegaConf.merge(*configs)

        config.tenants = tenant_ids

    else:
        tenant_path = (
            tenants_path
            / f"{tenant}.yaml"
        )

        if not tenant_path.exists():
            raise FileNotFoundError(
                f"Tenant configuration not found: "
                f"{tenant_path}"
            )

        tenant_config = OmegaConf.load(
            tenant_path
        )

        if not hasattr(
            tenant_config,
            "tenant",
        ):
            raise ValueError(
                f"Missing 'tenant' configuration "
                f"in: {tenant_path}"
            )

        if not hasattr(
            tenant_config.tenant,
            "id",
        ):
            raise ValueError(
                f"Missing 'tenant.id' in: "
                f"{tenant_path}"
            )

        configs.append(
            tenant_config
        )

        config = OmegaConf.merge(*configs)

        # Keep a uniform configuration contract.
        config.tenants = [tenant]

    # ---------------------------------------------------------
    # Execution configuration
    # ---------------------------------------------------------

    config.execution.tenant = tenant

    # ---------------------------------------------------------
    # Quality configuration validation
    # ---------------------------------------------------------

    if not hasattr(
        config,
        "quality",
    ):
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
