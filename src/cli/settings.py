from pydantic_settings import BaseSettings


class GatewaySettings(BaseSettings):
    GATEWAY_URL: str = "http://localhost:9080"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = GatewaySettings()
