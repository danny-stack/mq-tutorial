"""应用配置 — pydantic-settings 管理，支持 .env 文件和环境变量

Exchange / Queue / Binding 的定义在 topology.py，不在本文件中。
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # RabbitMQ
    amqp_url: str = "amqp://guest:guest@localhost/"

    # API
    api_port: int = 8000

    # Routing Key 前缀（发布者用）
    routing_text_prefix: str = "compliance.text"
    routing_image_prefix: str = "compliance.image"

    # Simulation (seconds)
    simulate_inventory: float = 0.5
    simulate_customs: float = 3.0
    simulate_nlp: float = 2.0
    simulate_cv: float = 4.0

    # Retry config
    max_retries: int = 3
    max_priority: int = 10
    retry_ttl_ms: int = 5000  # retry_queue 消息 TTL（.env: RETRY_TTL_MS）
    customs_ttl_ms: int = 8000  # customs_queue 消息 TTL（.env: CUSTOMS_TTL_MS）

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
