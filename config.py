"""共享配置 — pydantic-settings 管理所有参数，支持 .env 文件和环境变量"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # RabbitMQ
    amqp_url: str = "amqp://guest:guest@localhost/"

    # API
    api_port: int = 8000

    # Exchange 名称
    order_fulfillment_exchange: str = "order.fulfillment"
    order_compliance_exchange: str = "order.compliance"

    # Queue 名称
    queue_inventory: str = "inventory_queue"
    queue_customs: str = "customs_queue"
    queue_nlp: str = "nlp_queue"
    queue_cv: str = "cv_queue"

    # Routing / Binding Key
    routing_text_prefix: str = "compliance.text"
    routing_image_prefix: str = "compliance.image"
    binding_text: str = "compliance.text.*"
    binding_image: str = "compliance.image.*"

    # Dead Letter
    dlx_name: str = "dead_letter_exchange"
    dlx_queue: str = "dead_letter_queue"

    # Retry
    retry_exchange: str = "retry.exchange"
    retry_queue: str = "retry_queue"
    retry_ttl_ms: int = 5000

    # Priority
    max_priority: int = 10

    # Simulation (seconds)
    simulate_inventory: float = 0.5
    simulate_customs: float = 3.0
    simulate_nlp: float = 2.0
    simulate_cv: float = 4.0

    # Retry config
    max_retries: int = 3

    # Queue TTL (ms)
    customs_ttl_ms: int = 8000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def queue_args_inventory(self) -> dict:
        return {
            "x-max-priority": self.max_priority,
            "x-dead-letter-exchange": self.dlx_name,
        }

    @property
    def queue_args_customs(self) -> dict:
        return {
            "x-message-ttl": self.customs_ttl_ms,
            "x-dead-letter-exchange": self.dlx_name,
        }

    @property
    def queue_args_nlp(self) -> dict:
        return {"x-dead-letter-exchange": self.dlx_name}

    @property
    def queue_args_cv(self) -> dict:
        return {"x-dead-letter-exchange": self.dlx_name}


settings = Settings()
