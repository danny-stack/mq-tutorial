"""共享配置：RabbitMQ 连接参数、Exchange / Queue 名称常量"""

AMQP_URL = "amqp://guest:guest@localhost/"

# Exchange
ORDER_FULFILLMENT_EXCHANGE = "order.fulfillment"  # fanout
ORDER_COMPLIANCE_EXCHANGE = "order.compliance"  # topic

# Queue
QUEUE_INVENTORY = "inventory_queue"
QUEUE_CUSTOMS = "customs_queue"
QUEUE_NLP = "nlp_queue"
QUEUE_CV = "cv_queue"

# Routing Key 前缀
ROUTING_TEXT_PREFIX = "compliance.text"
ROUTING_IMAGE_PREFIX = "compliance.image"

# Binding Key（Topic 模式通配符）
BINDING_TEXT = "compliance.text.*"
BINDING_IMAGE = "compliance.image.*"

# 模拟处理耗时（秒）
SIMULATE_INVENTORY = 0.5
SIMULATE_CUSTOMS = 3.0
SIMULATE_NLP = 2.0
SIMULATE_CV = 4.0
