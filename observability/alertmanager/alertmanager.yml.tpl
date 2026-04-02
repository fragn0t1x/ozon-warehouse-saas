global:
  resolve_timeout: 5m

templates:
  - /etc/alertmanager/templates/*.tmpl

route:
  receiver: telegram-admin
  group_by: ["alertname", "service"]
  group_wait: 10s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - matchers:
        - alertname="OverdueSuppliesDetected"
      receiver: telegram-business
      group_by: ["alertname", "service", "store_id", "store_name"]
    - matchers:
        - service="business"
      receiver: telegram-business

receivers:
  - name: telegram-admin
    telegram_configs:
      - bot_token: "__TELEGRAM_TOKEN__"
        chat_id: __TELEGRAM_CHAT_ID__
        parse_mode: HTML
        send_resolved: true
        message: '{{ template "telegram.default.message" . }}'
  - name: telegram-business
    telegram_configs:
      - bot_token: "__TELEGRAM_TOKEN__"
        chat_id: __TELEGRAM_CHAT_ID__
        parse_mode: HTML
        send_resolved: false
        message: '{{ template "telegram.business.message" . }}'
