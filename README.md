# Estoque

Essa API desenvolvida com Java 17 + Spring Boot, serve como lado de Consumer (Consumidor) de mensagens para o Broker Kafka  
configurado localmente. A funcionalidade base é que o componente de KafkaListener escute as mensagens enviadas para o tópico _pedidos-criados_,  
para que o respectivo produto seja descontado do estque (Banco de dados H2 em memória), contornando um envio direto de requisição entre APIs