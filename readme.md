# README.md — Sistema de detección de fraude en claims (Kafka + Redis + PostgreSQL)

# 1. Descripción del caso de uso

Este proyecto implementa un sistema de procesamiento de eventos en tiempo casi real para la detección de fraude en siniestros (claims) de seguros.

El sistema simula un flujo continuo de claims de seguros, que son evaluados automáticamente para determinar si deben:

Ser aprobados
Ser bloqueados (fraude)
Ser persistidos para auditoría

El objetivo es demostrar una arquitectura event-driven desacoplada basada en Kafka.

# 2. Arquitectura del sistema

## 2.1. Producer (simulación de eventos)

Genera continuamente eventos de claims:

claimId
customerId
amount
contractDate
claimDate
timestamp

Publica en Kafka:

claims-reported

## 2.2. Blocker / Fraud Service (stream processing)

Este es el núcleo del sistema.

Consume eventos desde Kafka y:

valida datos
aplica idempotencia (Redis)
ejecuta reglas de fraude
toma decisión final:
Decisiones:
APPROVED → pago permitido
BLOCKED → fraude detectado

Publica en:

payment-decisions → decisión final
fraud-events → eventos sospechosos

## 2.3. Writing Service (persistencia)

Consume eventos desde Kafka y los almacena en PostgreSQL.

Responsabilidades:

almacenar todos los claims
permitir auditoría histórica
soporte para análisis posterior

# 2.4. Redis (estado en memoria)

Se utiliza para:

idempotencia
evitar procesamiento duplicado de eventos

## 2.5. Kafka (event bus)

Actúa como columna vertebral del sistema:

pub/sub desacoplado
streaming de eventos
tolerancia a fallos
replay de eventos

## 2.6. PostgreSQL (almacenamiento)

Se utiliza como:

base de datos de auditoría
almacenamiento estructurado de claims


# 3. Arquitectura general

                 ┌──────────────┐
                 │  Producer     │
                 │ (claims gen)  │
                 └──────┬───────┘
                        │
                        ▼
              ┌───────────────────┐
              │ Kafka topic       │
              │ claims-reported   │
              └─────────┬─────────┘
                        │
        ┌───────────────┴───────────────┐
        ▼                               ▼
┌─────────────────┐         ┌────────────────────┐
│ Fraud / Blocker │         │ Writing Service    │
│ (stream logic)  │         │ (PostgreSQL)       │
└──────┬──────────┘         └─────────┬──────────┘
       │                              │
       ▼                              ▼
Redis (idempotency)           PostgreSQL DB

       │
       ▼
Kafka outputs:
- payment-decisions
- fraud-events

# 4. Flujo de eventos

El producer genera claims continuamente
Kafka recibe eventos en claims-reported
El blocker service:
valida evento
elimina duplicados (Redis)
evalúa fraude
Resultado:
APPROVED → payment-decisions
BLOCKED → fraud-events
El writing service almacena todos los eventos en PostgreSQL

# 5. Lógica de fraude

Un claim se considera fraudulento si:

amount > 10.000
claim realizado menos de 7 días después del contrato

# 6. Procesamiento en streaming

El sistema demuestra:

consumo continuo de Kafka
procesamiento near real-time
idempotencia (Redis SET NX)
control de errores
DLQ opcional (eventos fallidos)

# 7. Persistencia

PostgreSQL

Se almacenan todos los claims:

claimId
customerId
amount
contractDate
claimDate
timestamp

Uso:

auditoría
trazabilidad
análisis offline
Redis

Uso:

evitar duplicados
garantizar idempotencia

# 8. Manejo de errores

El sistema contempla:

validación de eventos
retry controlado
commit manual en Kafka
aislamiento de errores por evento

# 9. Patrón arquitectónico

El sistema sigue un modelo:

Event-Driven Architecture (EDA)

Características:

desacoplamiento total
escalabilidad horizontal
comunicación asíncrona
tolerancia a fallos

# 10. Tecnologías utilizadas
Streaming
Kafka (pub/sub + streaming)
Procesamiento
Python consumers
Estado
Redis (idempotencia)
Persistencia
PostgreSQL

# 11. Generación de eventos

El producer simula:

claims aleatorios
diferentes clientes
importes variables
casos de fraude intencionales

# 12. Decisiones técnicas
Kafka
elegido por su capacidad de streaming y replay
Redis
estado rápido y ligero para idempotencia
PostgreSQL
almacenamiento estructurado y analítico

# 13. Conclusión

Este sistema demuestra:

arquitectura event-driven real
separación de responsabilidades
procesamiento en streaming
manejo de estado
persistencia híbrida
tolerancia a fallos