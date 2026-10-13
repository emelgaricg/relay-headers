# Ejemplos de logs reales

Salidas reales del servicio contra RabbitMQ 3.12, capturadas durante las pruebas. Se pueden usar en las diapositivas.

## 1. Arranque correcto y procesamiento de un mensaje

```text
------------------------------------------------------------------
  relay-headers 1.0.0  (go1.24.7, linux/amd64)
  Cola RabbitMQ  ->  log  ->  exchange headers  (+ header estado=procesado)
  pod: relay-headers-7d9c5b6f4-x2k8p
------------------------------------------------------------------
[1/6] Leyendo configuración desde variables de entorno (ConfigMap + Secret)
      Probes HTTP en :8080  ->  /healthz (liveness)  /ready (readiness)  / (estado)
      [OK] Configuración completa:
        ORIGEN  amqp://demo:****@rabbitmq.clase-k8s.svc.cluster.local:5672  vhost='/ventas'
                cola='entrada'
        DESTINO amqp://demo:****@rabbitmq.clase-k8s.svc.cluster.local:5672  vhost='/procesados'
                exchange='salida' (tipo headers)  routing key=''
        prefetch=10  reintento=5s  sin_ruta=esperar
        Valores de destino heredados del origen (no se definieron):
          DESTINO_HOST <- ORIGEN_HOST
          DESTINO_PUERTO <- ORIGEN_PUERTO
          DESTINO_USUARIO <- ORIGEN_USUARIO
          DESTINO_PASSWORD <- ORIGEN_PASSWORD
          DESTINO_TLS <- ORIGEN_TLS
        Escenario: mismo servidor, vhosts distintos.
[2/6] Conectando al RabbitMQ de ORIGEN  amqp://demo:****@...:5672  vhost='/ventas'
      [OK] Sesión iniciada y vhost '/ventas' accesible.
[3/6] Verificando que exista la cola 'entrada' en el vhost '/ventas' (declaración pasiva, no se crea nada)
      [OK] La cola existe. Mensajes en espera: 1, consumidores conectados: 0.
[4/6] Conectando al RabbitMQ de DESTINO amqp://demo:****@...:5672  vhost='/procesados'
      [OK] Sesión iniciada y vhost '/procesados' accesible.
[5/6] Verificando que exista el exchange 'salida' en el vhost '/procesados' (declaración pasiva, no se crea nada)
      [OK] El exchange existe.
      Nota: el permiso de escritura (write) sobre el exchange solo se puede comprobar al publicar el primer mensaje.
[6/6] Iniciando consumo de 'entrada' (prefetch=10, ack manual)
      [OK] Consumidor 'relay-headers-7d9c5b6f4-x2k8p' registrado.
------------------------------------------------------------------
  SERVICIO LISTO: esperando mensajes
  cola origen    : entrada  (vhost /ventas)
  exchange destino: salida  (vhost /procesados)
  a cada mensaje se le agrega el header  estado = procesado
------------------------------------------------------------------
======================== MENSAJE RECIBIDO ========================
  delivery tag : 1   redelivered: false
  exchange     : ''   routing key: 'entrada'
  --- propiedades ---
  content_type    : application/json
  delivery_mode   : 2 (persistente)
  message_id      : pedido-0001
  --- headers (2) ---
  estado          : 'pendiente' (texto)
  origen          : 'caja-01' (texto)
  --- contenido (49 bytes) ---
{"cliente": 123, "monto": 45.50, "moneda": "GTQ"}
=================================================================
      Header 'estado' ya existía con valor 'pendiente' (texto); se SOBRESCRIBE con 'procesado'.
      Publicando en exchange 'salida' (vhost /procesados, routing key '')...
      [OK] Publicado y confirmado por RabbitMQ destino. Mensaje de origen confirmado (ack) y eliminado de 'entrada'.
```

## 2. Contenido binario (no es texto)

```text
  --- contenido (7 bytes) ---
  (el contenido NO es texto UTF-8 válido: probablemente es binario; se muestra en hexadecimal)
  00000000  00 01 02 ff fe 41 42                             |.....AB|
```

## 3. Falta el exchange (el pod queda 0/1 READY, no se cae)

```text
##################################################################
#  ACCIÓN REQUERIDA: el servicio NO puede procesar mensajes
##################################################################
  Paso fallido : verificación del exchange
  Problema     : El exchange 'pagos-procesados' NO EXISTE en el vhost '/procesados' (NOT_FOUND).
  RabbitMQ dijo: Exception (404) Reason: "NOT_FOUND - no exchange 'pagos-procesados' in vhost '/procesados'"
  Qué hacer    : Créelo en la consola de RabbitMQ, vhost '/procesados': Exchanges > Add a new exchange (Type: headers).
  Después      : reinicie el pod para que vuelva a verificar todo:
                 kubectl -n clase-k8s rollout restart deployment/relay-headers   (o: kubectl -n clase-k8s delete pod relay-headers-7d9c5b6f4-x2k8p)

  El servicio queda VIVO y en espera (no se cae). El pod se verá como 0/1 READY
  porque la readiness probe (/ready) responde 503. Ningún mensaje se pierde:
  siguen en la cola de origen.
##################################################################
[ESPERANDO] Hace 1m0s que falta corregir: El exchange 'pagos-procesados' NO EXISTE ...
```

## 4. Contraseña incorrecta

```text
  Paso fallido : conexión de origen
  Problema     : RabbitMQ rechazó el inicio de sesión del usuario 'demo' (ACCESS_REFUSED). El usuario no existe o la contraseña es incorrecta.
  RabbitMQ dijo: Exception (403) Reason: "username or password not allowed"
  Qué hacer    : Revise ORIGEN_USUARIO / ORIGEN_PASSWORD en el Secret, o cree el usuario en la consola de RabbitMQ (Admin > Users > Add a user).
```

## 5. El vhost no existe o el usuario no tiene permisos en él

```text
  Paso fallido : conexión de origen
  Problema     : No se pudo entrar al vhost '/noexiste' (acceso denegado). El vhost no existe o el usuario 'demo' no tiene permisos en él.
  RabbitMQ dijo: Exception (403) Reason: "no access to this vhost"
  Qué hacer    : En la consola de RabbitMQ: cree el vhost (Admin > Virtual Hosts) y asigne permisos al usuario 'demo' sobre '/noexiste' (Admin > Users > demo > Set permission). Revise también ORIGEN_VHOST.
```

## 6. Sin permiso de lectura sobre la cola

```text
  Paso fallido : inicio del consumo
  Problema     : El usuario 'sinLectura' no tiene permisos suficientes sobre la cola 'entrada' en el vhost '/ventas' (ACCESS_REFUSED).
  RabbitMQ dijo: Exception (403) Reason: "ACCESS_REFUSED - read access to queue 'entrada' in vhost '/ventas' refused for user 'sinLectura'"
  Qué hacer    : En Admin > Users > sinLectura > Permissions, el permiso 'read' del vhost '/ventas' debe incluir 'entrada' (por ejemplo '.*').
```

## 7. Sin permiso de escritura sobre el exchange (se detecta al publicar el primer mensaje)

```text
      [ERROR] El canal de destino se cerró al publicar. El mensaje de origen vuelve a la cola (nack + requeue).
  Paso fallido : publicación en destino
  Problema     : El usuario 'soloLectura' NO tiene permiso de escritura (write) sobre el exchange 'salida' en el vhost '/procesados' (ACCESS_REFUSED). El mensaje de origen se devolvió a la cola, no se perdió.
  RabbitMQ dijo: Exception (403) Reason: "ACCESS_REFUSED - write access to exchange 'salida' in vhost '/procesados' refused for user 'soloLectura'"
  Qué hacer    : En Admin > Users > soloLectura > Permissions (vhost '/procesados'), el permiso 'write' debe incluir 'salida' (por ejemplo '.*').
```

## 8. El exchange no tiene un binding que coincida

```text
  Paso fallido : enrutamiento en el exchange
  Problema     : El exchange 'sinbind' recibió el mensaje pero NO tiene ningún binding cuyos headers coincidan (312 NO_ROUTE), así que RabbitMQ no lo entregó a ninguna cola. El mensaje de origen se devolvió a la cola 'entrada', no se perdió.
  Qué hacer    : Cree un binding en el exchange 'sinbind' (vhost /procesados) hacia una cola, por ejemplo con x-match=all y estado=procesado. (Si prefiere descartar los mensajes sin ruta, use SIN_RUTA=descartar.)
```

## 9. Faltan variables de configuración

```text
  Paso fallido : lectura de configuración
  Problema     : La configuración está incompleta o tiene valores inválidos:
        - falta la variable ORIGEN_COLA
        - falta la variable DESTINO_EXCHANGE
        - valor inválido: ORIGEN_PUERTO='abc' (debe ser un número entero >= 1)
  Qué hacer    : Agregue/corrija las variables en el ConfigMap (datos) o en el Secret (credenciales). Las variables de entorno solo se leen al arrancar: después de editarlas hay que reiniciar el pod.
```

## 10. RabbitMQ se reinicia (se recupera solo, sin intervención)

```text
[REINTENTO] Problema transitorio en conexión de origen: Exception (320) Reason: "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'"
            No es un error de configuración, así que se reintenta automáticamente en 5s (intento 1).
[2/6] Conectando al RabbitMQ de ORIGEN  amqp://demo:****@...:5672  vhost='/ventas'
[REINTENTO] Problema transitorio en conexión de origen: no se pudo contactar a RabbitMQ en ...:5672: connect: connection refused (si persiste, revise ORIGEN_HOST y ORIGEN_PUERTO)
            No es un error de configuración, así que se reintenta automáticamente en 5s (intento 2).
[2/6] Conectando al RabbitMQ de ORIGEN  amqp://demo:****@...:5672  vhost='/ventas'
  ...
  SERVICIO LISTO: esperando mensajes
```

## 11. La cola se elimina mientras el servicio corre

```text
  Paso fallido : consumo
  Problema     : RabbitMQ canceló el consumidor 'relay-headers-7d9c5b6f4-x2k8p'. Lo más probable es que la cola 'entrada' haya sido ELIMINADA mientras el servicio corría.
  Qué hacer    : Vuelva a crear la cola 'entrada' en el vhost '/ventas'.
```
