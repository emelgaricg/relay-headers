# relay-headers

Servicio didáctico para una clase de Kubernetes. Consume mensajes de una cola de RabbitMQ, imprime en el log **todo** lo que traen (propiedades, headers y contenido, en cualquier formato) y los publica idénticos en un exchange de tipo **headers**, agregando el header `estado = procesado`.

```
 RabbitMQ (vhost origen)                         RabbitMQ (vhost destino, mismo u otro servidor)
 ┌──────────────┐        ┌────────────────┐        ┌──────────────────────┐      ┌───────────┐
 │ cola origen  │──────► │ relay-headers  │──────► │ exchange tipo headers│ ───► │ cola(s)   │
 └──────────────┘consume └───────┬────────┘publica └──────────────────────┘ bind └───────────┘
                                 │                   mismo mensaje + header estado=procesado
                                 ▼
                          kubectl logs (contenido + headers + propiedades)
```

## Qué hace, paso a paso

1. Lee la configuración de variables de entorno (ConfigMap + Secret).
2. Se conecta al RabbitMQ de origen (servidor + vhost + credenciales).
3. Verifica que la cola exista (declaración pasiva: **nunca crea nada**).
4. Se conecta al RabbitMQ de destino, que puede ser otro vhost, otro servidor y otras credenciales.
5. Verifica que el exchange exista.
6. Consume con *ack* manual. Por cada mensaje:
   - imprime delivery info, propiedades, headers (con su tipo) y el contenido tal cual; si es binario, lo muestra en hexadecimal;
   - publica el mismo cuerpo y las mismas propiedades, agregando o sobrescribiendo el header `estado = procesado`;
   - espera la **confirmación** del broker destino (*publisher confirms*) y solo entonces confirma (*ack*) el mensaje en el origen. Si algo falla, el mensaje vuelve a la cola: **no se pierden mensajes**.

## Comportamiento ante errores (didáctico)

El servicio **nunca se cae** por un error de configuración. Explica en el log qué falta y cómo corregirlo, deja el pod en `0/1 READY` y pide reiniciarlo cuando esté corregido.

| Situación | Tipo | Qué hace |
|---|---|---|
| Falta una variable o tiene un valor inválido | Configuración | Lista todas las que faltan y espera |
| Usuario o contraseña incorrectos | Configuración | Explica y espera reinicio |
| El vhost no existe o el usuario no tiene permisos en él | Configuración | Explica y espera reinicio |
| La cola no existe | Configuración | Explica cómo crearla y espera |
| El exchange no existe | Configuración | Explica cómo crearlo y espera |
| Sin permiso *read* sobre la cola | Configuración | Explica qué permiso falta |
| Sin permiso *write* sobre el exchange | Configuración | Se detecta al publicar el primer mensaje, que vuelve a la cola |
| El exchange no tiene un binding que coincida | Configuración | El mensaje vuelve a la cola y explica cómo crear el binding (`SIN_RUTA=esperar`) |
| La cola o el exchange se eliminan en caliente | Configuración | Lo detecta y explica |
| RabbitMQ caído, reiniciado o problema de red | Transitorio | **Reintenta solo** cada `REINTENTO_SEGUNDOS` |

Mientras espera una corrección, repite el aviso cada `RECORDATORIO_SEGUNDOS` e imprime el comando exacto para reiniciar el pod, con el namespace y el nombre del pod reales (obtenidos con la *Downward API*).

Ejemplos reales de cada caso: [docs/ejemplos-de-logs.md](docs/ejemplos-de-logs.md).

### Probes

| Endpoint | Probe | Responde |
|---|---|---|
| `/healthz` | liveness | 200 mientras el proceso esté vivo, **aunque falte configuración**, para que Kubernetes no lo reinicie en bucle (CrashLoopBackOff). |
| `/ready` | readiness | 200 solo mientras procesa mensajes; 503 con el motivo en cualquier otro caso. |
| `/` | — | Estado en JSON: fase, motivo, acción, mensajes procesados y reintentos. |

```bash
kubectl -n clase-k8s port-forward deploy/relay-headers 8080:8080
curl localhost:8080/
```

## Variables de entorno

| Variable | Obligatoria | Por defecto | Descripción |
|---|---|---|---|
| `ORIGEN_HOST` | sí | | Host de RabbitMQ de origen |
| `ORIGEN_PUERTO` | no | `5672` | Puerto AMQP (5671 con TLS) |
| `ORIGEN_VHOST` | sí | | Vhost de la cola (por ejemplo `/ventas`) |
| `ORIGEN_USUARIO` | sí | | Usuario (Secret) |
| `ORIGEN_PASSWORD` | sí | | Contraseña (Secret) |
| `ORIGEN_COLA` | sí | | Cola de la que se consume |
| `ORIGEN_TLS` | no | `false` | Usar `amqps` |
| `DESTINO_HOST` | no | = origen | Host del RabbitMQ de destino |
| `DESTINO_PUERTO` | no | = origen | |
| `DESTINO_VHOST` | no | = origen | Vhost del exchange |
| `DESTINO_USUARIO` | no | = origen | Usuario de destino (Secret) |
| `DESTINO_PASSWORD` | no | = origen | Contraseña de destino (Secret) |
| `DESTINO_TLS` | no | = origen | |
| `DESTINO_EXCHANGE` | sí | | Exchange tipo headers |
| `DESTINO_ROUTING_KEY` | no | vacío | Un exchange headers la ignora |
| `PREFETCH` | no | `10` | Mensajes entregados por adelantado |
| `REINTENTO_SEGUNDOS` | no | `5` | Espera entre reintentos de red |
| `RECORDATORIO_SEGUNDOS` | no | `60` | Cada cuánto se repite el aviso de acción requerida |
| `SIN_RUTA` | no | `esperar` | `esperar`: si ningún binding coincide, el mensaje vuelve a la cola y se pide corregir. `descartar`: se confirma y se pierde, con una advertencia. |
| `HTTP_PUERTO` | no | `8080` | Puerto de las probes |
| `TZ` | no | UTC | Zona horaria de los logs (por ejemplo `America/Guatemala`) |

El log informa qué valores de destino se heredaron del origen y cuál es el escenario: mismo vhost, vhosts distintos o servidores distintos.

## 1. Publicar la imagen (sin instalar nada en su máquina)

La imagen se construye en GitHub Actions con un *multi-stage build*: el compilador de Go vive dentro de la primera etapa y la imagen final es `FROM scratch` (unos 7 MB, sin shell y sin usuario root).

1. Cree un repositorio en GitHub y suba este proyecto.
2. Cree un tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. El workflow `.github/workflows/publicar-imagen.yml` construye la imagen para `linux/amd64` y `linux/arm64` y la publica en:
   - `ghcr.io/<usuario>/relay-headers:1.0.0` (y `:1.0`), **sin configurar ningún secret**;
   - opcionalmente en Docker Hub, si define la variable `DOCKERHUB_USUARIO` y el secret `DOCKERHUB_TOKEN` en *Settings > Secrets and variables > Actions*.
4. **Solo la primera vez:** en GitHub, en *su perfil > Packages > relay-headers > Package settings > Change visibility*, márquela como **Public**. Desde entonces cualquiera la descarga sin credenciales y el Deployment no necesita `imagePullSecrets`.

Para construir localmente con Docker:

```bash
cd app
docker build -t relay-headers:dev --build-arg VERSION=dev .
```

## 2. Preparar RabbitMQ (se hace a mano en la clase)

Ejemplo con dos vhosts en el mismo servidor:

1. Crear los vhosts `/ventas` y `/procesados` (*Admin > Virtual Hosts*).
2. Crear el usuario `demo` y darle permisos en ambos vhosts (*Admin > Users*).
3. En `/ventas`, crear la cola `entrada`.
4. En `/procesados`:
   - crear el exchange `salida` de tipo **headers**;
   - crear la cola `resultado`;
   - crear el binding `salida` → `resultado` con los argumentos `x-match = all` y `estado = procesado`.

Si no tiene un RabbitMQ disponible, use `k8s/opcional/rabbitmq-demo.yaml`.

## 3. Desplegar

1. Edite `k8s/01-configmap.yaml` y `k8s/02-secret.yaml`.
2. En `k8s/03-deployment.yaml`, cambie `USUARIO` por su usuario de GitHub.
3. Aplique los manifiestos y revise el resultado:

```bash
kubectl apply -f k8s/
kubectl -n clase-k8s get pods -w
kubectl -n clase-k8s logs -f deploy/relay-headers
```

Para enviar un mensaje de prueba, abra la consola de RabbitMQ y vaya a *Queues > entrada > Publish message*. Agregue headers y propiedades y escriba cualquier contenido: JSON, XML o texto. Luego revise los logs y la cola `resultado`.

## Ejercicios sugeridos para la clase

1. **Readiness y liveness.** Despliegue antes de crear la cola. Observe `0/1 READY`, lea el log, cree la cola y ejecute `rollout restart`.
2. **ConfigMap y Secret.** Cambie el vhost de destino o el usuario. Note que las variables de entorno solo se leen al arrancar.
3. **Permisos.** Quite el permiso *write* al usuario de destino y publique un mensaje. Verifique que no se pierde.
4. **Bindings de un exchange headers.** Borre el binding. Luego pruebe `x-match=any` y otros headers.
5. **Resiliencia.** Reinicie el pod de RabbitMQ y observe la reconexión automática.
6. **Escalado.** Use `kubectl scale --replicas=3` y observe cómo RabbitMQ reparte los mensajes entre consumidores (*competing consumers*).
7. **Apagado limpio.** Borre el pod mientras procesa: el SIGTERM devuelve a la cola los mensajes no confirmados.
8. **Downward API.** Use `POD_NAMESPACE` en el Deployment.
9. **Seguridad.** Revise `runAsNonRoot`, `readOnlyRootFilesystem` y `capabilities: drop ALL`, y por qué una imagen `scratch` no tiene shell (`kubectl exec` falla).

## Estructura

```
app/                código Go, go.mod, Dockerfile
  main.go           arranque, ciclo de reintentos y espera de corrección
  config.go         lectura y validación de variables
  rabbit.go         conexión, verificaciones pasivas y traducción de errores
  relay.go          consumo, impresión del mensaje, publicación con confirmación
  estado.go         probes HTTP y estado
k8s/                namespace, configmap, secret, deployment (+ rabbitmq opcional)
.github/workflows/  pipeline que construye y publica la imagen
docs/               ejemplos reales de logs
```
