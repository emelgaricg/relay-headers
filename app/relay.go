package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"
)

const pasosTotales = 6

// Ejecutar hace todas las verificaciones y luego procesa mensajes hasta que:
//   - el contexto se cancela (SIGTERM de Kubernetes)  -> devuelve nil
//   - hay un error de configuración                   -> devuelve *FalloConfiguracion
//   - hay un error de red / broker                    -> devuelve *FalloTransitorio
func Ejecutar(ctx context.Context, cfg *Config, est *Estado) error {
	est.Cambiar(FaseConectando, "verificando conexiones y recursos en RabbitMQ", "")

	// ---------- Paso 2: conexión al origen ----------
	logPaso(2, pasosTotales, "Conectando al RabbitMQ de ORIGEN  "+cfg.Origen.Describir())
	connOrigen, err := Conectar(cfg.Origen, "conexión de origen")
	if err != nil {
		return err
	}
	defer connOrigen.Close()
	logOK("Sesión iniciada y vhost '%s' accesible.", cfg.Origen.Vhost)

	// ---------- Paso 3: la cola existe? ----------
	logPaso(3, pasosTotales, fmt.Sprintf("Verificando que exista la cola '%s' en el vhost '%s' (declaración pasiva, no se crea nada)",
		cfg.Cola, cfg.Origen.Vhost))
	q, err := VerificarCola(connOrigen, cfg.Cola, cfg.Origen.Vhost, cfg.Origen.Usuario, "verificación de la cola")
	if err != nil {
		return err
	}
	logOK("La cola existe. Mensajes en espera: %d, consumidores conectados: %d.", q.Messages, q.Consumers)

	// ---------- Paso 4: conexión al destino ----------
	// Siempre es una conexión aparte (buena práctica: no publicar y consumir por la
	// misma conexión), aunque sea el mismo servidor y el mismo vhost.
	logPaso(4, pasosTotales, "Conectando al RabbitMQ de DESTINO "+cfg.Destino.Describir())
	connDestino, err := Conectar(cfg.Destino, "conexión de destino")
	if err != nil {
		return err
	}
	defer connDestino.Close()
	logOK("Sesión iniciada y vhost '%s' accesible.", cfg.Destino.Vhost)

	// ---------- Paso 5: el exchange existe? ----------
	logPaso(5, pasosTotales, fmt.Sprintf("Verificando que exista el exchange '%s' en el vhost '%s' (declaración pasiva, no se crea nada)",
		cfg.Exchange, cfg.Destino.Vhost))
	if err := VerificarExchange(connDestino, cfg.Exchange, cfg.Destino.Vhost, cfg.Destino.Usuario, "verificación del exchange"); err != nil {
		return err
	}
	logOK("El exchange existe.")
	log.Printf("      Nota: el permiso de escritura (write) sobre el exchange solo se puede comprobar al publicar el primer mensaje.")

	// ---------- Paso 6: preparar canales y empezar a consumir ----------
	logPaso(6, pasosTotales, fmt.Sprintf("Iniciando consumo de '%s' (prefetch=%d, ack manual)", cfg.Cola, cfg.Prefetch))

	chDestino, err := connDestino.Channel()
	if err != nil {
		return &FalloTransitorio{Paso: "abrir canal de destino", Err: err}
	}
	// Publisher confirms: RabbitMQ nos avisa cuando realmente guardó el mensaje.
	if err := chDestino.Confirm(false); err != nil {
		return &FalloTransitorio{Paso: "activar publisher confirms", Err: err}
	}
	devueltos := chDestino.NotifyReturn(make(chan amqp.Return, 1))
	cierreChDestino := chDestino.NotifyClose(make(chan *amqp.Error, 1))
	cierreConnDestino := connDestino.NotifyClose(make(chan *amqp.Error, 1))

	chOrigen, err := connOrigen.Channel()
	if err != nil {
		return &FalloTransitorio{Paso: "abrir canal de origen", Err: err}
	}
	if err := chOrigen.Qos(cfg.Prefetch, 0, false); err != nil {
		return &FalloTransitorio{Paso: "configurar prefetch", Err: err}
	}
	cierreChOrigen := chOrigen.NotifyClose(make(chan *amqp.Error, 1))
	cierreConnOrigen := connOrigen.NotifyClose(make(chan *amqp.Error, 1))
	cancelado := chOrigen.NotifyCancel(make(chan string, 1))

	consumerTag := hostname() // en Kubernetes es el nombre del pod
	entregas, err := chOrigen.Consume(cfg.Cola, consumerTag, false, false, false, false, nil)
	if err != nil {
		return traducirErrorRecurso(err, "inicio del consumo", "cola", cfg.Cola, cfg.Origen.Vhost, cfg.Origen.Usuario,
			"Queues and Streams (o Queues) > Add a new queue", "read")
	}
	logOK("Consumidor '%s' registrado.", consumerTag)

	est.Cambiar(FaseProcesando, "", "")
	logBloque(linea,
		"  SERVICIO LISTO: esperando mensajes",
		fmt.Sprintf("  cola origen    : %s  (vhost %s)", cfg.Cola, cfg.Origen.Vhost),
		fmt.Sprintf("  exchange destino: %s  (vhost %s)", cfg.Exchange, cfg.Destino.Vhost),
		"  a cada mensaje se le agrega el header  estado = procesado",
		linea)

	p := &procesador{cfg: cfg, est: est, ch: chDestino, devueltos: devueltos, cierreCh: cierreChDestino}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[APAGADO] Kubernetes pidió detener el pod (SIGTERM). Cancelando el consumidor; los mensajes no confirmados vuelven a la cola.")
			_ = chOrigen.Cancel(consumerTag, false)
			return nil

		case d, ok := <-entregas:
			if !ok {
				// El canal de entregas se cerró: averiguamos por qué.
				return motivoDeCierre(cierreChOrigen, cierreConnOrigen, "origen", cfg)
			}
			if err := p.procesar(ctx, d); err != nil {
				return err
			}

		case tag := <-cancelado:
			return &FalloConfiguracion{Paso: "consumo",
				Problema: fmt.Sprintf("RabbitMQ canceló el consumidor '%s'. Lo más probable es que la cola '%s' haya sido ELIMINADA mientras el servicio corría.", tag, cfg.Cola),
				Accion:   fmt.Sprintf("Vuelva a crear la cola '%s' en el vhost '%s'.", cfg.Cola, cfg.Origen.Vhost)}

		case e := <-cierreChDestino:
			return clasificarCierre(e, "destino", cfg)
		case e := <-cierreConnDestino:
			return clasificarCierre(e, "destino", cfg)
		case e := <-cierreChOrigen:
			return clasificarCierre(e, "origen", cfg)
		case e := <-cierreConnOrigen:
			return clasificarCierre(e, "origen", cfg)
		}
	}
}

type procesador struct {
	cfg       *Config
	est       *Estado
	ch        *amqp.Channel
	devueltos chan amqp.Return
	cierreCh  chan *amqp.Error
}

func (p *procesador) procesar(ctx context.Context, d amqp.Delivery) error {
	// 1) Imprimir TODO lo que trae el mensaje, tal cual.
	imprimirMensaje(d)

	// 2) Armar el mismo mensaje, con las mismas propiedades, y el header estado=procesado.
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	anterior, existía := headers["estado"]
	headers["estado"] = "procesado"

	salida := amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body, // el contenido NO se toca
	}
	// RabbitMQ valida que user_id coincida con el usuario que publica.
	// Si el destino usa otro usuario, copiar ese campo haría fallar la publicación.
	if d.UserId != "" {
		if d.UserId == p.cfg.Destino.Usuario {
			salida.UserId = d.UserId
		} else {
			log.Printf("      Nota: se omite la propiedad user_id='%s' porque el destino publica con el usuario '%s' y RabbitMQ la rechazaría.",
				d.UserId, p.cfg.Destino.Usuario)
		}
	}

	if existía {
		log.Printf("      Header 'estado' ya existía con valor %v; se SOBRESCRIBE con 'procesado'.", formatearValor(anterior))
	} else {
		log.Printf("      Se agrega el header 'estado' = 'procesado'.")
	}

	// 3) Publicar con mandatory=true: si ningún binding coincide, RabbitMQ nos lo devuelve.
	log.Printf("      Publicando en exchange '%s' (vhost %s, routing key '%s')...", p.cfg.Exchange, p.cfg.Destino.Vhost, p.cfg.RoutingKey)
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.cfg.Exchange, p.cfg.RoutingKey, true, false, salida)
	if err != nil {
		_ = d.Nack(false, true)
		log.Printf("      [ERROR] No se pudo publicar: %v. El mensaje de origen se devuelve a la cola (nack + requeue).", err)
		return esperarCierre(p.cierreCh, "destino", p.cfg, err)
	}

	// 4) Esperar la confirmación del broker destino.
	espera, cancelar := context.WithTimeout(ctx, 30*time.Second)
	defer cancelar()
	select {
	case <-conf.Done():
	case e := <-p.cierreCh:
		_ = d.Nack(false, true)
		log.Printf("      [ERROR] El canal de destino se cerró al publicar. El mensaje de origen vuelve a la cola (nack + requeue).")
		return clasificarCierre(e, "destino", p.cfg)
	case <-espera.Done():
		_ = d.Nack(false, true)
		return &FalloTransitorio{Paso: "confirmación de publicación", Err: errors.New("RabbitMQ destino no confirmó en 30 s; el mensaje vuelve a la cola")}
	}

	if !conf.Acked() {
		_ = d.Nack(false, true)
		// Puede ser un nack del broker o que el canal se cerró (por ejemplo, sin permiso write).
		return esperarCierre(p.cierreCh, "destino", p.cfg,
			errors.New("RabbitMQ destino respondió NACK (no pudo guardar el mensaje); el mensaje de origen vuelve a la cola"))
	}

	// 5) Fue devuelto por no tener ruta? (llega antes que la confirmación)
	select {
	case r := <-p.devueltos:
		if p.cfg.SinRuta == "descartar" {
			_ = d.Ack(false)
			log.Printf("      [ADVERTENCIA] RabbitMQ devolvió el mensaje (%d %s): ningún binding del exchange '%s' coincide con sus headers. "+
				"SIN_RUTA=descartar: el mensaje se confirma en origen y se PIERDE.", r.ReplyCode, r.ReplyText, p.cfg.Exchange)
			p.est.SumarProcesado()
			return nil
		}
		_ = d.Nack(false, true)
		return &FalloConfiguracion{Paso: "enrutamiento en el exchange",
			Problema: fmt.Sprintf("El exchange '%s' recibió el mensaje pero NO tiene ningún binding cuyos headers coincidan (%d %s), así que RabbitMQ no lo entregó a ninguna cola. "+
				"El mensaje de origen se devolvió a la cola '%s', no se perdió.", p.cfg.Exchange, r.ReplyCode, r.ReplyText, p.cfg.Cola),
			Accion: fmt.Sprintf("Cree un binding en el exchange '%s' (vhost %s) hacia una cola, por ejemplo con x-match=all y estado=procesado. "+
				"(Si prefiere descartar los mensajes sin ruta, use SIN_RUTA=descartar.)", p.cfg.Exchange, p.cfg.Destino.Vhost)}
	default:
	}

	// 6) Todo bien: recién ahora se confirma el mensaje en el origen.
	if err := d.Ack(false); err != nil {
		return &FalloTransitorio{Paso: "ack en origen", Err: err}
	}
	p.est.SumarProcesado()
	log.Printf("      [OK] Publicado y confirmado por RabbitMQ destino. Mensaje de origen confirmado (ack) y eliminado de '%s'.", p.cfg.Cola)
	return nil
}

// esperarCierre: tras un fallo al publicar, damos un instante para que llegue el
// motivo real del cierre del canal (por ejemplo ACCESS_REFUSED por falta de permiso).
func esperarCierre(cierre chan *amqp.Error, lado string, cfg *Config, porDefecto error) error {
	select {
	case e := <-cierre:
		return clasificarCierre(e, lado, cfg)
	case <-time.After(2 * time.Second):
		return &FalloTransitorio{Paso: "publicación en destino", Err: porDefecto}
	}
}

func motivoDeCierre(cierreCh, cierreConn chan *amqp.Error, lado string, cfg *Config) error {
	select {
	case e := <-cierreCh:
		return clasificarCierre(e, lado, cfg)
	case e := <-cierreConn:
		return clasificarCierre(e, lado, cfg)
	case <-time.After(2 * time.Second):
		return &FalloTransitorio{Paso: "consumo", Err: errors.New("el canal de entregas se cerró sin informar el motivo")}
	}
}

// clasificarCierre decide si un cierre de canal/conexión durante la operación es
// un problema de configuración (esperar correccion) o transitorio (reconectar).
func clasificarCierre(e *amqp.Error, lado string, cfg *Config) error {
	if e == nil {
		return &FalloTransitorio{Paso: "conexión de " + lado, Err: errors.New("la conexión se cerró")}
	}
	switch e.Code {
	case amqp.AccessRefused:
		if lado == "destino" {
			return &FalloConfiguracion{Paso: "publicación en destino",
				Problema: fmt.Sprintf("El usuario '%s' NO tiene permiso de escritura (write) sobre el exchange '%s' en el vhost '%s' (ACCESS_REFUSED). "+
					"El mensaje de origen se devolvió a la cola, no se perdió.", cfg.Destino.Usuario, cfg.Exchange, cfg.Destino.Vhost),
				Accion: fmt.Sprintf("En Admin > Users > %s > Permissions (vhost '%s'), el permiso 'write' debe incluir '%s' (por ejemplo '.*').",
					cfg.Destino.Usuario, cfg.Destino.Vhost, cfg.Exchange),
				Detalle: e.Error()}
		}
		return &FalloConfiguracion{Paso: "consumo en origen",
			Problema: fmt.Sprintf("El usuario '%s' perdió el acceso a la cola '%s' (ACCESS_REFUSED).", cfg.Origen.Usuario, cfg.Cola),
			Accion:   fmt.Sprintf("Revise los permisos de '%s' en el vhost '%s' (permiso 'read').", cfg.Origen.Usuario, cfg.Origen.Vhost),
			Detalle:  e.Error()}
	case amqp.NotFound:
		if lado == "destino" {
			return &FalloConfiguracion{Paso: "publicación en destino",
				Problema: fmt.Sprintf("El exchange '%s' ya no existe en el vhost '%s' (NOT_FOUND); probablemente fue eliminado.", cfg.Exchange, cfg.Destino.Vhost),
				Accion:   fmt.Sprintf("Vuelva a crear el exchange '%s' (tipo headers) en el vhost '%s'.", cfg.Exchange, cfg.Destino.Vhost),
				Detalle:  e.Error()}
		}
		return &FalloConfiguracion{Paso: "consumo en origen",
			Problema: fmt.Sprintf("La cola '%s' ya no existe en el vhost '%s' (NOT_FOUND).", cfg.Cola, cfg.Origen.Vhost),
			Accion:   fmt.Sprintf("Vuelva a crear la cola '%s'.", cfg.Cola),
			Detalle:  e.Error()}
	case amqp.NotAllowed, amqp.PreconditionFailed:
		return &FalloConfiguracion{Paso: "operación en " + lado,
			Problema: fmt.Sprintf("RabbitMQ cerró el canal de %s (código %d).", lado, e.Code),
			Accion:   "Lea el detalle de RabbitMQ y corrija la configuración.",
			Detalle:  e.Error()}
	}
	// 320 CONNECTION_FORCED (reinicio del broker, conexión cerrada desde la consola),
	// errores de red, heartbeats perdidos... todo eso se reintenta solo.
	return &FalloTransitorio{Paso: "conexión de " + lado, Err: e}
}

// ---------- Impresión del mensaje ----------

func imprimirMensaje(d amqp.Delivery) {
	l := []string{
		"======================== MENSAJE RECIBIDO ========================",
		fmt.Sprintf("  delivery tag : %d   redelivered: %v", d.DeliveryTag, d.Redelivered),
		fmt.Sprintf("  exchange     : '%s'   routing key: '%s'", d.Exchange, d.RoutingKey),
		"  --- propiedades ---",
	}
	prop := func(nombre, valor string) {
		if valor != "" {
			l = append(l, fmt.Sprintf("  %-16s: %s", nombre, valor))
		}
	}
	prop("content_type", d.ContentType)
	prop("content_encoding", d.ContentEncoding)
	if d.DeliveryMode != 0 {
		modo := "1 (no persistente)"
		if d.DeliveryMode == 2 {
			modo = "2 (persistente)"
		}
		prop("delivery_mode", modo)
	}
	if d.Priority != 0 {
		prop("priority", fmt.Sprint(d.Priority))
	}
	prop("message_id", d.MessageId)
	prop("correlation_id", d.CorrelationId)
	prop("reply_to", d.ReplyTo)
	prop("expiration", d.Expiration)
	if !d.Timestamp.IsZero() {
		prop("timestamp", d.Timestamp.Format(time.RFC3339))
	}
	prop("type", d.Type)
	prop("user_id", d.UserId)
	prop("app_id", d.AppId)

	if strings.HasSuffix(l[len(l)-1], "--- propiedades ---") {
		l = append(l, "  (sin propiedades)")
	}
	l = append(l, fmt.Sprintf("  --- headers (%d) ---", len(d.Headers)))
	if len(d.Headers) == 0 {
		l = append(l, "  (sin headers)")
	}
	claves := make([]string, 0, len(d.Headers))
	for k := range d.Headers {
		claves = append(claves, k)
	}
	sort.Strings(claves)
	for _, k := range claves {
		l = append(l, fmt.Sprintf("  %-16s: %s", k, formatearValor(d.Headers[k])))
	}

	l = append(l, fmt.Sprintf("  --- contenido (%d bytes) ---", len(d.Body)))
	if utf8.Valid(d.Body) {
		l = append(l, string(d.Body))
	} else {
		l = append(l, "  (el contenido NO es texto UTF-8 válido: probablemente es binario; se muestra en hexadecimal)")
		l = append(l, volcadoHex(d.Body, 512)...)
	}
	l = append(l, "=================================================================")
	logBloque(l...)
}

// formatearValor muestra el valor y su tipo AMQP, para que se vea exactamente que llegó.
func formatearValor(v any) string {
	switch x := v.(type) {
	case nil:
		return "(nulo)"
	case string:
		return fmt.Sprintf("'%s' (texto)", x)
	case []byte:
		return fmt.Sprintf("'%s' (bytes)", string(x))
	case bool:
		return fmt.Sprintf("%v (booleano)", x)
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v (entero %T)", x, x)
	case float32, float64:
		return fmt.Sprintf("%v (decimal %T)", x, x)
	case time.Time:
		return fmt.Sprintf("%s (fecha)", x.Format(time.RFC3339))
	case amqp.Table:
		return fmt.Sprintf("%v (tabla)", map[string]any(x))
	case []any:
		return fmt.Sprintf("%v (lista)", x)
	default:
		return fmt.Sprintf("%v (%T)", x, x)
	}
}

// volcadoHex muestra bytes binarios como "hexdump": 16 bytes por línea + texto imprimible.
func volcadoHex(b []byte, max int) []string {
	var out []string
	recortado := false
	if len(b) > max {
		b, recortado = b[:max], true
	}
	for i := 0; i < len(b); i += 16 {
		fin := i + 16
		if fin > len(b) {
			fin = len(b)
		}
		var hex, txt strings.Builder
		for j := i; j < i+16; j++ {
			if j < fin {
				fmt.Fprintf(&hex, "%02x ", b[j])
				if b[j] >= 32 && b[j] < 127 {
					txt.WriteByte(b[j])
				} else {
					txt.WriteByte('.')
				}
			} else {
				hex.WriteString("   ")
			}
		}
		out = append(out, fmt.Sprintf("  %08x  %s |%s|", i, hex.String(), txt.String()))
	}
	if recortado {
		out = append(out, fmt.Sprintf("  ... (se muestran solo los primeros %d bytes)", max))
	}
	return out
}
