package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// FalloConfiguracion: algo falta o esta mal configurado en RabbitMQ o en el
// ConfigMap/Secret. Reintentar no sirve; hay que corregir y reiniciar el pod.
type FalloConfiguracion struct {
	Paso     string
	Problema string
	Accion   string
	Detalle  string // texto original de RabbitMQ
}

func (f *FalloConfiguracion) Error() string { return f.Paso + ": " + f.Problema }

// FalloTransitorio: RabbitMQ no responde, se cayo la red, se reinicio el broker...
// Se reintenta solo, sin intervención.
type FalloTransitorio struct {
	Paso string
	Err  error
}

func (f *FalloTransitorio) Error() string { return f.Paso + ": " + f.Err.Error() }

// comandoReinicio arma el comando exacto usando el namespace y el nombre del pod,
// que Kubernetes inyecta con la Downward API (ver POD_NAMESPACE en el Deployment).
func comandoReinicio() string {
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		ns = "<namespace>"
	}
	return fmt.Sprintf("kubectl -n %s rollout restart deployment/relay-headers   (o: kubectl -n %s delete pod %s)", ns, ns, hostname())
}

// Conectar abre una conexión AMQP y traduce los errores tipicos a mensajes claros.
func Conectar(c Conexion, paso string) (*amqp.Connection, error) {
	cfg := amqp.Config{
		Vhost:     c.Vhost,
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Properties: amqp.Table{
			// Aparece en la consola de RabbitMQ (pestaña Connections).
			"connection_name": fmt.Sprintf("relay-headers-%s (%s)", c.Nombre, hostname()),
		},
	}
	if c.TLS {
		cfg.TLSClientConfig = &tls.Config{ServerName: c.Host}
	}
	conn, err := amqp.DialConfig(c.URL(), cfg)
	if err == nil {
		return conn, nil
	}

	prefijo := "ORIGEN"
	if c.Nombre == "destino" {
		prefijo = "DESTINO"
	}
	switch {
	case errors.Is(err, amqp.ErrCredentials):
		return nil, &FalloConfiguracion{
			Paso:     paso,
			Problema: fmt.Sprintf("RabbitMQ rechazó el inicio de sesión del usuario '%s' (ACCESS_REFUSED). El usuario no existe o la contraseña es incorrecta.", c.Usuario),
			Accion: fmt.Sprintf("Revise %s_USUARIO / %s_PASSWORD en el Secret, o cree el usuario en la consola de RabbitMQ (Admin > Users > Add a user).",
				prefijo, prefijo),
			Detalle: err.Error(),
		}
	case errors.Is(err, amqp.ErrVhost):
		return nil, &FalloConfiguracion{
			Paso:     paso,
			Problema: fmt.Sprintf("No se pudo entrar al vhost '%s' (acceso denegado). El vhost no existe o el usuario '%s' no tiene permisos en él.", c.Vhost, c.Usuario),
			Accion: fmt.Sprintf("En la consola de RabbitMQ: cree el vhost (Admin > Virtual Hosts) y asigne permisos al usuario '%s' sobre '%s' (Admin > Users > %s > Set permission). Revise también %s_VHOST.",
				c.Usuario, c.Vhost, c.Usuario, prefijo),
			Detalle: err.Error(),
		}
	case errors.Is(err, amqp.ErrSASL):
		return nil, &FalloConfiguracion{
			Paso:     paso,
			Problema: "El servidor no acepta el mecanismo de autenticación PLAIN.",
			Accion:   "Revise la configuración de autenticación del servidor RabbitMQ.",
			Detalle:  err.Error(),
		}
	}
	var ae *amqp.Error
	if errors.As(err, &ae) && !ae.Recover {
		return nil, &FalloConfiguracion{Paso: paso,
			Problema: fmt.Sprintf("RabbitMQ rechazó la conexión (código %d).", ae.Code),
			Accion:   "Lea el detalle de RabbitMQ y corrija la configuración.",
			Detalle:  ae.Error()}
	}
	// Todo lo demas (host no resuelve, puerto cerrado, timeout) se considera transitorio:
	// en Kubernetes es normal que RabbitMQ todavía no esté listo cuando arranca el pod.
	return nil, &FalloTransitorio{Paso: paso, Err: fmt.Errorf(
		"no se pudo contactar a RabbitMQ en %s:%d: %w (si persiste, revise %s_HOST y %s_PUERTO)",
		c.Host, c.Puerto, err, prefijo, prefijo)}
}

// VerificarCola usa una declaración PASIVA: solo pregunta si existe, nunca la crea.
func VerificarCola(conn *amqp.Connection, cola, vhost, usuario, paso string) (amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return amqp.Queue{}, &FalloTransitorio{Paso: paso, Err: err}
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(cola, false, false, false, false, nil)
	if err == nil {
		return q, nil
	}
	return q, traducirErrorRecurso(err, paso, "cola", cola, vhost, usuario,
		"Queues and Streams (o Queues) > Add a new queue", "read")
}

// VerificarExchange igual que la cola: declaración pasiva, nunca lo crea.
func VerificarExchange(conn *amqp.Connection, exchange, vhost, usuario, paso string) error {
	ch, err := conn.Channel()
	if err != nil {
		return &FalloTransitorio{Paso: paso, Err: err}
	}
	defer ch.Close()
	err = ch.ExchangeDeclarePassive(exchange, amqp.ExchangeHeaders, false, false, false, false, nil)
	if err == nil {
		return nil
	}
	return traducirErrorRecurso(err, paso, "exchange", exchange, vhost, usuario,
		"Exchanges > Add a new exchange (Type: headers)", "write")
}

// traducirErrorRecurso convierte los codigos AMQP en una explicacion y una acción.
func traducirErrorRecurso(err error, paso, tipo, nombre, vhost, usuario, dondeCrear, permiso string) error {
	var ae *amqp.Error
	if !errors.As(err, &ae) {
		return &FalloTransitorio{Paso: paso, Err: err}
	}
	articulo, verbo := "La", "Créela"
	if tipo == "exchange" {
		articulo, verbo = "El", "Créelo"
	}
	switch ae.Code {
	case amqp.NotFound: // 404
		return &FalloConfiguracion{Paso: paso,
			Problema: fmt.Sprintf("%s %s '%s' NO EXISTE en el vhost '%s' (NOT_FOUND).", articulo, tipo, nombre, vhost),
			Accion:   fmt.Sprintf("%s en la consola de RabbitMQ, vhost '%s': %s.", verbo, vhost, dondeCrear),
			Detalle:  ae.Error()}
	case amqp.AccessRefused: // 403
		return &FalloConfiguracion{Paso: paso,
			Problema: fmt.Sprintf("El usuario '%s' no tiene permisos suficientes sobre %s %s '%s' en el vhost '%s' (ACCESS_REFUSED).",
				usuario, articuloMin(tipo), tipo, nombre, vhost),
			Accion: fmt.Sprintf("En Admin > Users > %s > Permissions, el permiso '%s' del vhost '%s' debe incluir '%s' (por ejemplo '.*').",
				usuario, permiso, vhost, nombre),
			Detalle: ae.Error()}
	case amqp.ResourceLocked: // 405
		return &FalloConfiguracion{Paso: paso,
			Problema: fmt.Sprintf("%s %s '%s' es exclusiva de otra conexión (RESOURCE_LOCKED).", articulo, tipo, nombre),
			Accion:   "Use una cola no exclusiva o cierre la conexión que la tiene tomada.",
			Detalle:  ae.Error()}
	case amqp.PreconditionFailed: // 406
		return &FalloConfiguracion{Paso: paso,
			Problema: fmt.Sprintf("RabbitMQ rechazó la operación sobre %s %s '%s' (PRECONDITION_FAILED).", articuloMin(tipo), tipo, nombre),
			Accion:   "Lea el detalle de RabbitMQ y corrija la definición del recurso.",
			Detalle:  ae.Error()}
	}
	if ae.Recover {
		return &FalloConfiguracion{Paso: paso, Problema: "Error de RabbitMQ al verificar el recurso.",
			Accion: "Lea el detalle de RabbitMQ.", Detalle: ae.Error()}
	}
	return &FalloTransitorio{Paso: paso, Err: ae}
}

func articuloMin(tipo string) string {
	if tipo == "exchange" {
		return "el"
	}
	return "la"
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "desconocido"
	}
	return h
}
