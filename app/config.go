package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Conexión describe como llegar a un RabbitMQ (servidor + vhost + credenciales).
type Conexion struct {
	Nombre   string // "origen" o "destino", solo para los logs
	Host     string
	Puerto   int
	Vhost    string
	Usuario  string
	Password string
	TLS      bool
}

// URL arma la URL AMQP con la contraseña escapada. El vhost se envia aparte
// (amqp.Config.Vhost) para no tener que codificar el "/" del vhost raiz.
func (c Conexion) URL() string {
	esquema := "amqp"
	if c.TLS {
		esquema = "amqps"
	}
	u := url.URL{
		Scheme: esquema,
		User:   url.UserPassword(c.Usuario, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Puerto),
		Path:   "/",
	}
	return u.String()
}

// Describir devuelve la conexión sin la contraseña, apta para imprimir en logs.
func (c Conexion) Describir() string {
	esquema := "amqp"
	if c.TLS {
		esquema = "amqps"
	}
	return fmt.Sprintf("%s://%s:****@%s:%d  vhost='%s'", esquema, c.Usuario, c.Host, c.Puerto, c.Vhost)
}

// Config es toda la configuración del servicio. Se lee de variables de entorno,
// que en Kubernetes vienen de un ConfigMap (datos) y un Secret (credenciales).
type Config struct {
	Origen       Conexion
	Cola         string
	Destino      Conexion
	Exchange     string
	RoutingKey   string
	Prefetch     int
	PuertoHTTP   int
	Reintento    time.Duration
	Recordatorio time.Duration
	SinRuta      string // "esperar" o "descartar"
	Heredados    []string
}

// ErrorConfiguracion junta todos los problemas de variables para mostrarlos de una sola vez.
type ErrorConfiguracion struct {
	Faltantes []string
	Invalidas []string
}

func (e *ErrorConfiguracion) Error() string {
	return fmt.Sprintf("configuración incompleta: faltan %v, inválidas %v", e.Faltantes, e.Invalidas)
}

type lector struct {
	err       ErrorConfiguracion
	heredados []string
}

func (l *lector) requerida(nombre string) string {
	v := strings.TrimSpace(os.Getenv(nombre))
	if v == "" {
		l.err.Faltantes = append(l.err.Faltantes, nombre)
	}
	return v
}

func (l *lector) opcional(nombre, porDefecto string) string {
	if v, ok := os.LookupEnv(nombre); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return porDefecto
}

// heredada: si la variable de destino no existe, toma el valor del origen y lo anota.
func (l *lector) heredada(nombre, valorOrigen, nombreOrigen string) string {
	if v, ok := os.LookupEnv(nombre); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	l.heredados = append(l.heredados, fmt.Sprintf("%s <- %s", nombre, nombreOrigen))
	return valorOrigen
}

func (l *lector) entero(nombre, texto string, min int) int {
	n, err := strconv.Atoi(texto)
	if err != nil || n < min {
		l.err.Invalidas = append(l.err.Invalidas, fmt.Sprintf("%s='%s' (debe ser un número entero >= %d)", nombre, texto, min))
		return min
	}
	return n
}

func (l *lector) booleano(nombre, texto string) bool {
	b, err := strconv.ParseBool(texto)
	if err != nil {
		l.err.Invalidas = append(l.err.Invalidas, fmt.Sprintf("%s='%s' (use true o false)", nombre, texto))
	}
	return b
}

// CargarConfig lee y valida las variables de entorno.
func CargarConfig() (*Config, error) {
	l := &lector{}
	c := &Config{}

	// ---- Origen: de donde se consumen los mensajes ----
	c.Origen.Nombre = "origen"
	c.Origen.Host = l.requerida("ORIGEN_HOST")
	puertoOrigen := l.opcional("ORIGEN_PUERTO", "5672")
	c.Origen.Puerto = l.entero("ORIGEN_PUERTO", puertoOrigen, 1)
	c.Origen.Vhost = l.requerida("ORIGEN_VHOST")
	c.Origen.Usuario = l.requerida("ORIGEN_USUARIO")
	c.Origen.Password = l.requerida("ORIGEN_PASSWORD")
	tlsOrigen := l.opcional("ORIGEN_TLS", "false")
	c.Origen.TLS = l.booleano("ORIGEN_TLS", tlsOrigen)
	c.Cola = l.requerida("ORIGEN_COLA")

	// ---- Destino: exchange tipo headers. Puede estar en otro vhost, otro
	// servidor y usar otras credenciales. Lo que no se defina se hereda del origen.
	c.Destino.Nombre = "destino"
	c.Destino.Host = l.heredada("DESTINO_HOST", c.Origen.Host, "ORIGEN_HOST")
	c.Destino.Puerto = l.entero("DESTINO_PUERTO", l.heredada("DESTINO_PUERTO", puertoOrigen, "ORIGEN_PUERTO"), 1)
	c.Destino.Vhost = l.heredada("DESTINO_VHOST", c.Origen.Vhost, "ORIGEN_VHOST")
	c.Destino.Usuario = l.heredada("DESTINO_USUARIO", c.Origen.Usuario, "ORIGEN_USUARIO")
	c.Destino.Password = l.heredada("DESTINO_PASSWORD", c.Origen.Password, "ORIGEN_PASSWORD")
	c.Destino.TLS = l.booleano("DESTINO_TLS", l.heredada("DESTINO_TLS", tlsOrigen, "ORIGEN_TLS"))
	c.Exchange = l.requerida("DESTINO_EXCHANGE")
	c.RoutingKey = l.opcional("DESTINO_ROUTING_KEY", "")

	// ---- Comportamiento ----
	c.Prefetch = l.entero("PREFETCH", l.opcional("PREFETCH", "10"), 1)
	c.PuertoHTTP = l.entero("HTTP_PUERTO", l.opcional("HTTP_PUERTO", "8080"), 1)
	c.Reintento = time.Duration(l.entero("REINTENTO_SEGUNDOS", l.opcional("REINTENTO_SEGUNDOS", "5"), 1)) * time.Second
	c.Recordatorio = time.Duration(l.entero("RECORDATORIO_SEGUNDOS", l.opcional("RECORDATORIO_SEGUNDOS", "60"), 5)) * time.Second
	c.SinRuta = strings.ToLower(l.opcional("SIN_RUTA", "esperar"))
	if c.SinRuta != "esperar" && c.SinRuta != "descartar" {
		l.err.Invalidas = append(l.err.Invalidas, fmt.Sprintf("SIN_RUTA='%s' (use esperar o descartar)", c.SinRuta))
	}
	c.Heredados = l.heredados

	if len(l.err.Faltantes) > 0 || len(l.err.Invalidas) > 0 {
		return c, &l.err
	}
	return c, nil
}
