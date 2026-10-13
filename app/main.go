// relay-headers: servicio didáctico para una clase de Kubernetes.
//
// Consume mensajes de una cola de RabbitMQ, imprime en el log todo lo que traen
// (propiedades, headers y contenido, sin validar el formato) y los publica
// idénticos en un exchange de tipo headers agregando el header estado=procesado.
//
// Nunca se cae por un error de configuración: explica en el log que falta,
// marca el pod como "no listo" y pide reiniciarlo cuando se haya corregido.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // zona horaria embebida: la imagen scratch no trae /usr/share/zoneinfo (use TZ=America/Guatemala)
)

// Version se inyecta al compilar: go build -ldflags "-X main.Version=v1.0.0"
var Version = "dev"

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	log.SetOutput(os.Stdout) // Kubernetes recoge stdout: `kubectl logs`

	ctx, parar := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer parar()

	logBloque(linea,
		fmt.Sprintf("  relay-headers %s  (%s, %s/%s)", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH),
		"  Cola RabbitMQ  ->  log  ->  exchange headers  (+ header estado=procesado)",
		"  pod: "+hostname(),
		linea)

	est := &Estado{Foto: Foto{Fase: FaseIniciando, Desde: time.Now(), Version: Version}}

	// ---------- Paso 1: configuración ----------
	logPaso(1, pasosTotales, "Leyendo configuración desde variables de entorno (ConfigMap + Secret)")
	cfg, err := CargarConfig()
	srv := ServidorHTTP(cfg.PuertoHTTP, est)
	log.Printf("      Probes HTTP en :%d  ->  /healthz (liveness)  /ready (readiness)  / (estado)", cfg.PuertoHTTP)

	if err != nil {
		var ec *ErrorConfiguracion
		errors.As(err, &ec)
		var b strings.Builder
		for _, f := range ec.Faltantes {
			b.WriteString("\n        - falta la variable " + f)
		}
		for _, i := range ec.Invalidas {
			b.WriteString("\n        - valor inválido: " + i)
		}
		esperarCorreccion(ctx, est, cfg, &FalloConfiguracion{
			Paso:     "lectura de configuración",
			Problema: "La configuración está incompleta o tiene valores inválidos:" + b.String(),
			Accion: "Agregue/corrija las variables en el ConfigMap (datos) o en el Secret (credenciales). " +
				"Las variables de entorno solo se leen al arrancar: después de editarlas hay que reiniciar el pod.",
		})
		apagar(srv)
		return
	}
	mostrarConfig(cfg)

	// ---------- Pasos 2 a 6 y procesamiento ----------
	intento := 0
	for {
		err := Ejecutar(ctx, cfg, est)
		if ctx.Err() != nil {
			break
		}
		var fc *FalloConfiguracion
		if errors.As(err, &fc) {
			esperarCorreccion(ctx, est, cfg, fc)
			break
		}
		// Error transitorio: reintentar solo.
		intento++
		est.SumarReintento()
		est.Cambiar(FaseReconexion, err.Error(), "ninguna: el servicio reintenta solo")
		log.Printf("[REINTENTO] Problema transitorio en %v", err)
		log.Printf("            No es un error de configuración, así que se reintenta automáticamente en %s (intento %d).",
			cfg.Reintento, intento)
		select {
		case <-ctx.Done():
		case <-time.After(cfg.Reintento):
			continue
		}
		break
	}

	apagar(srv)
}

func mostrarConfig(cfg *Config) {
	l := []string{
		"      [OK] Configuración completa:",
		"        ORIGEN  " + cfg.Origen.Describir(),
		"                cola='" + cfg.Cola + "'",
		"        DESTINO " + cfg.Destino.Describir(),
		"                exchange='" + cfg.Exchange + "' (tipo headers)  routing key='" + cfg.RoutingKey + "'",
		fmt.Sprintf("        prefetch=%d  reintento=%s  sin_ruta=%s", cfg.Prefetch, cfg.Reintento, cfg.SinRuta),
	}
	if len(cfg.Heredados) > 0 {
		l = append(l, "        Valores de destino heredados del origen (no se definieron):")
		for _, h := range cfg.Heredados {
			l = append(l, "          "+h)
		}
	}
	if cfg.Origen.Host == cfg.Destino.Host && cfg.Origen.Puerto == cfg.Destino.Puerto {
		if cfg.Origen.Vhost == cfg.Destino.Vhost {
			l = append(l, "        Escenario: mismo servidor y mismo vhost.")
		} else {
			l = append(l, "        Escenario: mismo servidor, vhosts distintos.")
		}
	} else {
		l = append(l, "        Escenario: servidores RabbitMQ distintos.")
	}
	log.Print(strings.Join(l, "\n"))
}

// esperarCorreccion deja el servicio vivo pero detenido, explicando que hacer.
// No termina el proceso: si terminara, Kubernetes lo reiniciaría en bucle
// (CrashLoopBackOff) y el mensaje se perdería entre reinicios.
func esperarCorreccion(ctx context.Context, est *Estado, cfg *Config, f *FalloConfiguracion) {
	est.Cambiar(FaseEsperando, f.Problema, f.Accion+" Luego: "+comandoReinicio())
	bloque := []string{
		"##################################################################",
		"#  ACCIÓN REQUERIDA: el servicio NO puede procesar mensajes",
		"##################################################################",
		"  Paso fallido : " + f.Paso,
		"  Problema     : " + f.Problema,
	}
	if f.Detalle != "" {
		bloque = append(bloque, "  RabbitMQ dijo: "+f.Detalle)
	}
	bloque = append(bloque,
		"  Qué hacer    : "+f.Accion,
		"  Después      : reinicie el pod para que vuelva a verificar todo:",
		"                 "+comandoReinicio(),
		"",
		"  El servicio queda VIVO y en espera (no se cae). El pod se verá como 0/1 READY",
		"  porque la readiness probe (/ready) responde 503. Ningún mensaje se pierde:",
		"  siguen en la cola de origen.",
		"##################################################################")
	logBloque(bloque...)

	recordatorio := 60 * time.Second
	if cfg != nil && cfg.Recordatorio > 0 {
		recordatorio = cfg.Recordatorio
	}
	t := time.NewTicker(recordatorio)
	defer t.Stop()
	desde := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Printf("[ESPERANDO] Hace %s que falta corregir: %s  ->  %s  Luego: %s",
				time.Since(desde).Round(time.Second), f.Problema, f.Accion, comandoReinicio())
		}
	}
}

func apagar(srv interface{ Shutdown(context.Context) error }) {
	ctx, cancelar := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelar()
	_ = srv.Shutdown(ctx)
	log.Printf("[APAGADO] Servicio detenido correctamente. Hasta luego.")
}
