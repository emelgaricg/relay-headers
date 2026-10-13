package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Fases por las que pasa el servicio. Se exponen en /ready y en /.
const (
	FaseIniciando  = "INICIANDO"
	FaseConectando = "CONECTANDO"
	FaseProcesando = "PROCESANDO"
	FaseReconexion = "RECONECTANDO"
	FaseEsperando  = "ESPERANDO_CORRECCION"
	FaseApagando   = "APAGANDO"
)

// Estado es la "foto" del servicio que consultan Kubernetes (probes) y las personas.
type Foto struct {
	Fase       string    `json:"fase"`
	Motivo     string    `json:"motivo,omitempty"`
	Accion     string    `json:"accion,omitempty"`
	Desde      time.Time `json:"desde"`
	Procesados uint64    `json:"mensajes_procesados"`
	Reintentos uint64    `json:"reintentos_conexion"`
	Version    string    `json:"version"`
}

// Estado protege la Foto con un mutex (la leen las probes desde otro goroutine).
type Estado struct {
	mu sync.Mutex
	Foto
}

func (e *Estado) Cambiar(fase, motivo, accion string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Fase, e.Motivo, e.Accion, e.Desde = fase, motivo, accion, time.Now()
}

func (e *Estado) SumarProcesado() {
	e.mu.Lock()
	e.Procesados++
	e.mu.Unlock()
}

func (e *Estado) SumarReintento() {
	e.mu.Lock()
	e.Reintentos++
	e.mu.Unlock()
}

func (e *Estado) foto() (Foto, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.Foto, e.Fase == FaseProcesando
}

// ServidorHTTP expone:
//
//	/healthz  liveness:  200 mientras el proceso este vivo (aunque falte configuración).
//	          Así Kubernetes NO reinicia el pod en bucle por un error de configuración.
//	/ready    readiness: 200 solo cuando esta procesando mensajes; 503 en otro caso,
//	          con el motivo. El pod se ve como 0/1 en `kubectl get pods`.
//	/         estado completo en JSON (siempre 200), util para `kubectl port-forward`.
func ServidorHTTP(puerto int, e *Estado) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "vivo")
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		f, listo := e.foto()
		w.Header().Set("Content-Type", "application/json")
		if !listo {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(f)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		f, _ := e.foto()
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(f)
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", puerto), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[ERROR] No se pudo abrir el puerto HTTP %d para las probes: %v", puerto, err)
		}
	}()
	return srv
}

// ---------- Ayudas para logs didacticos ----------

const linea = "------------------------------------------------------------------"

func logPaso(n, total int, texto string) {
	log.Printf("[%d/%d] %s", n, total, texto)
}

func logOK(texto string, args ...any) {
	log.Printf("      [OK] "+texto, args...)
}

// logBloque imprime varias lineas como un solo registro para que no se mezclen.
func logBloque(lineas ...string) {
	log.Print("\n" + strings.Join(lineas, "\n"))
}
