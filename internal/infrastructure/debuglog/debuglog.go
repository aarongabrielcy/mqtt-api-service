// Package debuglog contiene helpers puros compartidos por la
// instrumentación de diagnóstico opcional del bridge LG (LG_DEBUG_STATE_LOGS,
// FASE LG-CMD-2E) — truncar cuerpos JSON antes de loguearlos, sin decidir
// nada sobre el logger ni sobre si el flag está activo.
package debuglog

// DefaultMaxBodyLogLength es el máximo razonable de bytes a incluir en un
// log de diagnóstico (JSON raw de LG API, payload normalizado, etc.) — evita
// inundar el log con un body inesperadamente grande.
const DefaultMaxBodyLogLength = 8192

// Truncate recorta data a max bytes. Devuelve el slice sin modificar y
// truncated=false si data ya cabe (o max no es positivo). Nunca entra en
// pánico, incluso con max<=0 o data vacío/nil.
func Truncate(data []byte, max int) (out []byte, truncated bool) {
	if max <= 0 || len(data) <= max {
		return data, false
	}
	return data[:max], true
}
