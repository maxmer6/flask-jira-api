# -*- coding: utf-8 -*-
"""
flask_jira_refactored.py
========================
Refactorización de flask_jira.py para integración con Power Automate.

CAMBIOS RESPECTO AL ORIGINAL
─────────────────────────────────────────────────────────────────
MÉTODOS HTTP
  GET  /process-data      → GET  ✅  Solo consulta Jira + BD (lectura pura)
  GET  /guardar-historico → POST ✅  CAMBIADO: escribe en PostgreSQL (no idempotente)
  GET  /download-excel    → GET  ✅  Descarga de archivo (idempotente)

  Criterio de decisión:
    • GET   → La operación es SEGURA (no muta nada) e IDEMPOTENTE (mismo
              resultado N veces). Usada para consultas y descargas.
    • POST  → La operación muta estado persistente (DB, ficheros). No es
              idempotente: ejecutarla dos veces produce dos inserciones distintas
              (aunque el DELETE previo lo mitiga, la INTENCIÓN sigue siendo mutar).
              /guardar-historico escribe en PostgreSQL → POST correcto.

RESPUESTAS JSON
  Todos los endpoints devuelven jsonify() con la envoltura estándar:
    {
      "status":    "success" | "error",
      "timestamp": "ISO-8601 UTC",
      "message":   "...",
      "meta":      { totales, parámetros usados, etc. },
      "data":      { payload principal }
    }

  Power Automate accede con expresiones como:
    @body('HTTP')?['data']?['tickets']
    @body('HTTP')?['meta']?['total_issues']
    @body('HTTP')?['status']

VALIDACIÓN
  • _parse_date()  — valida YYYY-MM-DD, lanza ValueError descriptivo
  • _parse_bool()  — acepta bool nativo, "true"/"false", "1"/"0", "si"/"no"
  • _get_params()  — lee query string (GET) y JSON body (POST) de forma unificada

CORS
  Headers Access-Control añadidos en after_request para que el conector
  HTTP de Power Automate no falle en el pre-flight OPTIONS.

OTROS
  • Content-Type: application/json; charset=utf-8 explícito en todas las
    respuestas JSON (Power Automate lo requiere para parsear automáticamente).
  • Credenciales de BD y Jira preferiblemente desde variables de entorno.
  • HTML_TEMPLATE y funciones de HTML intactos (no se tocan, siguen usándose
    en /process-data para el render de correo/navegador).
  • /download-excel devuelve archivo binario, no JSON (correcto por diseño).
  • /health añadido: permite que Power Automate verifique disponibilidad
    antes de ejecutar el flujo principal.

@author: mmerinori
@refactored: Claude (Anthropic) — Feb 2026
"""

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import io
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from flask import Flask, jsonify, make_response, request, send_file, render_template_string
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, text

from dotenv import load_dotenv


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


# ─────────────────────────────────────────────
# APLICACIÓN FLASK
# ─────────────────────────────────────────────
app = Flask(__name__)
app.config["JSON_AS_ASCII"]  = False   # Preserva UTF-8 en respuestas JSON
app.config["JSON_SORT_KEYS"] = False   # Mantiene orden lógico de claves


# ─────────────────────────────────────────────
# CORS — requerido por Power Automate
# ─────────────────────────────────────────────
@app.after_request
def apply_cors(response):
    """
    Power Automate realiza pre-flight OPTIONS antes de cada llamada real.
    Sin estos headers el conector HTTP devuelve 'Network Error' genérico.
    """
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, Accept"
    return response


# ─────────────────────────────────────────────
# CONFIGURACIÓN JIRA
# ─────────────────────────────────────────────
JIRA_URL  = os.getenv("JIRA_URL")
EMAIL     = os.getenv("JIRA_EMAIL")
API_TOKEN = os.getenv("JIRA_TOKEN")

# Validación de seguridad
if not JIRA_URL or not EMAIL or not API_TOKEN:
    raise RuntimeError(
        "Variables de entorno JIRA_URL, JIRA_EMAIL y JIRA_TOKEN son obligatorias."
    )

AUTH         = HTTPBasicAuth(EMAIL, API_TOKEN)
REQ_HEADERS  = {"Accept": "application/json"}
API_ENDPOINT = "/rest/api/3/search/jql"

# Custom Fields
CF_FECHA_FIN    = "customfield_11365"
CF_FECHA_INICIO = "customfield_11363"
CF_SUPERVISOR   = "customfield_11451"
CF_FECHA_COMITE = "customfield_11673"
CF_TIPO_VENTANA = "customfield_10486"

JIRA_FIELDS = [
    "summary", "status", "creator",
    CF_FECHA_INICIO, CF_FECHA_FIN, CF_SUPERVISOR, CF_FECHA_COMITE, CF_TIPO_VENTANA,
]

MESES = ["", "Ene", "Feb", "Mar", "Abr", "May", "Jun",
         "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


# ─────────────────────────────────────────────
# CONFIGURACIÓN POSTGRESQL
# ─────────────────────────────────────────────
PG_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "port":     os.getenv("PG_PORT"),
    "database": os.getenv("PG_DATABASE"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}

# Validación de seguridad PostgreSQL
if not all(PG_CONFIG.values()):
    raise RuntimeError(
        "Variables de entorno PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD son obligatorias."
    )

PG_SCHEMA          = os.getenv("PG_SCHEMA", "proof")
PG_TABLE_EVOLUTIVO = "evol_jira_supervisores_p_im"

DB_URI = (
    f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
    f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
)


# ═══════════════════════════════════════════════════════════════
# UTILIDADES DE RESPUESTA JSON ESTANDARIZADA
# ═══════════════════════════════════════════════════════════════

def _ok(data: Any = None, message: str = "OK",
        meta: Dict = None, status: int = 200):
    """
    Construye la respuesta JSON de éxito canónica.

    Estructura que Power Automate puede parsear directamente:
    {
        "status":    "success",
        "timestamp": "2026-02-17T15:30:00+00:00",
        "message":   "42 issues procesados",
        "meta":      { "total_issues": 42, "fecha_inicio": "2025-11-01", ... },
        "data":      { "tickets": [...], "resumen_mensual": [...], ... }
    }

    Expresiones en Power Automate:
        @body('HTTP')?['data']?['tickets']
        @body('HTTP')?['meta']?['total_issues']
        @equals(body('HTTP')?['status'], 'success')
    """
    body = {
        "status":    "success",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message":   message,
        "meta":      meta if meta is not None else {},
        "data":      data if data is not None else {},
    }
    resp = make_response(jsonify(body), status)
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp


def _err(message: str, errors: List[str] = None, status: int = 500):
    """
    Construye la respuesta JSON de error canónica.

    Power Automate puede usar una condición sobre body('HTTP')?['status']
    para bifurcar el flujo y leer body('HTTP')?['errors'] para diagnóstico.
    """
    body = {
        "status":    "error",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message":   message,
        "errors":    errors if errors else [],
        "data":      {},
    }
    resp = make_response(jsonify(body), status)
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp


def handle_errors(f):
    """
    Decorador de manejo centralizado de excepciones.
    Garantiza que el conector HTTP de Power Automate SIEMPRE reciba JSON
    válido, incluso ante errores inesperados (Flask por defecto devuelve HTML).
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as exc:
            # Parámetros inválidos → 400 Bad Request
            return _err("Parámetro inválido", errors=[str(exc)], status=400)
        except requests.exceptions.Timeout:
            # Jira no respondió → 504 Gateway Timeout
            return _err(
                "Timeout al conectar con Jira",
                errors=["El servicio externo no respondió en el tiempo esperado"],
                status=504,
            )
        except requests.exceptions.ConnectionError as exc:
            # Red caída → 502 Bad Gateway
            return _err("Error de conexión con Jira", errors=[str(exc)], status=502)
        except Exception as exc:
            traceback.print_exc()
            return _err("Error interno del servidor", errors=[str(exc)], status=500)
    return wrapper


# ─────────────────────────────────────────────
# VALIDACIÓN Y PARSEO DE PARÁMETROS
# ─────────────────────────────────────────────

def _parse_date(value: str, param_name: str) -> str:
    """
    Valida formato YYYY-MM-DD y devuelve el valor como string limpio.
    Lanza ValueError con mensaje descriptivo si el formato es incorrecto.

    Conversión de tipo explícita: Power Automate puede enviar la fecha
    como string o como datetime serializado; ambos se normalizan aquí.
    """
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()[:10]          # Trunca a YYYY-MM-DD si trae hora
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise ValueError(
            f"Parámetro '{param_name}' con formato inválido: '{value}'. "
            "Se esperaba YYYY-MM-DD (ej. 2026-02-17)."
        )


def _parse_bool(value: Any, default: bool = False) -> bool:
    """
    Conversión robusta a bool.
    Power Automate puede enviar true/false como bool nativo o como string.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "si", "sí")
    return default


def _get_params() -> Dict:
    """
    Lee parámetros de forma unificada:
      - GET:  query string  ?inicio=2025-11-01&fin=2026-02-17
      - POST: JSON body     {"inicio": "2025-11-01", "fin": "2026-02-17"}

    El body JSON tiene prioridad sobre query string para evitar
    ambigüedad cuando Power Automate envía ambos.
    """
    params = dict(request.args)
    if request.is_json:
        body = request.get_json(silent=True) or {}
        params.update(body)
    return params


# ═══════════════════════════════════════════════════════════════
# LÓGICA DE NEGOCIO — sin cambios funcionales respecto al original
# ═══════════════════════════════════════════════════════════════

def get_value(field: Any) -> Optional[str]:
    """Extrae el valor de un campo Jira (dict, string o None)."""
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get("value") or field.get("displayName") or field.get("name")
    return field


def calcular_supervisor_final(row: pd.Series) -> str:
    """Aplica las reglas de negocio para determinar el supervisor final."""
    resumen       = row.get("resumen",       "") or ""
    supervisor    = row.get("supervisor",    "") or ""
    registrado_por = row.get("registrado_por", "") or ""
    estado        = row.get("estado",        "") or ""

    if estado not in ("Programado", "Implantación en Curso"):
        return supervisor

    if "MP@" in resumen and registrado_por not in (
        "Lydia Taco Cáceres", "Maria Amparo Meza Obando"
    ):
        return "HERMANN ANDRE ROBLES CUADROS"

    if "|" in supervisor:
        return supervisor.split("|")[0].strip()

    return supervisor


def obtener_issues(fecha_inicio: str, fecha_fin: str) -> List[Dict[str, Any]]:
    """
    Consulta paginada a Jira usando nextPageToken.
    GET a Jira es correcto: solo LEEMOS datos, no mutamos nada en Jira.
    """
    jql_query = (
        f'project = TPRO '
        f'AND (status = "Programado" OR status = "Implantación en Curso") '
        f'AND {CF_FECHA_FIN} >= "{fecha_inicio} 00:00" '
        f'AND {CF_FECHA_FIN} <= "{fecha_fin} 23:59"'
    )
    print(f"[{datetime.now():%H:%M:%S}] JQL: {jql_query}")

    url        = f"{JIRA_URL}{API_ENDPOINT}"
    all_issues = []
    next_token = None

    while True:
        params = {
            "jql":        jql_query,
            "fields":     ",".join(JIRA_FIELDS),
            "maxResults": 100,
        }
        if next_token:
            params["nextPageToken"] = next_token

        try:
            response = requests.get(
                url, headers=REQ_HEADERS, params=params,
                auth=AUTH, timeout=60,
            )

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 60))
                print(f"[{datetime.now():%H:%M:%S}] Rate limit, esperando {wait}s…")
                time.sleep(wait)
                continue

            if response.status_code != 200:
                print(f"[{datetime.now():%H:%M:%S}] Error {response.status_code}: {response.text[:300]}")
                break

            data   = response.json()
            issues = data.get("issues", [])
            all_issues.extend(issues)
            print(f"[{datetime.now():%H:%M:%S}] {len(issues)} issues | acumulado: {len(all_issues)}")

            next_token = data.get("nextPageToken")
            if not next_token:
                break

            time.sleep(0.2)

        except Exception as exc:
            print(f"[{datetime.now():%H:%M:%S}] Error en request: {exc}")
            break

    return all_issues


def construir_dataframe(issues: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transforma la lista cruda de issues Jira a un DataFrame normalizado."""
    rows = []
    for issue in issues:
        try:
            f = issue["fields"]
            rows.append({
                "cod_trabajo_programado": issue["key"],
                "tipo_ventana":           get_value(f.get(CF_TIPO_VENTANA)),
                "estado":                 f["status"]["name"],
                "resumen":                f.get("summary"),
                "fecha_inicio":           f.get(CF_FECHA_INICIO),
                "fecha_fin":              f.get(CF_FECHA_FIN),
                "supervisor":             get_value(f.get(CF_SUPERVISOR)),
                "fecha_comite":           f.get(CF_FECHA_COMITE),
                "registrado_por":         get_value(f.get("creator")),
            })
        except Exception as exc:
            print(f"[{datetime.now():%H:%M:%S}] Error procesando {issue.get('key')}: {exc}")

    df = pd.DataFrame(rows)

    if not df.empty:
        for col in ("fecha_inicio", "fecha_fin", "fecha_comite"):
            df[col] = (
                pd.to_datetime(df[col], errors="coerce", utc=True)
                .dt.tz_convert("America/Lima")
                .dt.tz_localize(None)
            )
        df["supervisor_final"] = df.apply(calcular_supervisor_final, axis=1)

    return df


def generar_mensual(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupación mensual por supervisor, registrado_por y estado."""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["mes"] = df["fecha_fin"].dt.strftime("%Y-%m")
    return (
        df.groupby(["supervisor_final", "registrado_por", "mes", "estado"])
        .size()
        .reset_index(name="cantidad")
    )


# ─────────────────────────────────────────────
# BASE DE DATOS — EVOLUTIVO
# ─────────────────────────────────────────────

def crear_engine_db():
    """Crea engine SQLAlchemy. Devuelve None si falla (no lanza excepción)."""
    try:
        engine = create_engine(DB_URI, pool_pre_ping=True)
        print(f"[{datetime.now():%H:%M:%S}] ✅ Conexión BD establecida")
        return engine
    except Exception as exc:
        print(f"[{datetime.now():%H:%M:%S}] ❌ Error BD: {exc}")
        return None


def guardar_historico_evolutivo(df: pd.DataFrame,
                                 fecha_inicio: str,
                                 fecha_fin: str) -> Dict:
    """
    Persiste el snapshot en PostgreSQL.
    Devuelve un dict con el resultado para incluirlo en la respuesta JSON.

    Por qué POST para el endpoint que llama a esta función:
      • Ejecutar dos veces con los mismos parámetros produce dos operaciones
        DELETE + INSERT distintas en la base de datos → no es idempotente.
      • La operación tiene EFECTO SECUNDARIO OBSERVABLE (modifica la BD).
      → POST es el método HTTP correcto según RFC 9110.
    """
    if df.empty:
        return {"guardado": False, "motivo": "DataFrame vacío, sin datos para persistir"}

    engine = crear_engine_db()
    if not engine:
        return {"guardado": False, "motivo": "Sin conexión a la base de datos"}

    try:
        resumen = (
            df.groupby(["supervisor_final", "estado"])
            .size()
            .unstack(fill_value=0)
        )
        resumen["total_registros"] = resumen.sum(axis=1)
        resumen = resumen.reset_index()

        resumen = resumen.rename(columns={
            "Implantación en Curso": "total_implantacion",
            "Programado":            "total_programado",
            "supervisor_final":      "supervisor",
        })

        for col in ("total_implantacion", "total_programado"):
            if col not in resumen.columns:
                resumen[col] = 0

        fecha_fin_obj    = datetime.strptime(fecha_fin,    "%Y-%m-%d").date()
        fecha_inicio_obj = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
        fecha_corte      = datetime.combine(fecha_fin_obj, datetime.max.time())

        resumen["fecha_corte"]    = fecha_corte
        resumen["fecha_inicio"]   = fecha_inicio_obj
        resumen["fecha_fin"]      = fecha_fin_obj
        resumen["fecha_ejecucion"] = datetime.now()

        cols_finales = [
            "fecha_corte", "supervisor", "total_registros",
            "total_implantacion", "total_programado",
            "fecha_inicio", "fecha_fin", "fecha_ejecucion",
        ]
        resumen_final = resumen[cols_finales]

        with engine.connect() as conn:
            conn.execute(
                text(f"DELETE FROM {PG_SCHEMA}.{PG_TABLE_EVOLUTIVO} WHERE fecha_fin = :fd"),
                {"fd": fecha_fin_obj},
            )
            conn.commit()

        resumen_final.to_sql(
            PG_TABLE_EVOLUTIVO, engine,
            schema=PG_SCHEMA, if_exists="append",
            index=False, method="multi",
        )

        n = len(resumen_final)
        print(f"[{datetime.now():%H:%M:%S}] ✅ Histórico guardado: {n} supervisores")
        return {
            "guardado":            True,
            "supervisores_guardados": n,
            "fecha_corte":         fecha_fin,
        }

    except Exception as exc:
        traceback.print_exc()
        raise RuntimeError(f"Error al guardar histórico en BD: {exc}") from exc
    finally:
        engine.dispose()


def consultar_historico_evolutivo() -> pd.DataFrame:
    """Consulta las últimas 5 fechas de corte desde PostgreSQL."""
    engine = crear_engine_db()
    if not engine:
        return pd.DataFrame()

    try:
        query = f"""
        WITH ultimas_fechas AS (
            SELECT DISTINCT fecha_corte::DATE AS fecha
            FROM {PG_SCHEMA}.{PG_TABLE_EVOLUTIVO}
            ORDER BY fecha DESC
            LIMIT 5
        )
        SELECT supervisor,
               fecha_fin::DATE AS fecha_orden,
               total_registros
        FROM {PG_SCHEMA}.{PG_TABLE_EVOLUTIVO}
        WHERE fecha_corte::DATE IN (SELECT fecha FROM ultimas_fechas)
        ORDER BY supervisor, fecha_fin
        """
        df = pd.read_sql(query, engine)
        if df.empty:
            return pd.DataFrame()

        df_pivot = df.pivot_table(
            index="supervisor", columns="fecha_orden",
            values="total_registros", fill_value=0,
        )
        print(f"[{datetime.now():%H:%M:%S}] ✅ Histórico: {len(df_pivot)} supervisores, {len(df_pivot.columns)} fechas")
        return df_pivot

    except Exception as exc:
        print(f"[{datetime.now():%H:%M:%S}] ❌ Error consultando histórico: {exc}")
        return pd.DataFrame()
    finally:
        engine.dispose()


def generar_evolutivo(df: pd.DataFrame, usar_bd: bool = True) -> pd.DataFrame:
    """Devuelve el DataFrame pivoteado para la tabla evolutiva."""
    if usar_bd:
        df_evol = consultar_historico_evolutivo()
        if not df_evol.empty:
            return df_evol
        print(f"[{datetime.now():%H:%M:%S}] ⚠️  Sin histórico en BD, usando datos actuales")

    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["fecha_corte"] = (datetime.now() - timedelta(days=1)).date()
    return (
        df.groupby(["supervisor_final", "fecha_corte"])
        .size()
        .reset_index(name="total_registros")
        .pivot_table(
            index="supervisor_final", columns="fecha_corte",
            values="total_registros", fill_value=0,
        )
    )


# ─────────────────────────────────────────────
# HELPERS DE SERIALIZACIÓN
# ─────────────────────────────────────────────

def _df_to_records(df: pd.DataFrame) -> List[Dict]:
    """
    Convierte un DataFrame a lista de dicts serializables por jsonify.
    Maneja Timestamp, NaT y NaN para evitar errores de serialización JSON.
    """
    records = []
    for _, row in df.iterrows():
        rec = {}
        for col, val in row.items():
            if isinstance(col, tuple):
                col = "_".join(str(c) for c in col)
            col = str(col)

            if pd.isna(val) if not isinstance(val, str) else False:
                rec[col] = None
            elif hasattr(val, "isoformat"):     # datetime / Timestamp
                rec[col] = val.isoformat()
            elif hasattr(val, "item"):          # numpy int/float
                rec[col] = val.item()
            else:
                rec[col] = val
        records.append(rec)
    return records


def _evolutivo_to_dict(df_evol: pd.DataFrame) -> List[Dict]:
    """
    Serializa el DataFrame pivoteado evolutivo a lista de dicts.
    Columnas fecha (índice de columnas del pivot) → string "Mes-DD".
    """
    if df_evol is None or df_evol.empty:
        return []

    result = []
    cols_fmt = []
    for c in df_evol.columns:
        if hasattr(c, "month"):
            cols_fmt.append(f"{MESES[c.month]}-{c.day:02d}")
        else:
            cols_fmt.append(str(c))

    for supervisor, row in df_evol.iterrows():
        entry = {"supervisor": str(supervisor)}
        for label, val in zip(cols_fmt, row):
            entry[label] = int(val) if not pd.isna(val) else 0
        result.append(entry)

    return result


# ─────────────────────────────────────────────
# HTML — sin modificaciones (mantiene compatibilidad con render_template_string)
# ─────────────────────────────────────────────

def fecha_corta_es(fecha) -> str:
    if isinstance(fecha, str):
        fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
    return f"{MESES[fecha.month]}-{fecha.day:02d}"


def generar_tabla_resumen_html(df_pivot: pd.DataFrame) -> str:
    if df_pivot.empty:
        return "<p>No hay datos</p>"

    style_th  = "background:#d4e8f0;color:#003d5c;padding:4px;border:1px solid #a6c9d7;font-size:11px;text-align:center;"
    style_td  = "padding:3px;border:1px solid #d0e4ed;font-size:11px;text-align:center;"
    style_sup = "background:#5a8ca3;color:#fff;font-weight:bold;"

    pivot = df_pivot.pivot_table(
        index=["supervisor_final", "registrado_por"],
        columns=["mes", "estado"], values="cantidad",
        aggfunc="sum", fill_value=0,
    ).sort_index(axis=1)

    pivot["Total"]   = pivot.sum(axis=1)
    totales_sup      = pivot.groupby(level=0).sum()
    orden            = totales_sup.sort_values("Total", ascending=False).index
    estados          = sorted(df_pivot["estado"].unique())
    meses            = sorted(df_pivot["mes"].unique())
    meses_label      = [f"{MESES[int(m[5:])]}-{m[2:4]}" for m in meses]

    html = [
        '<div><h3 style="color:#003d5c;font-size:13px;margin-bottom:8px;">'
        'Tabla resumen de tickets vencidos en estado "Programado" o "Implantación en curso"'
        "</h3></div>",
        '<table cellspacing="0" cellpadding="0" style="border-collapse:collapse;'
        'border:2px solid #7fa8ba;font-family:Calibri,Arial,sans-serif;width:100%;">',
        "<tr>",
        f'<th rowspan="2" style="{style_th}min-width:140px;">SUPERVISOR</th>',
    ]
    for est in estados:
        html.append(f'<th colspan="{len(meses)}" style="{style_th}">{est}</th>')
    html.append(f'<th rowspan="2" style="{style_th}border-left:2px solid #7fa8ba;">Total</th></tr>')
    html.append("<tr>")
    for _ in estados:
        for ml in meses_label:
            html.append(f'<th style="{style_th}">{ml}</th>')
    html.append("</tr>")

    for sup in orden:
        if sup not in pivot.index.get_level_values(0):
            continue
        datos     = pivot.loc[sup]
        if isinstance(datos, pd.Series):
            datos = pd.DataFrame([datos])
        # .iloc[0] evita FutureWarning cuando .loc devuelve una Series de un elemento
        # en lugar de un escalar (ocurre con MultiIndex o índices duplicados)
        val_total = totales_sup.loc[sup, "Total"]
        total_sup = int(val_total.iloc[0]) if isinstance(val_total, pd.Series) else int(val_total)

        html.append("<tr>")
        html.append(f'<td style="{style_td}{style_sup}text-align:left;padding-left:5px;">▼ {sup}</td>')
        for est in estados:
            for m in meses:
                v = totales_sup.loc[sup, (m, est)] if (m, est) in totales_sup.columns else 0
                html.append(f'<td style="{style_td}{style_sup}">{int(v) if v else ""}</td>')
        html.append(f'<td style="{style_td}{style_sup}border-left:2px solid #7fa8ba;">{total_sup}</td></tr>')

        detalle = datos.sort_values("Total", ascending=False)
        alt = False
        for reg, row in detalle.iterrows():
            bg = "#f5f9fb" if alt else "#fff"
            alt = not alt
            html.append("<tr>")
            html.append(f'<td style="{style_td}background:{bg};text-align:left;padding-left:15px;">{reg}</td>')
            for est in estados:
                for m in meses:
                    v = row.get((m, est), 0)
                    html.append(f'<td style="{style_td}background:{bg};">{int(v) if v else ""}</td>')
            val_row_total = row["Total"]
            total_row = int(val_row_total.iloc[0]) if isinstance(val_row_total, pd.Series) else int(val_row_total)
            html.append(f'<td style="{style_td}background:{bg};border-left:2px solid #7fa8ba;">{total_row}</td></tr>')

    html.append("</table></div>")
    return "".join(html)


def generar_tabla_evolutivo_html(df_evol: pd.DataFrame) -> str:
    if df_evol is None or df_evol.empty:
        return "<p>No hay datos evolutivos</p>"

    style_th = "background:#d4e8f0;color:#003d5c;padding:4px;border:1px solid #a6c9d7;font-size:11px;text-align:center;"
    style_td = "padding:3px;border:1px solid #d0e4ed;font-size:11px;text-align:center;"

    df_evol = df_evol.copy()
    df_evol.columns = [
        fecha_corta_es(c) if isinstance(c, (datetime, pd.Timestamp)) else str(c)
        for c in df_evol.columns
    ]
    if len(df_evol.columns) > 0:
        df_evol = df_evol.sort_values(df_evol.columns[-1], ascending=False)

    html = [
        '<div><h3 style="color:#003d5c;font-size:13px;margin-bottom:8px;">'
        "Evolución de tickets pendientes de cierre</h3></div>",
        '<table cellspacing="0" cellpadding="0" style="border-collapse:collapse;'
        'border:2px solid #7fa8ba;font-family:Calibri,Arial,sans-serif;">',
        f'<tr><th style="{style_th}">RESPONSABLE</th>',
    ]
    for col in df_evol.columns:
        html.append(f'<th style="{style_th}">{col}</th>')
    html.append("</tr>")

    alt = False
    for sup, row in df_evol.iterrows():
        bg = "#f5f9fb" if alt else "#fff"
        alt = not alt
        html.append(f'<tr><td style="{style_td}background:{bg};text-align:left;padding-left:5px;font-weight:600;">{sup}</td>')
        for v in row:
            html.append(f'<td style="{style_td}background:{bg};">{int(v) if v else "-"}</td>')
        html.append("</tr>")

    html.append("</table></div>")
    return "".join(html)


HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Reporte TPRO Jira</title>
<style>
    body { margin:0; padding:20px; font-family:Calibri,Arial,sans-serif; color:#1c1b1b; background-color:#f5f5f5; }
    .container { max-width:100%; margin:0 auto; background-color:white; padding:30px; border-radius:8px; box-shadow:0 2px 10px rgba(0,0,0,0.1); }
    .header { font-size:14px; line-height:1.6; margin-bottom:25px; color:#333; }
    .tables-wrapper { width:100%; border-collapse:collapse; }
    .tables-wrapper td { vertical-align:top; padding:0 10px; }
    .table-section-left  { width:58%; }
    .table-section-right { width:42%; }
</style>
</head>
<body>
<div class="container">
    <div class="header">
        Estimados,<br><br>
        Adjunto la relación de tickets TPRO Jira que se encuentran vencidos (su periodo programado ya finalizó)
        y permanecen en estado <strong>"Programado"</strong> o <strong>"Implantación en curso"</strong>.
        Agradeceremos su apoyo para realizar la <strong>regularización correspondiente a la brevedad posible.</strong><br>
        Les recordamos que, según el procedimiento vigente, <strong>cada ticket debe gestionarse dentro del periodo de
        ejecución definido en el propio TPRO</strong>, por tanto las fechas de apertura y cierre del ticket deben reflejar
        exactamente el inicio y fin reales del trabajo. En caso de no ejecutarse, el ticket debe ser cancelado.
        Este cumplimiento garantiza la correcta trazabilidad dentro del proceso de Gestión de Cambios.<br><br>
        La fecha de corte de este reporte es del <strong>{{ fecha_corte }}</strong>
    </div>
    <table class="tables-wrapper" cellpadding="0" cellspacing="0">
        <tr>
            <td class="table-section-left">{{ tabla_resumen|safe }}</td>
            <td class="table-section-right">{{ tabla_evolutivo|safe }}</td>
        </tr>
    </table>
</div>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────
# GET /
# ─────────────────────────────────────────────
@app.route("/")
def index():
    """Documentación de la API en JSON — fácil de consumir desde Power Automate."""
    return _ok(
        data={
            "endpoints": [
                {
                    "method":      "GET",
                    "path":        "/process-data",
                    "descripcion": "Consulta Jira y devuelve resumen mensual + evolutivo en JSON. "
                                   "Opcionalmente guarda snapshot en BD con ?guardar=true.",
                    "params": {
                        "inicio":  "YYYY-MM-DD (default 2025-11-01)",
                        "fin":     "YYYY-MM-DD (default hoy)",
                        "guardar": "bool (default false) — si true, persiste en BD vía lógica interna",
                    },
                    "power_automate": {
                        "tickets":         "@body('HTTP')?['data']?['tickets']",
                        "resumen_mensual": "@body('HTTP')?['data']?['resumen_mensual']",
                        "evolutivo":       "@body('HTTP')?['data']?['evolutivo']",
                        "html_completo":   "@body('HTTP')?['data']?['html_completo']",
                    },
                },
                {
                    "method":      "POST",
                    "path":        "/guardar-historico",
                    "descripcion": "Guarda snapshot histórico en PostgreSQL. "
                                   "POST porque escribe en BD (no idempotente).",
                    "body_json": {
                        "inicio": "YYYY-MM-DD (default 2025-11-01)",
                        "fin":    "YYYY-MM-DD (default hoy)",
                    },
                    "power_automate": {
                        "supervisores_guardados": "@body('HTTP')?['data']?['supervisores_guardados']",
                        "guardado":               "@body('HTTP')?['data']?['guardado']",
                    },
                },
                {
                    "method":      "GET",
                    "path":        "/download-excel",
                    "descripcion": "Descarga Excel con hojas Detalle, Mensual y Evolutivo.",
                    "params": {
                        "inicio": "YYYY-MM-DD (default 2025-11-01)",
                        "fin":    "YYYY-MM-DD (default hoy)",
                    },
                    "nota": "Devuelve binario .xlsx, no JSON. Use 'Get file content' en Power Automate.",
                },
                {
                    "method":      "GET",
                    "path":        "/health",
                    "descripcion": "Verifica disponibilidad de la API y conectividad con Jira.",
                    "power_automate": {
                        "jira": "@body('HTTP')?['data']?['jira']",
                        "api":  "@body('HTTP')?['data']?['api']",
                    },
                },
            ]
        },
        message="API Flask TPRO Jira — documentación de endpoints",
        meta={"version": "2.0", "puerto": 5000},
    )


# ─────────────────────────────────────────────────────────────────
# ╔═══════════════════════════════════════════════════════════════╗
# ║  GET /process-data                                           ║
# ╠═══════════════════════════════════════════════════════════════╣
# ║  Método: GET ✅                                              ║
# ║  Razón  : Solo CONSULTA Jira (GET externo) y BD (SELECT).   ║
# ║           No muta ningún estado → seguro e idempotente.      ║
# ║                                                               ║
# ║  Si ?guardar=true se DELEGA la escritura al endpoint POST    ║
# ║  /guardar-historico internamente. La lógica de escritura     ║
# ║  se llama desde aquí por conveniencia, pero el endpoint      ║
# ║  principal sigue siendo de LECTURA.                          ║
# ╚═══════════════════════════════════════════════════════════════╝
# ─────────────────────────────────────────────────────────────────
@app.route("/process-data", methods=["GET", "OPTIONS"])
@handle_errors
def process_data():
    """
    Endpoint principal de consulta.

    Devuelve JSON estructurado con:
      • tickets       — lista de issues normalizados
      • resumen_mensual — agrupación por supervisor / mes / estado
      • evolutivo     — evolución histórica (últimas 5 fechas de la BD)
      • html_completo — HTML renderizado listo para pegar en un correo

    Power Automate puede acceder a:
        @body('HTTP')?['data']?['tickets']
        @body('HTTP')?['data']?['resumen_mensual']
        @body('HTTP')?['data']?['evolutivo']
        @body('HTTP')?['data']?['html_completo']
        @body('HTTP')?['meta']?['total_issues']
    """
    if request.method == "OPTIONS":
        return _ok(message="CORS OK")

    params = _get_params()

    # ── Validación explícita de tipos ─────────────────────────────
    fecha_inicio = _parse_date(
        params.get("inicio", "2025-11-01"), "inicio"
    )
    fecha_fin = _parse_date(
        params.get("fin", datetime.now().strftime("%Y-%m-%d")), "fin"
    )
    guardar = _parse_bool(params.get("guardar", False))

    print(f"\n{'='*55}")
    print(f"[{datetime.now():%H:%M:%S}] GET /process-data")
    print(f"[{datetime.now():%H:%M:%S}] Rango  : {fecha_inicio} → {fecha_fin}")
    print(f"[{datetime.now():%H:%M:%S}] Guardar: {guardar}")
    print(f"{'='*55}\n")

    # ── Consulta a Jira (GET, solo lectura) ───────────────────────
    issues = obtener_issues(fecha_inicio, fecha_fin)

    if not issues:
        return _err(
            "No se encontraron issues para el rango indicado",
            errors=[f"Rango: {fecha_inicio} → {fecha_fin}. "
                    "Verifica fechas y permisos de acceso a Jira."],
            status=404,
        )

    print(f"[{datetime.now():%H:%M:%S}] Total issues: {len(issues)}")

    # ── Procesamiento ─────────────────────────────────────────────
    df         = construir_dataframe(issues)
    df_mensual = generar_mensual(df)

    # ── Guardar histórico si se solicitó ─────────────────────────
    resultado_guardado = {}
    if guardar:
        try:
            resultado_guardado = guardar_historico_evolutivo(df, fecha_inicio, fecha_fin)
        except Exception as exc:
            print(f"[{datetime.now():%H:%M:%S}] ⚠️  No se pudo guardar histórico: {exc}")
            resultado_guardado = {"guardado": False, "motivo": str(exc)}

    # ── Evolutivo ─────────────────────────────────────────────────
    df_evolutivo = generar_evolutivo(df, usar_bd=True)

    # ── Serializar a JSON ─────────────────────────────────────────
    tickets_json        = _df_to_records(df)
    resumen_mensual_json = _df_to_records(df_mensual)
    evolutivo_json      = _evolutivo_to_dict(df_evolutivo)

    # ── Fecha de corte en texto ───────────────────────────────────
    meses_es = [
        "", "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    ]
    fc        = datetime.strptime(fecha_fin, "%Y-%m-%d")
    fecha_txt = f"{fc.day} de {meses_es[fc.month]} del {fc.year}"

    # ── HTML completo (para correo / visualización) ───────────────
    tabla_resumen   = generar_tabla_resumen_html(df_mensual)
    tabla_evolutivo = generar_tabla_evolutivo_html(df_evolutivo)
    html_completo   = render_template_string(
        HTML_TEMPLATE,
        tabla_resumen=tabla_resumen,
        tabla_evolutivo=tabla_evolutivo,
        fecha_corte=fecha_txt,
    )

    return _ok(
        data={
            "tickets":           tickets_json,
            "resumen_mensual":   resumen_mensual_json,
            "evolutivo":         evolutivo_json,
            "html_completo":     html_completo,
            "tabla_resumen_html":   tabla_resumen,
            "tabla_evolutivo_html": tabla_evolutivo,
            "fecha_corte_texto": fecha_txt,
            "historico":         resultado_guardado,
        },
        message=f"{len(issues)} issues procesados correctamente",
        meta={
            "total_issues":      len(issues),
            "total_supervisores": int(df["supervisor_final"].nunique()) if not df.empty else 0,
            "fecha_inicio":      fecha_inicio,
            "fecha_fin":         fecha_fin,
            "guardar_solicitado": guardar,
        },
    )


# ─────────────────────────────────────────────────────────────────
# ╔═══════════════════════════════════════════════════════════════╗
# ║  POST /guardar-historico                                     ║
# ╠═══════════════════════════════════════════════════════════════╣
# ║  Método: POST ✅  ← CAMBIADO del GET original               ║
# ║  Razón  : Escribe (DELETE + INSERT) en PostgreSQL.           ║
# ║           Ejecutarlo dos veces con los mismos parámetros     ║
# ║           produce dos operaciones distintas en la BD         ║
# ║           → NO es idempotente → POST es correcto.            ║
# ╚═══════════════════════════════════════════════════════════════╝
# ─────────────────────────────────────────────────────────────────
@app.route("/guardar-historico", methods=["POST", "OPTIONS"])
@handle_errors
def guardar_historico_endpoint():
    """
    Persiste el snapshot actual en la tabla evolutiva de PostgreSQL.

    Body JSON esperado:
        { "inicio": "2025-11-01", "fin": "2026-02-17" }

    Respuesta JSON para Power Automate:
        @body('HTTP')?['data']?['guardado']               → true/false
        @body('HTTP')?['data']?['supervisores_guardados'] → número
        @body('HTTP')?['data']?['fecha_corte']            → "2026-02-17"
        @body('HTTP')?['meta']?['total_issues']           → número
    """
    if request.method == "OPTIONS":
        return _ok(message="CORS OK")

    params = _get_params()

    fecha_inicio = _parse_date(
        params.get("inicio", "2025-11-01"), "inicio"
    )
    fecha_fin = _parse_date(
        params.get("fin", datetime.now().strftime("%Y-%m-%d")), "fin"
    )

    print(f"\n{'='*55}")
    print(f"[{datetime.now():%H:%M:%S}] POST /guardar-historico")
    print(f"[{datetime.now():%H:%M:%S}] Rango: {fecha_inicio} → {fecha_fin}")
    print(f"{'='*55}\n")

    issues = obtener_issues(fecha_inicio, fecha_fin)

    if not issues:
        return _err(
            "No se encontraron issues para guardar",
            errors=[f"Rango: {fecha_inicio} → {fecha_fin}"],
            status=404,
        )

    df              = construir_dataframe(issues)
    resultado_bd    = guardar_historico_evolutivo(df, fecha_inicio, fecha_fin)

    return _ok(
        data=resultado_bd,
        message="Snapshot histórico guardado correctamente en PostgreSQL",
        meta={
            "total_issues":      len(issues),
            "total_supervisores": int(df["supervisor_final"].nunique()) if not df.empty else 0,
            "fecha_inicio":      fecha_inicio,
            "fecha_fin":         fecha_fin,
        },
    )


# ─────────────────────────────────────────────────────────────────
# ╔═══════════════════════════════════════════════════════════════╗
# ║  GET /download-excel                                         ║
# ╠═══════════════════════════════════════════════════════════════╣
# ║  Método: GET ✅                                              ║
# ║  Razón  : Descarga de recurso. Idempotente. No muta estado.  ║
# ║  Nota   : Devuelve binario .xlsx, no JSON.                   ║
# ║           En Power Automate usar acción "Get file content"   ║
# ║           o guardar en SharePoint/OneDrive directamente.     ║
# ╚═══════════════════════════════════════════════════════════════╝
# ─────────────────────────────────────────────────────────────────
@app.route("/download-excel", methods=["GET"])
@handle_errors
def download_excel():
    """
    Genera y descarga el archivo Excel con tres hojas:
      • Detalle    — todos los issues individuales
      • Mensual    — agrupación por supervisor/mes/estado
      • Evolutivo  — evolución histórica pivoteada
    """
    params = _get_params()

    fecha_inicio = _parse_date(
        params.get("inicio", "2025-11-01"), "inicio"
    )
    fecha_fin = _parse_date(
        params.get("fin", datetime.now().strftime("%Y-%m-%d")), "fin"
    )

    print(f"\n[{datetime.now():%H:%M:%S}] GET /download-excel | {fecha_inicio} → {fecha_fin}")

    issues = obtener_issues(fecha_inicio, fecha_fin)

    if not issues:
        return _err("No hay datos para exportar", status=404)

    df           = construir_dataframe(issues)
    df_mensual   = generar_mensual(df)
    df_evolutivo = generar_evolutivo(df, usar_bd=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer,           sheet_name="Detalle",   index=False)
        df_mensual.to_excel(writer,   sheet_name="Mensual",   index=False)
        df_evolutivo.to_excel(writer, sheet_name="Evolutivo")

    output.seek(0)
    filename = f"Detalle_jira_{fecha_fin.replace('-', '')}.xlsx"

    # ── Detección del cliente ──────────────────────────────────────
    # Power Automate envía Accept: application/json → necesita Base64
    # Navegador / curl envían Accept: */* o application/octet-stream
    #   → recibe el binario directo para descarga normal
    import base64
    from flask import Response as FlaskResponse
    accept = request.headers.get("Accept", "")

    if "application/json" in accept:
        archivo_base64 = base64.b64encode(output.read()).decode("utf-8")
        return _ok(
            data={
                "filename":    filename,
                "filecontent": archivo_base64,
                "encoding":    "base64",
                "mimetype":    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            message=f"Excel generado correctamente: {filename}",
            meta={
                "fecha_inicio": fecha_inicio,
                "fecha_fin":    fecha_fin,
            },
        )

    # Descarga binaria directa (navegador / Postman / curl)
    output.seek(0)
    return FlaskResponse(
        output.read(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─────────────────────────────────────────────────────────────────
# ╔═══════════════════════════════════════════════════════════════╗
# ║  GET /health                                                 ║
# ╠═══════════════════════════════════════════════════════════════╣
# ║  Método: GET ✅  Nuevo endpoint                              ║
# ║  Razón  : Monitoreo/warm-up. Siempre idempotente.            ║
# ║           Power Automate puede usarlo como "ping" antes de   ║
# ║           lanzar el flujo principal, evitando timeouts en    ║
# ║           instancias dormidas (Render free tier, etc.).      ║
# ╚═══════════════════════════════════════════════════════════════╝
# ─────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    """
    Verifica disponibilidad de la API y conectividad con Jira.

    Power Automate:
        @body('HTTP')?['data']?['jira']  → "ok" | "error"
        @body('HTTP')?['data']?['api']   → "ok"
    """
    jira_ok = False
    try:
        r = requests.get(
            f"{JIRA_URL}/rest/api/3/myself",
            auth=AUTH, headers=REQ_HEADERS, timeout=10,
        )
        jira_ok = (r.status_code == 200)
    except Exception:
        pass

    status_code = 200 if jira_ok else 503
    return _ok(
        data={
            "api":  "ok",
            "jira": "ok" if jira_ok else "error",
        },
        message="API operativa" if jira_ok else "Jira no disponible",
        meta={"version": "2.0"},
        status=status_code,
    )


# ─────────────────────────────────────────────────────────────────
# HANDLERS PARA RUTAS AUTOMÁTICAS DEL NAVEGADOR
# Chrome/Edge las solicitan automáticamente al abrir la API.
# Sin estos handlers generan 404 que ensucian los logs.
# No tienen impacto en Power Automate.
# ─────────────────────────────────────────────────────────────────
@app.route("/favicon.ico")
def favicon():
    """Chrome solicita favicon automáticamente — respuesta vacía sin error."""
    return "", 204  # 204 No Content


@app.route("/.well-known/appspecific/com.chrome.devtools.json")
def chrome_devtools():
    """Chrome DevTools lo solicita al inspeccionar — respuesta vacía sin error."""
    return "", 204  # 204 No Content


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n" + "=" * 58)
    print("  🚀  flask_jira_refactored.py — Power Automate Ready")
    print("=" * 58)
    print()
    print("  MÉTODOS HTTP (justificación resumida):")
    print("  ┌─────────────────────────────────────────────────┐")
    print("  │ GET  /process-data      → solo lectura Jira+BD  │")
    print("  │ POST /guardar-historico → escribe en PostgreSQL  │")
    print("  │ GET  /download-excel    → descarga archivo       │")
    print("  │ GET  /health            → monitoreo/warm-up      │")
    print("  └─────────────────────────────────────────────────┘")
    print()
    print("  RESPUESTA JSON (todos los endpoints data):")
    print('  { "status", "timestamp", "message", "meta", "data" }')
    print()
    print("  Power Automate → Parse JSON schema desde /process-data")
    print("  @body('HTTP')?['data']?['tickets']")
    print("  @body('HTTP')?['data']?['resumen_mensual']")
    print("  @body('HTTP')?['data']?['evolutivo']")
    print("  @body('HTTP')?['data']?['html_completo']")
    print()
    print("  Servidor: http://localhost:5000")
    print("=" * 58 + "\n")

    app.run(debug=True, host="0.0.0.0", port=5000)