# Código desarrollado por Juan Rodriguez Lopez & Luis Segura Peña

import asyncio
from pathlib import Path
from datetime import datetime
import re
import time
 
import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
 
# Hay que ejecutar log-sim.py primero para que se cree el fichero 'cu-lan-ho.log'
# Si no existe el fichero el programa se queda esperando, no peta
version = "2.11"
data_file = "cu-lan-ho.log"

parquet_period_s  = 30    # cada 30s guardo el parquet por si acaso
parquet_file      = "phase2_agg.parquet"
max_seconds_shown = 10    # máximo de segundos visibles en el eje X a la vez
 
# Si no llega nada en 1.2s asumimos que el segundo ya ha terminado.
# Con 1s justo a veces fallaba porque el log tiene pequeños huecos,
# así que le puse un poco más de margen y solucionado
WATCHDOG_TIMEOUT_S = 1.2

# Cola compartida de asyncio 
# Esta cola es el canal entre el productor (lee el log) y el consumidor (procesa los datos)
# El productor mete cosas, el consumidor las saca. Así no se pisan entre ellos.
queue = asyncio.Queue()
 
# DataFrames compartidos (básicamente donde guardo todo) 
# Aquí van los eventos de tráfico en crudo según llegan del log, sin agregar nada todavía
events_df = pl.DataFrame(
    schema={
        "timestamp": pl.Datetime,      # timestamp completo con microsegundos
        "timestamp_sec": pl.Datetime,  # el mismo pero redondeado al segundo (para agrupar)
        "ue": pl.Int64,                # identificador del equipo de usuario
        "bytes_dl": pl.Int64,          # bytes que han bajado en ese paquete
    }
)
 
# Una vez que cerramos un segundo, el tráfico agregado va aquí.
# O sea, aquí tenemos la suma de bytes por UE y por segundo
agg_df = pl.DataFrame(
    schema={
        "timestamp_sec": pl.Datetime,
        "ue": pl.Int64,
        "bytes_dl": pl.Int64,
    }
)
 
# Estado del segundo actual que todavía no hemos cerrado 
# El segundo de log que estamos acumulando ahora mismo
current_log_second    = None
 
# Buffer con los eventos de ese segundo. Cuando llega el siguiente segundo
# o el watchdog se activa, vaciamos esto y lo metemos en agg_df
current_second_buffer = []
 
# Guardo cuándo llegó el último evento según el reloj real de la máquina.
# No uso el timestamp del log porque el log puede ir más lento o más rápido que el tiempo real.
# El watchdog compara esto con time.monotonic() para saber si llevamos mucho sin recibir nada.
last_event_wall_time  = None

# Expresiones regulares para parsear el log 
# Cojo el timestamp ISO del principio de cada línea
timestamp_re = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)"
)
 
# Estos son para las líneas de tráfico SDAP
ue_re      = re.compile(r"\bue=(?P<ue>\d+)\b")
pdu_len_re = re.compile(r"\bpdu_len=(?P<pdu_len>\d+)\b")
 
# Parser de líneas de tráfico SDAP
def parse_sdap_line(line):
    # Solo me interesan líneas SDAP de bajada con UE y longitud de PDU.
    # Hago estos checks rápidos primero para no lanzar las regex en cada línea del log
    if "[SDAP"    not in line: return None
    if "DL:"      not in line: return None
    if "ue="      not in line: return None
    if "pdu_len=" not in line: return None
 
    # Si pasa los checks, ya sí busco con regex
    m_ts  = timestamp_re.search(line)
    m_ue  = ue_re.search(line)
    m_len = pdu_len_re.search(line)
 
    # Por si alguna regex no matchea (línea rara o mal formada)
    if not m_ts or not m_ue or not m_len:
        return None
 
    ts      = datetime.fromisoformat(m_ts.group("ts"))
    ue      = int(m_ue.group("ue"))
    pdu_len = int(m_len.group("pdu_len"))
 
    # Devuelvo un dict con type="traffic" para que el consumidor sepa qué tipo de evento es
    return {
        "type":          "traffic",
        "timestamp":     ts,
        "timestamp_sec": ts.replace(microsecond=0),  # quito los microsegundos para agrupar por segundo
        "ue":            ue,
        "bytes_dl":      pdu_len,
    }
 
# Cierra el segundo que tenemos abierto y lo mete en agg_df 
def finalize_current_second():
    global agg_df, current_second_buffer, current_log_second
 
    # Si no hay nada abierto no hago nada
    if current_log_second is None or len(current_second_buffer) == 0:
        return
 
    # Convierto el buffer a DataFrame para poder hacer el group_by fácilmente
    second_df  = pl.DataFrame(current_second_buffer)
 
    # Sumo los bytes de todos los paquetes del mismo UE en ese segundo
    second_agg = (
        second_df
        .group_by(["timestamp_sec", "ue"])
        .agg(pl.col("bytes_dl").sum().alias("bytes_dl"))
        .sort(["timestamp_sec", "ue"])
    )
 
    # Lo añado al DataFrame global de datos agregados
    agg_df = pl.concat([agg_df, second_agg], how="vertical_relaxed")
    print(f"Closed log second {current_log_second} → {second_agg.height} rows added to agg_df")
 
# Productor: lee el fichero de log como si fuera un tail -f 
async def tail_file_producer(path: str, data_queue: asyncio.Queue):
    file = Path(path)
    print("Waiting for:", data_file)
 
    # Espera hasta que el fichero exista, por si el simulador todavía no ha arrancado
    while not file.exists():
        await asyncio.sleep(0.5)
 
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        print("Opened:", data_file)
        f.seek(0, 2)  # me pongo al final del fichero para no reprocesar lo que ya había
 
        while True:
            line = f.readline()
 
            # Si no hay línea nueva esperamos un poco y volvemos a intentarlo
            if not line:
                await asyncio.sleep(0.2)
                continue
 
            # Intento parsear como evento de tráfico
            traffic_event = parse_sdap_line(line)
            if traffic_event is not None:
                print("Traffic event added to queue:", traffic_event)
                await data_queue.put(traffic_event)
 
            # Si no matchea el parser simplemente ignoramos la línea
 
# Consumidor: procesa lo que el productor mete en la cola 
async def consumer(data_queue: asyncio.Queue):
    global events_df, agg_df
    global current_log_second, current_second_buffer, last_event_wall_time
 
    last_parquet_save = asyncio.get_event_loop().time()
 
    while True:
        # Esto se bloquea hasta que haya algo en la cola, sin gastar CPU
        item = await data_queue.get()
 
        if item["type"] == "traffic":
 
            # Meto el evento en el DataFrame de eventos en crudo
            new_row = pl.DataFrame([{
                "timestamp":     item["timestamp"],
                "timestamp_sec": item["timestamp_sec"],
                "ue":            item["ue"],
                "bytes_dl":      item["bytes_dl"],
            }])
 
            events_df = pl.concat([events_df, new_row], how="vertical_relaxed")
 
            item_second          = item["timestamp_sec"]
            last_event_wall_time = time.monotonic()  # apunto cuándo llegó en tiempo real
 
            if current_log_second is None:
                # Primer evento que recibimos, abrimos el primer segundo
                current_log_second    = item_second
                current_second_buffer = [new_row.to_dicts()[0]]
 
            elif item_second == current_log_second:
                # Mismo segundo que el anterior, sigo acumulando en el buffer
                current_second_buffer.append(new_row.to_dicts()[0])
 
            elif item_second > current_log_second:
                # Llegó un evento de un segundo nuevo, así que cierro el anterior
                # y abro uno nuevo con este evento
                finalize_current_second()
                current_log_second    = item_second
                current_second_buffer = [new_row.to_dicts()[0]]
 
            else:
                # El timestamp es menor que el segundo actual, o sea llegó tarde.
                # No lo proceso para no liar el orden de los datos
                print("Out-of-order traffic event ignored:", item)
 
            print("Traffic event consumed:", item)
 
        # Guardo el parquet cada X segundos por si se cae el programa y no pierdo todo
        now = asyncio.get_event_loop().time()
        if now - last_parquet_save >= parquet_period_s:
            if agg_df.height > 0:
                agg_df.write_parquet(parquet_file)
                print("Parquet file saved:", parquet_file)
            last_parquet_save = now
 
        # Le digo a la cola que ya terminé con este item
        data_queue.task_done()
 
# Watchdog: cierra el segundo abierto si no llega nada en un rato
# El problema era que el consumidor solo cierra un segundo cuando
# llega el siguiente evento. Si el log se queda parado un momento
# (o se acaba), el último segundo nunca se cerraba y la barra
# no aparecía en la gráfica.
# Este watchdog comprueba cada 0.2s si llevamos más de WATCHDOG_TIMEOUT_S
# sin recibir nada, y si es así cierra el segundo a la fuerza.
 
async def watchdog_task():
    global current_log_second, current_second_buffer, last_event_wall_time
 
    while True:
        await asyncio.sleep(0.2)  # compruebo cada 200ms, no hace falta más frecuencia
 
        if (
            current_log_second is not None       # hay un segundo abierto
            and len(current_second_buffer) > 0   # con datos dentro
            and last_event_wall_time is not None  # y ya hemos recibido algún evento antes
            and (time.monotonic() - last_event_wall_time) >= WATCHDOG_TIMEOUT_S  # y lleva mucho sin llegar nada
        ):
            print(f"⏱ Watchdog closing idle log second {current_log_second}")
            finalize_current_second()
            # Reseteo el estado para que el próximo evento empiece limpio
            current_second_buffer = []
            current_log_second    = None
 
# App de Dash 
app = Dash(__name__)
 
# Layout de la app: título, texto de estado y la gráfica
# El Interval hace que Dash llame al callback cada 1000ms para actualizar todo
app.layout = html.Div([
    html.H3("Tráfico DL por UE"),
    html.Div(id="status-text"),
 
    dcc.Graph(id="live-graph"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),  # refresco cada segundo
])
 
 
# Este callback se ejecuta cada vez que el Interval hace tick.
# Actualiza la gráfica y el texto de estado.
@app.callback(
    Output("live-graph",  "figure"),
    Output("status-text", "children"),
    Input("interval",     "n_intervals")
)
def update_data(n):
    # Colores para distinguir las UEs en la gráfica, se repiten si hay más de 10
    colors = [
        "#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
    ]
 
    # Si todavía no hay datos agregados, muestro una gráfica vacía con mensaje de espera
    if agg_df.height == 0:
        fig = go.Figure()
        fig.update_layout(
            title="esperando datos",
            xaxis_title="tiempo",
            yaxis_title="bytes/s",
        )
        status_text = f"aún sin segundos cerrados — filas leídas: {events_df.height}"
        return fig, status_text
 
    # Cojo los últimos N segundos únicos del log y me quedo solo con esos.
    # Así el eje X siempre muestra una ventana fija de max_seconds_shown segundos,
    # y cuando llega uno nuevo el más viejo desaparece por la izquierda
    last_seconds = (
        agg_df
        .select("timestamp_sec")
        .unique()
        .sort("timestamp_sec")
        .tail(max_seconds_shown)
        .to_series()
        .to_list()
    )
    filtered_df = agg_df.filter(pl.col("timestamp_sec").is_in(last_seconds))
 
    # Saco la lista de UEs únicas a partir de TODO el histórico (agg_df), no solo de
    # la ventana visible. Si la sacase de filtered_df, un UE que deje de transmitir
    # durante más de max_seconds_shown segundos desaparecería de ue_list y su subplot
    # se iría entero (en vez de quedarse vacío como debería).
    ue_list = (
        agg_df
        .select("ue")
        .unique()
        .sort("ue")
        .to_series()
        .to_list()
    )
 
    # Creo un subplot por UE, todos comparten el eje X para que sea fácil comparar
    fig = make_subplots(
        rows=len(ue_list),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=[f"UE {ue}" for ue in ue_list]
    )
 
    for i, ue in enumerate(ue_list, start=1):
        # Siempre pongo el título del eje Y del subplot, aunque este UE no tenga
        # datos en la ventana actual (si no, el subplot se quedaría sin etiqueta).
        fig.update_yaxes(title_text="Bytes", row=i, col=1)
 
        # Con shared_xaxes=True Plotly oculta los ticks de tiempo en los subplots
        # de arriba y solo los deja en el de abajo. Lo fuerzo para que salgan
        # en todos, porque si no no sabes a qué segundo corresponde cada barra
        # en los de arriba.
        fig.update_xaxes(showticklabels=True, row=i, col=1)
 
        # Cojo los datos de este UE en la ventana visible
        ue_df_raw = (
            filtered_df
            .filter(pl.col("ue") == ue)
            .sort("timestamp_sec")
        )
 
        # Aunque el UE tenga datos, puede no haber transmitido en TODOS los
        # segundos de la ventana. Para que la barra salga a 0 en esos huecos
        # (en vez de no pintarse nada), hago un left join contra la ventana
        # completa y relleno los nulls con 0.
        #
        # Esto también arregla el caso en el que el UE no ha transmitido nada
        # en toda la ventana: antes el subplot se quedaba totalmente en blanco
        # (solo con el título "UE N" flotando), y ahora sale una línea plana a 0.
        window_df = pl.DataFrame({"timestamp_sec": last_seconds})
        ue_df = (
            window_df
            .join(ue_df_raw.select(["timestamp_sec", "bytes_dl"]),
                  on="timestamp_sec", how="left")
            .with_columns(pl.col("bytes_dl").fill_null(0))
            .sort("timestamp_sec")
        )
 
        # Cada UE tiene su color, si hay más de 10 UEs se repiten los colores
        color = colors[(i - 1) % len(colors)]
 
        fig.add_trace(
            go.Bar(
                x=ue_df["timestamp_sec"].to_list(),
                y=ue_df["bytes_dl"].to_list(),
                name=f"UE {ue}",
                marker_color=color,
                hovertemplate=(
                    "Time: %{x}<br>"
                    "Bytes DL: %{y}<br>"
                    f"UE: {ue}"
                    "<extra></extra>"  # esto oculta el nombre de la traza en el tooltip
                ),
                showlegend=False,  # no hace falta leyenda porque ya están los títulos de subplot
            ),
            row=i,
            col=1,
        )
 
    # Título del eje X solo en el subplot de abajo, pero los ticks se ven en todos
    fig.update_xaxes(title_text="tiempo", row=len(ue_list), col=1)
 
    last_ts = agg_df["timestamp_sec"].max()
 
    fig.update_layout(
        title=f"últimos {max_seconds_shown}s (hasta {last_ts})",
        height=max(400, 280 * len(ue_list)),  # altura dinámica según cuántas UEs haya
        barmode="group",
    )
 
    # Texto de estado que aparece encima de la gráfica, útil para debugear
    status_text = (
        f"filas raw: {events_df.height} · "
        f"agregadas: {agg_df.height} · "
        f"segundo abierto: {current_log_second} · "
        f"parquet: {parquet_file}"
    )
 
    return fig, status_text
 
# Main: arranca todo 
async def main():
    print("Live data Producer-Consumer + Dash :: " + version)
 
    # Arranco las tres tareas asíncronas en paralelo
    asyncio.create_task(tail_file_producer(data_file, queue))  # lee el log
    asyncio.create_task(consumer(queue))                        # procesa los eventos
    asyncio.create_task(watchdog_task())                        # cierra segundos huérfanos
 
    # Corro Dash en un hilo aparte para que no bloquee el event loop de asyncio
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=8059,
        debug=False,
    )
 
if __name__ == '__main__':
    asyncio.run(main())