"""Capa de Notion: reintentos seguros y guardado de la configuración sin perder datos."""
import asyncio
import json

import httpx

import notion_datastore as nd
from notion_datastore import NotionDataStore, UserConfig, _ClienteConReintentos


def _store(handler):
    ds = NotionDataStore(token="t", db_ids={"config": "c" * 32, "finances": "f" * 32})
    ds._http = _ClienteConReintentos(transport=httpx.MockTransport(handler), timeout=5)
    return ds


async def _sin_espera(_):
    return None


def test_que_es_seguro_repetir():
    f = _ClienteConReintentos._es_repetible
    assert f("GET", "https://api.notion.com/v1/pages/abc")
    assert f("POST", "https://api.notion.com/v1/databases/abc/query")       # leer
    assert f("PATCH", "https://api.notion.com/v1/pages/abc")                # actualizar
    assert not f("POST", "https://api.notion.com/v1/pages")                 # crear
    assert not f("PATCH", "https://api.notion.com/v1/blocks/abc/children")  # agregar bloques


def test_una_lectura_se_reintenta_si_notion_falla(monkeypatch):
    monkeypatch.setattr(nd.asyncio, "sleep", _sin_espera)
    llamadas = []

    def handler(request):
        llamadas.append(request.url.path)
        if len(llamadas) < 3:
            return httpx.Response(502 if len(llamadas) == 1 else 429, json={})
        return httpx.Response(200, json={"results": [{"id": "p1"}], "has_more": False})

    ds = _store(handler)
    assert asyncio.run(ds._query_db("finances")) == [{"id": "p1"}]
    assert len(llamadas) == 3


def test_una_creacion_no_se_repite_ante_un_error_del_servidor(monkeypatch):
    """Repetir una creación podría duplicar el gasto: ante un 500 se falla una sola vez."""
    monkeypatch.setattr(nd.asyncio, "sleep", _sin_espera)
    llamadas = []

    def handler(request):
        llamadas.append(1)
        return httpx.Response(500, json={"message": "boom"})

    ds = _store(handler)
    try:
        asyncio.run(ds._create_page("finances", {"Name": {"title": []}}))
        assert False, "tenía que fallar"
    except nd.DataStoreError:
        pass
    assert len(llamadas) == 1


def test_una_creacion_si_se_reintenta_con_limite_de_velocidad(monkeypatch):
    """429 = Notion no procesó el pedido: reintentar es seguro."""
    monkeypatch.setattr(nd.asyncio, "sleep", _sin_espera)
    llamadas = []

    def handler(request):
        llamadas.append(1)
        return httpx.Response(429, json={}) if len(llamadas) == 1 else httpx.Response(200, json={"id": "nuevo"})

    ds = _store(handler)
    assert asyncio.run(ds._create_page("finances", {}))["id"] == "nuevo"
    assert len(llamadas) == 2


def test_titulo_con_formato_no_se_corta():
    props = {"Name": {"title": [{"plain_text": "Luz - "}, {"plain_text": "CALF"}]}}
    assert nd._get_title(props) == "Luz - CALF"
    assert nd._get_title({"Name": {"title": []}}) == ""
    assert nd._get_title({}) == ""


def test_configuracion_larga_se_guarda_y_se_relee_entera(monkeypatch):
    """Antes un JSON de más de 2000 caracteres se cortaba por la mitad (y al releerlo
    se perdía todo) o hacía que Notion rechazara el guardado completo."""
    guardado = {}

    def handler(request):
        if request.method == "PATCH":
            for campo, valor in json.loads(request.content)["properties"].items():
                for frag in valor.get("rich_text", []):
                    assert len(frag["text"]["content"]) <= 2000, "Notion rechaza fragmentos de más de 2000"
                guardado[campo] = valor
            return httpx.Response(200, json={"id": "pag"})
        # consulta: devolver lo guardado como lo devolvería Notion
        props = {}
        for campo, valor in guardado.items():
            if "rich_text" in valor:
                props[campo] = {"rich_text": [{"plain_text": f["text"]["content"]} for f in valor["rich_text"]]}
            else:
                props[campo] = valor
        return httpx.Response(200, json={"results": [{"id": "pag", "properties": props}], "has_more": False})

    ds = _store(handler)
    lugares = [{"name": f"Lugar número {i}", "lat": -38.95 - i / 1000, "lon": -68.06, "radius": 200} for i in range(80)]
    compras = {f"producto {i}": i for i in range(400)}
    estado = {"ultimo_resumen": "2026-11-14", "facturas_desde_ms": 1794000000000}
    cfg = UserConfig(phone="54299151234567", known_places=lugares, purchase_counts=compras,
                     knot_state=estado, personas={"Ana": {"relacion": "hermana"}})
    assert len(json.dumps(lugares)) > 2000 and len(json.dumps(compras)) > 2000

    assert asyncio.run(ds.save_config("pag", cfg)) is True
    releido, page_id = asyncio.run(ds.load_config())
    assert page_id == "pag"
    assert releido.known_places == lugares
    assert releido.purchase_counts == compras
    assert releido.knot_state == estado
    assert releido.personas == {"Ana": {"relacion": "hermana"}}


def test_si_una_columna_falla_el_estado_interno_se_guarda_igual(monkeypatch):
    """De 'Estado Knot' depende no repetir el saludo ni releer facturas: tiene que
    guardarse aunque Notion rechace otra columna."""
    monkeypatch.setattr(nd.asyncio, "sleep", _sin_espera)
    guardadas = []

    def handler(request):
        campos = list(json.loads(request.content)["properties"])
        if "Known Places" in campos:
            return httpx.Response(400, json={"message": "Known Places is not a property that exists"})
        guardadas.extend(campos)
        return httpx.Response(200, json={"id": "pag"})

    ds = _store(handler)
    cfg = UserConfig(phone="x", knot_state={"ultimo_resumen": "2026-11-14"}, greeting_name="Buen día")
    assert asyncio.run(ds.save_config("pag", cfg)) is False        # avisa que no quedó completo
    assert "Estado Knot" in guardadas
    assert "Greeting Name" in guardadas                            # el resto se salvó de a una
    assert "Known Places" not in guardadas


def test_servicio_por_alias_y_vencimiento_en_notas():
    ds = _store(lambda r: httpx.Response(200, json={}))
    ds._services = [
        {"servicio": "Expensas", "empresa": "Consorcio", "aliases": ["interfast"], "frecuencia": "Mensual"},
        {"servicio": "Internet", "empresa": "Calfibra", "aliases": ["calfibra"], "frecuencia": "Mensual"},
        {"servicio": "Luz", "empresa": "CALF", "aliases": ["calf"], "frecuencia": "Mensual"},
    ]
    assert ds.service_of("pagué interfast 98.000")["servicio"] == "Expensas"
    assert ds.service_of("Factura CALF Energía")["servicio"] == "Luz"
    assert ds.service_of("Calfibra noviembre")["servicio"] == "Internet"     # "calf" no es "calfibra"
    assert ds.service_of("verdulería 3500") is None
    assert NotionDataStore.vencimiento_de("Factura 123. Vence 16/10/2026").isoformat() == "2026-10-16"
    assert NotionDataStore.vencimiento_de("sin fecha") is None


def test_sumar_meses_respeta_fin_de_mes():
    from datetime import date
    assert NotionDataStore._sumar_meses(date(2026, 11, 30), 3) == date(2027, 2, 28)
    assert NotionDataStore._sumar_meses(date(2026, 1, 31), 1) == date(2026, 2, 28)
    assert NotionDataStore._sumar_meses(date(2026, 10, 15), 6) == date(2027, 4, 15)


def test_impagas_en_modo_estricto_avisa_si_notion_falla(monkeypatch):
    """El saludo usa el modo estricto para no decir "Facturas al día" sin haber mirado."""
    monkeypatch.setattr(nd.asyncio, "sleep", _sin_espera)
    ds = _store(lambda r: httpx.Response(503, json={"message": "caído"}))
    assert asyncio.run(ds.get_impaga_facturas()) == []            # modo normal: lista vacía
    try:
        asyncio.run(ds.get_impaga_facturas(estricto=True))
        assert False, "tenía que avisar del fallo"
    except nd.DataStoreError:
        pass
