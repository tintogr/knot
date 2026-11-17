"""Si Martin no contesta dos veces si un pago corresponde a una factura, Knot lo anota
igual pero con "(sin confirmar)" en el nombre (decisión de Martin, 17/11/2026), y el
mensaje dice solo lo que de verdad quedó hecho."""
import asyncio

import pytest

import summaries
from tests.test_finanzas import _Datos, _mov


class _DatosConArchivo(_Datos):
    def __init__(self, movs):
        super().__init__(movs)
        self.archivados = []

    async def archive_expense(self, entry_id):
        self.archivados.append(entry_id)
        return True


@pytest.fixture
def datos(monkeypatch):
    d = _DatosConArchivo([
        _mov("f1", "Factura Camuzzi — Noviembre 2026", 30000, estado="Impaga"),
        _mov("g1", "Camuzzi", 31000),
    ])
    monkeypatch.setattr(summaries, "_ds", d)
    return d


def test_una_factura_y_monto_parecido_queda_pagada_sin_confirmar(datos):
    conf = {"situation": "diff_moderate", "provider": "Camuzzi", "finance_page_ids": ["f1"],
            "gasto_page_id": "g1", "paid_amount": 31000}
    msg = asyncio.run(summaries._anotar_sin_confirmar(conf))
    f = datos.movs["f1"]
    assert f.estado == "Pagada" and f.value_ars == 31000
    assert f.name.endswith("(sin confirmar)")
    assert "sin que Martin lo confirmara" in f.notes
    assert datos.archivados == ["g1"]
    assert "sin confirmar" in msg


def test_pago_muy_distinto_no_toca_la_factura(datos):
    conf = {"situation": "diff_large", "provider": "Camuzzi", "finance_page_ids": ["f1"],
            "gasto_page_id": "g1", "paid_amount": 31000}
    msg = asyncio.run(summaries._anotar_sin_confirmar(conf))
    assert datos.movs["f1"].estado == "Impaga"
    assert datos.movs["g1"].name == "Camuzzi (sin confirmar)"
    assert "Camuzzi" in datos.movs["g1"].notes
    assert datos.archivados == []
    assert "sigue impaga" in msg


def test_si_notion_falla_no_dice_que_lo_anoto(datos):
    datos.marcar_ok = False
    conf = {"situation": "diff_moderate", "provider": "Camuzzi", "finance_page_ids": ["f1"],
            "gasto_page_id": None, "paid_amount": 31000}
    msg = asyncio.run(summaries._anotar_sin_confirmar(conf))
    assert datos.movs["f1"].estado == "Impaga"
    assert "no pude" in msg.lower()
