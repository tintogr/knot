"""Quién puede hablarle a Knot y cómo se reconoce un mismo número en sus dos formatos."""
import state


def test_mismo_numero_en_formato_entrante_y_saliente():
    # WhatsApp entrega 549 + característica + número; para escribirle se usa 54 + car. + 15 + número.
    assert state.mismo_telefono("5492991234567", "54299151234567")
    assert state.mismo_telefono("54299151234567", "5492991234567")


def test_caracteristicas_de_2_3_y_4_digitos():
    assert state.mismo_telefono("5491112345678", "54111512345678")      # Buenos Aires (11)
    assert state.mismo_telefono("5492991234567", "54299151234567")      # Neuquén (299)
    assert state.mismo_telefono("5492944123456", "54294415123456")      # Bariloche (2944)


def test_numeros_iguales_y_con_simbolos():
    assert state.mismo_telefono("+54 9 299 123-4567", "5492991234567")
    assert state.mismo_telefono("54299151234567", "54299151234567")


def test_numeros_distintos_no_coinciden():
    assert not state.mismo_telefono("5492991234568", "54299151234567")   # último dígito
    assert not state.mismo_telefono("5492981234567", "54299151234567")   # otra característica
    assert not state.mismo_telefono("", "54299151234567")
    assert not state.mismo_telefono("5492991234567", "")
    assert not state.mismo_telefono("14155550100", "54299151234567")     # otro país


def test_preguntas_pendientes_no_dependen_del_formato_del_numero():
    ps = state._PorTelefono()
    ps["5492991234567"] = {"type": "snooze"}
    assert "542991234567" in ps
    assert ps.get("542991234567") == {"type": "snooze"}
    assert ps["5492991234567"]["type"] == "snooze"
    del ps["542991234567"]
    assert "5492991234567" not in ps
    assert ps.pop("5492991234567", None) is None


def test_historial_no_duplica_y_recorta():
    tel = "5490000000001"
    state.chat_history.pop(state._clave_historial(tel), None)
    state.add_to_history(tel, "user", "hola")
    state.add_to_history(tel, "user", "hola")            # repetido consecutivo: se ignora
    assert len(state.get_history(tel)) == 1
    for i in range(state.MAX_HISTORY + 10):
        state.add_to_history(tel, "assistant", f"m{i}")
    assert len(state.get_history(tel)) == state.MAX_HISTORY


def test_historial_para_api_empieza_con_el_usuario():
    tel = "5490000000002"
    state.chat_history.pop(state._clave_historial(tel), None)
    state.add_to_history(tel, "assistant", "aviso que mandó Knot solo")
    assert state.historial_para_api(tel)[0]["role"] == "user"


def test_mapa_de_citas_no_se_vacia_entero():
    state.recent_message_texts.clear()
    for i in range(state._RECENT_MSG_CAP + 5):
        state.record_message_text(f"id{i}", "texto")
    ultimo = f"id{state._RECENT_MSG_CAP + 4}"
    assert ultimo in state.recent_message_texts          # lo reciente sigue estando
    assert len(state.recent_message_texts) > 100
