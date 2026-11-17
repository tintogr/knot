"""Regla del proyecto: ningún `except Exception: pass`. Un error tragado sin rastro
es como se perdían facturas sin que nadie se enterara."""
import glob
import os
import re

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_no_hay_errores_tragados_en_silencio():
    culpables = []
    for f in glob.glob(os.path.join(RAIZ, "*.py")) + glob.glob(os.path.join(RAIZ, "banco", "*.py")):
        lineas = open(f, encoding="utf-8").read().split("\n")
        for i, l in enumerate(lineas[:-1]):
            if re.match(r"\s*except\s*(Exception|BaseException)?\s*:\s*$", l) and lineas[i + 1].strip() == "pass":
                culpables.append(f"{os.path.basename(f)}:{i + 1}")
    assert not culpables, "except que se traga el error sin dejar rastro: " + ", ".join(culpables)
