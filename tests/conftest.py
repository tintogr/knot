"""Configuración común de las pruebas.

Las pruebas NO tocan nada real: no hablan con WhatsApp, Notion, Google ni Anthropic.
Las credenciales de abajo son falsas y solo existen para que el código se pueda importar.
"""
import os
import sys

os.environ.setdefault("NOTION_TOKEN", "test")
os.environ.setdefault("NOTION_DATABASE_ID", "0" * 32)
os.environ.setdefault("WHATSAPP_TOKEN", "test")
os.environ.setdefault("WHATSAPP_PHONE_ID", "1")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")
os.environ["MY_WA_NUMBER"] = "54299151234567"   # número inventado
os.environ.pop("ALLOWED_PHONES", None)
os.environ.pop("WHATSAPP_APP_SECRET", None)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
