"""
controller/app/core/responses.py
PrettyJSONResponse — indent=2 for human-readable curl / browser output.
"""
import json
from typing import Any

from fastapi.responses import JSONResponse


class PrettyJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")
