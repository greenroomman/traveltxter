# --- PATCHED RENDER ENDPOINT FIX ONLY ---
# This version forces POST to /render to avoid 405 errors

def render_image(render_url: str, payload_text: str) -> str:
    """
    FIXED:
    - Ensures POST goes to /render
    - Avoids 405 Method Not Allowed from PythonAnywhere
    """

    base = render_url.rstrip("/")

    # 🔒 HARD RULE: renderer only accepts POST /render
    if not base.endswith("/render"):
        base = base + "/render"

    r = requests.post(
        base,
        json={"text": payload_text},
        timeout=90
    )

    if r.status_code >= 400:
        raise RuntimeError(f"Renderer error {r.status_code}: {r.text[:400]}")

    j = r.json()
    graphic_url = (j.get("graphic_url") or j.get("url") or "").strip()

    if not graphic_url:
        raise RuntimeError(f"Renderer response missing graphic_url: {j}")

    return graphic_url
