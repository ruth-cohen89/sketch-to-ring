"""ImageProvider layer: swap providers without touching the queues."""
import base64
import hashlib
import io
import os
import time

import requests
from PIL import Image, ImageDraw


class ProviderError(Exception):
    """kind: RETRYABLE (request certainly not executed), UNKNOWN (may have executed),
    QUOTA (daily free allocation used), FATAL (bad request / content blocked / auth)."""

    def __init__(self, kind: str, msg: str):
        super().__init__(msg)
        self.kind = kind


class ImageProvider:
    name = "base"
    # Details of the last call for the debug trace (no secrets, no image bytes).
    last_meta: dict = {}

    def generate(self, prompt: str, ref_png: bytes, width: int, height: int) -> bytes:
        raise NotImplementedError


class FakeImageProvider(ImageProvider):
    """Visible MOCK: draws a ring over the reference image and stamps 'MOCK'."""

    name = "fake"

    def generate(self, prompt, ref_png, width, height):
        self.last_meta = {"mock": True, "endpoint": "none (FakeImageProvider draws locally)", "fieldsSent": {"prompt": prompt, "width": width, "height": height}}
        time.sleep(1.0)
        h = int(hashlib.sha256((prompt + str(time.time_ns())).encode()).hexdigest()[:6], 16)
        img = Image.open(io.BytesIO(ref_png)).convert("RGB").resize((width, height))
        img = Image.blend(img, Image.new("RGB", img.size, "white"), 0.6)
        d = ImageDraw.Draw(img)
        cx, cy, r = width // 2, height // 2 + 60, width // 4
        gold = (200 + h % 50, 160 + h % 40, 40 + h % 60)
        d.ellipse([cx - r, cy - r // 2, cx + r, cy + r // 2], outline=gold, width=28)
        gem = (80 + h % 150, 120 + (h >> 4) % 120, 220)
        d.polygon([(cx, cy - r // 2 - 120), (cx - 60, cy - r // 2 - 40), (cx, cy - r // 2 + 10), (cx + 60, cy - r // 2 - 40)], fill=gem)
        d.rectangle([0, 0, width, 70], fill=(220, 30, 30))
        d.text((20, 20), "MOCK IMAGE - FakeImageProvider (no real FLUX call)", fill="white", font_size=32)
        out = io.BytesIO()
        img.save(out, "PNG")
        return out.getvalue()


class CloudflareFluxProvider(ImageProvider):
    """Cloudflare Workers AI FLUX.2 [klein] via REST, multipart body.
    Only verified fields are sent: prompt, input_image_0, width, height."""

    name = "cloudflare"

    def __init__(self):
        self.account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        self.token = os.environ["CLOUDFLARE_API_TOKEN"]
        self.model = os.getenv("CF_MODEL", "@cf/black-forest-labs/flux-2-klein-4b")

    def generate(self, prompt, ref_png, width, height):
        url = f"https://api.cloudflare.com/client/v4/accounts/{self.account}/ai/run/{self.model}"
        self.last_meta = {
            "mock": False,
            "endpoint": f"POST https://api.cloudflare.com/client/v4/accounts/<account>/ai/run/{self.model}",
            "encoding": "multipart/form-data",
            "fieldsSent": {"prompt": prompt, "width": str(width), "height": str(height),
                           "input_image_0": f"<PNG file, {len(ref_png)} bytes>"},
            "timeoutSec": {"connect": 10, "read": 180},
        }
        try:
            r = requests.post(
                url,
                headers={"Authorization": f"Bearer {self.token}"},
                files={"input_image_0": ("ref.png", ref_png, "image/png")},
                data={"prompt": prompt, "width": str(width), "height": str(height)},
                timeout=(10, 180),
            )
        except requests.exceptions.ConnectTimeout as e:
            raise ProviderError("RETRYABLE", f"connect timeout: {e}")
        except requests.exceptions.ReadTimeout:
            raise ProviderError("UNKNOWN", "provider read timeout - request may have been executed")
        except requests.exceptions.ConnectionError as e:
            # Only connection establishment failures are known-safe to retry.
            txt = repr(e)
            if "NewConnectionError" in txt or "NameResolutionError" in txt or "Failed to resolve" in txt:
                raise ProviderError("RETRYABLE", f"connection failed before send: {e}")
            raise ProviderError("UNKNOWN", f"connection dropped after send: {e}")

        body = r.text[:500]
        self.last_meta.update(httpStatus=r.status_code, responseContentType=r.headers.get("content-type"),
                              responseBytes=len(r.content), cfRay=r.headers.get("cf-ray"))
        if r.status_code != 200:
            self.last_meta["responseBodyStart"] = body[:300]
        if r.status_code == 200:
            try:
                b64 = r.json()["result"]["image"]
                return base64.b64decode(b64)
            except Exception:
                raise ProviderError("UNKNOWN", f"unparseable success response: {body[:200]}")
        low = body.lower()
        if r.status_code == 429 and ("allocation" in low or "neuron" in low or "quota" in low or "daily" in low):
            raise ProviderError("QUOTA", f"free quota exhausted: {body[:200]}")
        if r.status_code == 429:
            raise ProviderError("RETRYABLE", f"rate limited: {body[:200]}")
        if r.status_code in (500, 503) and '"errors"' in body:
            raise ProviderError("RETRYABLE", f"provider error {r.status_code}: {body[:200]}")
        if r.status_code >= 500:
            raise ProviderError("UNKNOWN", f"ambiguous provider error {r.status_code}: {body[:200]}")
        raise ProviderError("FATAL", f"provider rejected request {r.status_code}: {body[:200]}")


def make_provider() -> ImageProvider:
    if os.getenv("PROVIDER", "fake") == "cloudflare" and os.getenv("CLOUDFLARE_API_TOKEN"):
        return CloudflareFluxProvider()
    return FakeImageProvider()
