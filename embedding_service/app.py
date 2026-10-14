#!/usr/bin/env python3
"""Small OpenAI-compatible BGE-M3 embedding service for a dedicated worker."""

from __future__ import annotations

import os
import threading
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from FlagEmbedding import BGEM3FlagModel


MODEL_PATH = os.environ.get(
    "MODEL_PATH", "/models/models/BAAI--bge-m3/snapshots/master"
)
MODEL_NAME = os.environ.get("MODEL_NAME", "BAAI/bge-m3")
MAX_BATCH = int(os.environ.get("MAX_BATCH", "64"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "16"))
MAX_LENGTH = int(os.environ.get("MAX_LENGTH", "8192"))
USE_FP16 = os.environ.get("USE_FP16", "1") not in {"0", "false", "False"}

app = FastAPI(title="paper-workspace-bge-m3", version="1.0")
model: BGEM3FlagModel | None = None
model_lock = threading.Lock()
started_at = time.time()


class EmbeddingRequest(BaseModel):
    input: str | list[str]
    model: str = MODEL_NAME
    encoding_format: str | None = Field(default="float")


@app.on_event("startup")
def load_model() -> None:
    global model
    if not os.path.isdir(MODEL_PATH):
        raise RuntimeError(f"model path not found: {MODEL_PATH}")
    model = BGEM3FlagModel(MODEL_PATH, use_fp16=USE_FP16)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok" if model is not None else "loading",
        "model": MODEL_NAME,
        "model_loaded": model is not None,
        "dimensions": 1024,
        "max_length": MAX_LENGTH,
        "uptime_seconds": round(time.time() - started_at, 1),
    }


@app.get("/v1/models")
def models() -> dict[str, Any]:
    return {"object": "list", "data": [{"id": MODEL_NAME, "object": "model"}]}


@app.post("/v1/embeddings")
def embeddings(request: EmbeddingRequest) -> dict[str, Any]:
    if model is None:
        raise HTTPException(status_code=503, detail="model is still loading")
    texts = [request.input] if isinstance(request.input, str) else request.input
    if not texts or len(texts) > MAX_BATCH:
        raise HTTPException(status_code=400, detail=f"input count must be 1..{MAX_BATCH}")
    if any(not isinstance(text, str) or not text.strip() for text in texts):
        raise HTTPException(status_code=400, detail="every input must be a non-empty string")
    with model_lock:
        vectors = model.encode(
            texts, batch_size=min(BATCH_SIZE, len(texts)), max_length=MAX_LENGTH
        )["dense_vecs"]
    return {
        "object": "list",
        "model": MODEL_NAME,
        "data": [
            {"object": "embedding", "index": index, "embedding": vector.tolist()}
            for index, vector in enumerate(vectors)
        ],
        "usage": {"prompt_tokens": 0, "total_tokens": 0},
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8091")))
