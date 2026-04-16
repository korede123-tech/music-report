from __future__ import annotations

from pathlib import Path
from typing import Iterable
import difflib
import re
import xml.etree.ElementTree as ET
import zipfile

from pypdf import PdfReader
import requests


LIVE_MODEL_CANDIDATES = [
    "command-a-03-2025",
    "command-r7b-12-2024",
    "command-r-plus-08-2024",
    "command-r-08-2024",
    "c4ai-aya-expanse-32b",
]


CHAT_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "what",
    "when",
    "where",
    "which",
    "how",
    "from",
    "into",
    "about",
    "your",
    "their",
    "data",
    "report",
    "reports",
}


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
        chunks: list[str] = []
        for page in reader.pages:
            chunks.append(page.extract_text() or "")
        return "\n".join(chunks)
    except Exception:
        return ""


def extract_docx_text(docx_path: Path) -> str:
    try:
        with zipfile.ZipFile(docx_path) as archive:
            if "word/document.xml" not in archive.namelist():
                return ""
            xml_payload = archive.read("word/document.xml")

        root = ET.fromstring(xml_payload)
        namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        fragments = [
            node.text
            for node in root.findall(".//w:t", namespace)
            if node.text and node.text.strip()
        ]
        return "\n".join(fragments)
    except Exception:
        return ""


def _extract_plaintext_fallback(file_path: Path) -> str:
    try:
        raw = file_path.read_bytes()
    except Exception:
        return ""

    for encoding in ("utf-8", "latin-1"):
        try:
            decoded = raw.decode(encoding, errors="ignore")
            break
        except Exception:
            decoded = ""

    if not decoded:
        return ""

    lines = []
    for line in decoded.splitlines():
        cleaned = " ".join(line.split())
        if len(cleaned) >= 8:
            lines.append(cleaned)
    if not lines:
        return ""
    return "\n".join(lines[:400])


def extract_document_text(file_path: Path) -> tuple[str, list[str]]:
    suffix = file_path.suffix.lower()
    warnings: list[str] = []

    if suffix == ".pdf":
        text = extract_pdf_text(file_path)
    elif suffix == ".docx":
        text = extract_docx_text(file_path)
    elif suffix in {".doc", ".docs"}:
        text = _extract_plaintext_fallback(file_path)
        if not text:
            warnings.append(
                "Uploaded document saved, but automatic text extraction for .doc/.docs is limited. "
                "Use .docx or .pdf for best indexing results."
            )
    else:
        raise ValueError("Unsupported document format. Use .pdf, .doc, .docx, or .docs")

    if not text.strip():
        warnings.append("No readable text was extracted from this document.")
    return text, warnings


def _dedupe_docs(docs: Iterable[dict]) -> list[dict]:
    unique_docs: list[dict] = []
    seen_signatures: set[str] = set()

    for doc in docs:
        content = " ".join((doc.get("content") or "").split())
        source_path = (doc.get("source_path") or "").strip().lower()
        title = (doc.get("title") or "").strip().lower()

        if source_path:
            signature = f"source::{source_path}"
        else:
            signature = f"title::{title}::content::{content[:180]}"

        if signature in seen_signatures:
            continue

        seen_signatures.add(signature)
        cloned = dict(doc)
        cloned["content"] = content
        unique_docs.append(cloned)

    return unique_docs


def _query_terms(question: str) -> list[str]:
    tokens = [term.lower() for term in re.findall(r"[a-zA-Z0-9]+", question)]
    filtered = [term for term in tokens if len(term) > 2 and term not in CHAT_STOPWORDS]
    if filtered:
        return filtered
    return [term for term in tokens if len(term) > 2]


def _extract_relevant_preview(content: str, terms: list[str], max_chars: int = 1400) -> str:
    compact = " ".join(content.split())
    if not compact:
        return ""

    if not terms:
        return compact[:max_chars]

    lowered = compact.lower()
    compact_tokens = set(re.findall(r"[a-zA-Z0-9]+", lowered))
    expanded_terms: list[str] = []

    for term in terms:
        expanded_terms.append(term)
        if term in compact_tokens:
            continue
        close = difflib.get_close_matches(term, compact_tokens, n=1, cutoff=0.84)
        if close:
            expanded_terms.append(close[0])

    snippets: list[str] = []
    seen_windows: set[tuple[int, int]] = set()
    for term in expanded_terms[:8]:
        index = lowered.find(term)
        if index < 0:
            continue

        start = max(0, index - 180)
        end = min(len(compact), index + 340)
        key = (start // 50, end // 50)
        if key in seen_windows:
            continue
        seen_windows.add(key)

        snippet = compact[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(compact):
            snippet = snippet + "..."
        snippets.append(snippet)

        if len(" ".join(snippets)) >= max_chars:
            break

    if not snippets:
        return compact[:max_chars]

    merged = "\n".join(snippets)
    return merged[:max_chars]


def _build_context(
    docs: Iterable[dict],
    song_summaries: Iterable[dict],
    question: str,
    max_chars: int = 12_000,
) -> str:
    deduped_docs = _dedupe_docs(docs)
    question_terms = _query_terms(question)
    sections: list[str] = ["Indexed Song Summaries:"]

    for song in song_summaries:
        sections.append(
            f"- {song.get('title', 'Unknown')} | Release: {song.get('release_date', 'n/a')} | Spotify: {song.get('spotify_link', 'n/a')}"
        )

    sections.append("\nIndexed Report Content:")
    for doc in deduped_docs:
        preview = _extract_relevant_preview(str(doc.get("content") or ""), question_terms, max_chars=1400)
        sections.append(f"\nTitle: {doc.get('title', '')}\nSource: {doc.get('source_path', '')}\nContent: {preview}")

    context = "\n".join(sections)
    return context[:max_chars]


def _extract_cohere_text(payload: dict) -> str:
    message = payload.get("message", {})
    content = message.get("content", [])
    if isinstance(content, list):
        collected = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                collected.append(item.get("text", ""))
        if collected:
            return "\n".join(collected).strip()

    if "text" in payload and isinstance(payload["text"], str):
        return payload["text"].strip()
    return ""


def _fallback_answer(question: str, docs: Iterable[dict]) -> str:
    deduped_docs = _dedupe_docs(docs)
    snippets: list[str] = []
    for doc in deduped_docs[:2]:
        content = doc.get("content") or ""
        preview = _extract_relevant_preview(content, _query_terms(question), max_chars=320)
        snippets.append(f"[{doc.get('title', 'Document')}] {preview}")

    if not snippets:
        return (
            "No indexed report content is available yet. Upload at least one CSV or PDF report, "
            "then ask your question again."
        )

    return (
        "Cohere response was unavailable, so this answer is based on local indexed snippets. "
        f"Question: {question}\n\n" + "\n\n".join(snippets)
    )


class CohereChatbot:
    def __init__(self, api_key: str, model: str = "command-a-03-2025") -> None:
        self.api_key = api_key.strip()
        self.model = model.strip()

    @staticmethod
    def _response_error(response: requests.Response) -> str:
        try:
            payload = response.json()
            message = payload.get("message") or payload.get("error") or payload.get("detail")
            if isinstance(message, str) and message.strip():
                return message.strip()
        except Exception:
            pass
        return (response.text or "").strip()[:260] or f"HTTP {response.status_code}"

    @staticmethod
    def _looks_like_removed_model_error(error_message: str) -> bool:
        lowered = error_message.lower()
        return any(token in lowered for token in ("was removed", "deprecated", "retired"))

    def _model_candidates(self) -> list[str]:
        candidates: list[str] = []
        if self.model:
            candidates.append(self.model)
        candidates.extend(LIVE_MODEL_CANDIDATES)

        unique: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            key = candidate.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(candidate.strip())
        return unique

    @staticmethod
    def _extract_v1_text(payload: dict) -> str:
        if isinstance(payload.get("text"), str):
            return payload["text"].strip()
        generations = payload.get("generations")
        if isinstance(generations, list) and generations:
            first = generations[0]
            if isinstance(first, dict) and isinstance(first.get("text"), str):
                return first["text"].strip()
        return ""

    def _request_v2(
        self,
        prompt: str,
        endpoint: str,
        headers: dict[str, str],
        model: str,
    ) -> tuple[str, str | None]:
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}],
                }
            ],
            "temperature": 0.2,
        }
        response = requests.post(endpoint, json=payload, headers=headers, timeout=45)
        if response.ok:
            text = _extract_cohere_text(response.json())
            return text, None
        return "", self._response_error(response)

    def _request_v1(
        self,
        prompt: str,
        endpoint: str,
        headers: dict[str, str],
        model: str,
    ) -> tuple[str, str | None]:
        payload = {
            "model": model,
            "message": prompt,
            "temperature": 0.2,
        }
        response = requests.post(endpoint, json=payload, headers=headers, timeout=45)
        if response.ok:
            text = self._extract_v1_text(response.json())
            return text, None
        return "", self._response_error(response)

    def ask(self, question: str, docs: list[dict], song_summaries: list[dict]) -> str:
        context = _build_context(docs, song_summaries, question=question)
        if not self.api_key:
            return _fallback_answer(question, docs)

        prompt = (
            "You are a music campaign analyst assistant. Use the context to answer the question. "
            "Prioritize exact values from report snippets when available. "
            "If data is missing, say exactly what is missing. Keep answers concise and factual.\n\n"
            f"Context:\n{context}\n\n"
            f"Question: {question}"
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        attempts: list[tuple[str, str]] = [
            ("v2", "https://api.cohere.com/v2/chat"),
            ("v2", "https://api.cohere.ai/v2/chat"),
            ("v1", "https://api.cohere.com/v1/chat"),
            ("v1", "https://api.cohere.ai/v1/chat"),
        ]

        failures: list[str] = []
        model_removed_detected = False
        for model in self._model_candidates():
            for mode, endpoint in attempts:
                if mode == "v2":
                    text, error_message = self._request_v2(prompt, endpoint, headers, model)
                else:
                    text, error_message = self._request_v1(prompt, endpoint, headers, model)

                if text:
                    return text

                if error_message:
                    failures.append(f"{model} @ {endpoint}: {error_message}")
                    if self._looks_like_removed_model_error(error_message):
                        model_removed_detected = True
                        break

        fallback = _fallback_answer(question, docs)
        details = "\n".join(failures[:4])
        guidance = (
            "Set COHERE_MODEL to a live model (for example command-a-03-2025) and retry."
            if model_removed_detected
            else "Check COHERE_API_KEY and network access, then retry."
        )
        return (
            "Cohere request failed for all tried models/endpoints.\n"
            f"{guidance}\n"
            f"{details}\n\n{fallback}"
        )
