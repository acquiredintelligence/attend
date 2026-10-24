"""Judges: ``jev_judge`` (TypeSafe's Jev, bring your own key), ``decisions_judge`` (the OpenAI Decisions API) and
``llm_judge`` (any model with structured output). The hosted two need the ``http`` extra (httpx)."""

from ._defaults import DEFAULT_CRITERIA, DEFAULT_SYSTEM
from ._http import PermanentProviderError, ProviderError
from .decisions import decisions_judge
from .jev import jev_judge
from .llm import llm_judge

__all__ = ["DEFAULT_CRITERIA", "DEFAULT_SYSTEM", "PermanentProviderError", "ProviderError", "decisions_judge", "jev_judge", "llm_judge"]
