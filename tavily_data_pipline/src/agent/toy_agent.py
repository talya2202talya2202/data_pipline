"""
Toy AI Agent for company research using Tavily API.

Three-step research: overview search, competitor search, and OpenAI summarization.
"""

import json
import os
import time
from typing import TypedDict, List, Dict, Any, Optional
from datetime import datetime, timezone

from tavily import TavilyClient  # type: ignore


class StepResult(TypedDict):
    """Result of a single research step."""
    step_name: str
    status: str
    latency_ms: float
    error: Optional[str]


class ApiCallResult(TypedDict):
    """Result of a single external API call."""
    provider: str
    query: str
    results_returned: int
    latency_ms: float
    called_at: str


class ResearchState(TypedDict):
    """Research state tracking company research progress and results."""
    query: str
    company_name: Optional[str]
    industry: Optional[str]
    summary: Optional[str]
    sources: List[Dict[str, Any]]
    steps: List[StepResult]
    api_calls: List[ApiCallResult]
    research_complete: bool
    error: Optional[str]


class TavilySearchTool:
    """Wrapper for Tavily search API."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TAVILY_API_KEY")
        if not self.api_key:
            raise ValueError("TAVILY_API_KEY must be provided or set as environment variable")
        self.client = TavilyClient(api_key=self.api_key)

    def search(self, query: str, max_results: int = 5) -> List[Dict[str, Any]]:
        try:
            response = self.client.search(
                query=query,
                max_results=max_results,
                search_depth="advanced",
            )
            return response.get("results", [])
        except Exception as e:
            raise RuntimeError(f"Tavily search failed: {str(e)}") from e


class CompanyResearcher:
    """
    Company research agent: two Tavily searches (overview + competitors)
    and an optional OpenAI summarization step that extracts a normalized
    company name, industry label, and brief summary.
    """

    def __init__(
        self,
        tavily_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        agent_version: str = "1.0.0",
        max_sources: int = 5,
    ):
        self.tavily_tool = TavilySearchTool(api_key=tavily_api_key)
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.agent_version = agent_version
        self.max_sources = max_sources

        self.openai_client = None
        if self.openai_api_key:
            try:
                from openai import OpenAI  # type: ignore
                self.openai_client = OpenAI(api_key=self.openai_api_key)
            except ImportError:
                pass

    # ------------------------------------------------------------------
    # Internal steps
    # ------------------------------------------------------------------

    def _tavily_step(
        self, step_name: str, query: str, state: ResearchState,
    ) -> List[Dict[str, Any]]:
        """Run a single Tavily search and record the step + API call."""
        t0 = time.time()
        error = None
        formatted: List[Dict[str, Any]] = []
        try:
            raw = self.tavily_tool.search(query, max_results=self.max_sources)
            formatted = [
                {
                    "title": s.get("title", ""),
                    "url": s.get("url", ""),
                    "content": s.get("content", ""),
                    "score": s.get("score", 0.0),
                }
                for s in raw
            ]
        except Exception as e:
            error = str(e)

        latency = (time.time() - t0) * 1000
        state["steps"].append({
            "step_name": step_name,
            "status": "failure" if error else "success",
            "latency_ms": latency,
            "error": error,
        })
        state["api_calls"].append({
            "provider": "tavily",
            "query": query,
            "results_returned": len(formatted),
            "latency_ms": latency,
            "called_at": datetime.now(timezone.utc).isoformat(),
        })
        return formatted

    def _summarize_step(
        self, query: str, sources: List[Dict[str, Any]], state: ResearchState,
    ) -> None:
        """Use OpenAI to extract company_name, industry, and a brief summary."""
        if not self.openai_client:
            state["steps"].append({
                "step_name": "summarize",
                "status": "skipped",
                "latency_ms": 0.0,
                "error": None,
            })
            state["company_name"] = query
            return

        combined = "\n\n".join(
            f"[{s['title']}] ({s['url']})\n{s['content']}"
            for s in sources[:10]
        )
        prompt = (
            f'Given this research about "{query}":\n\n{combined}\n\n'
            "Extract the following as JSON (no markdown fences, just raw JSON):\n"
            '{"company_name": "<official company name>", '
            '"industry": "<single label, e.g. SaaS, Fintech, Semiconductors, '
            'AI/ML, Healthcare, E-commerce>", '
            '"summary": "<2-3 sentence summary>"}'
        )

        t0 = time.time()
        error = None
        try:
            resp = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=300,
            )
            text = resp.choices[0].message.content.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            parsed = json.loads(text)
            state["company_name"] = parsed.get("company_name", query)
            state["industry"] = parsed.get("industry")
            state["summary"] = parsed.get("summary")
        except Exception as e:
            error = str(e)
            state["company_name"] = query

        latency = (time.time() - t0) * 1000
        state["steps"].append({
            "step_name": "summarize",
            "status": "failure" if error else "success",
            "latency_ms": latency,
            "error": error,
        })
        state["api_calls"].append({
            "provider": "openai",
            "query": f"summarize: {query}",
            "results_returned": 0 if error else 1,
            "latency_ms": latency,
            "called_at": datetime.now(timezone.utc).isoformat(),
        })

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def research(self, query: str) -> ResearchState:
        """
        Three-step company research:
        1. search_overview — Tavily search for company overview
        2. search_competitors — Tavily search for competitors and market
        3. summarize — OpenAI extracts company_name, industry, and summary

        If OpenAI is not configured, step 3 is skipped (company_name = raw
        query, industry = null).
        """
        state: ResearchState = {
            "query": query,
            "company_name": None,
            "industry": None,
            "summary": None,
            "sources": [],
            "steps": [],
            "api_calls": [],
            "research_complete": False,
            "error": None,
        }

        try:
            overview = self._tavily_step(
                "search_overview", f"{query} company overview", state,
            )
            competitors = self._tavily_step(
                "search_competitors",
                f"{query} competitors market landscape",
                state,
            )

            state["sources"] = overview + competitors

            self._summarize_step(query, state["sources"], state)

            has_sources = any(
                s["status"] == "success"
                for s in state["steps"]
                if s["step_name"].startswith("search_")
            )
            state["research_complete"] = has_sources

        except Exception as e:
            state["error"] = str(e)
            state["research_complete"] = False

        return state

    def get_research_summary(self, state: ResearchState) -> str:
        """Generate a human-readable summary of research results."""
        if state["error"]:
            return f"Research failed: {state['error']}"
        if not state["research_complete"]:
            return "Research incomplete."

        lines = [f"Company: {state.get('company_name', state['query'])}"]
        if state.get("industry"):
            lines.append(f"Industry: {state['industry']}")
        if state.get("summary"):
            lines.append(f"Summary: {state['summary']}")
        lines.append(f"\nSources ({len(state['sources'])}):")
        for i, s in enumerate(state["sources"], 1):
            lines.append(f"  {i}. {s['title']} ({s['url']})")
        return "\n".join(lines)
