"""
Toy AI Agent for company research using Tavily API.

This module implements a simplified company research agent based on Tavily,
adapted from a notebook implementation.
"""

import os
import time
from typing import TypedDict, List, Dict, Any, Optional
from datetime import datetime

from tavily import TavilyClient  # type: ignore


class ResearchState(TypedDict):
    """Simplified research state for tracking company research."""
    query: str
    sources: List[Dict[str, Any]]
    research_complete: bool
    error: Optional[str]


class TavilySearchTool:
    """Wrapper for Tavily search API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Tavily client.
        
        Args:
            api_key: Tavily API key. If None, reads from TAVILY_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("TAVILY_API_KEY")
        if not self.api_key:
            raise ValueError("TAVILY_API_KEY must be provided or set as environment variable")
        self.client = TavilyClient(api_key=self.api_key)
    
    def search(self, query: str, max_results: int = 5) -> List[Dict[str, Any]]:
        """
        Search for information using Tavily.
        
        Args:
            query: Search query string
            max_results: Maximum number of results to return
            
        Returns:
            List of search results with title, url, content, score
        """
        try:
            response = self.client.search(
                query=query,
                max_results=max_results,
                search_depth="advanced"
            )
            return response.get("results", [])
        except Exception as e:
            raise RuntimeError(f"Tavily search failed: {str(e)}") from e


class CompanyResearcher:
    """
    Company research agent using Tavily for information retrieval.
    
    This class performs company research by querying Tavily API and
    collecting relevant sources about a company.
    """
    
    def __init__(
        self,
        tavily_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        agent_version: str = "1.0.0",
        max_sources: int = 5
    ):
        """
        Initialize the company researcher.
        
        Args:
            tavily_api_key: Tavily API key (optional, reads from env if not provided)
            openai_api_key: OpenAI API key (optional, reads from env if not provided)
            agent_version: Version identifier for the agent
            max_sources: Maximum number of sources to retrieve per query
        """
        self.tavily_tool = TavilySearchTool(api_key=tavily_api_key)
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.agent_version = agent_version
        self.max_sources = max_sources
        
        # Initialize OpenAI client if API key is available (optional dependency)
        self.openai_client = None
        if self.openai_api_key:
            try:
                from openai import OpenAI  # type: ignore
                self.openai_client = OpenAI(api_key=self.openai_api_key)
            except ImportError:
                pass  # openai not installed; agent works with Tavily only
    
    def research(self, query: str) -> ResearchState:
        """
        Perform company research for the given query.
        
        Args:
            query: Company name or research query
            
        Returns:
            ResearchState dictionary with query, sources, research_complete, and error
        """
        state: ResearchState = {
            "query": query,
            "sources": [],
            "research_complete": False,
            "error": None
        }
        
        try:
            # Search for information about the company
            sources = self.tavily_tool.search(query, max_results=self.max_sources)
            
            # Format sources
            formatted_sources = []
            for source in sources:
                formatted_sources.append({
                    "title": source.get("title", ""),
                    "url": source.get("url", ""),
                    "content": source.get("content", ""),
                    "score": source.get("score", 0.0)
                })
            
            state["sources"] = formatted_sources
            state["research_complete"] = True
            
        except Exception as e:
            state["error"] = str(e)
            state["research_complete"] = False
        
        return state
    
    def get_research_summary(self, state: ResearchState) -> str:
        """
        Generate a summary of the research results.
        
        Args:
            state: ResearchState from research() method
            
        Returns:
            Formatted summary string
        """
        if state["error"]:
            return f"Research failed: {state['error']}"
        
        if not state["research_complete"]:
            return "Research incomplete."
        
        summary_lines = [f"Research Query: {state['query']}", ""]
        summary_lines.append(f"Found {len(state['sources'])} sources:")
        summary_lines.append("")
        
        for i, source in enumerate(state["sources"], 1):
            summary_lines.append(f"{i}. {source['title']}")
            summary_lines.append(f"   URL: {source['url']}")
            summary_lines.append(f"   Score: {source['score']:.2f}")
            summary_lines.append("")
        
        return "\n".join(summary_lines)
