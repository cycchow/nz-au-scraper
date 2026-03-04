"""Scraper provider registry."""

from .base import ScraperProvider
from .loveracing_provider import LoveracingProvider
from .racingcom_provider import RacingComProvider

__all__ = ["ScraperProvider", "LoveracingProvider", "RacingComProvider"]
