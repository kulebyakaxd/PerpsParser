"""
Пакет парсеров для различных бирж деривативов
"""

from .hyperliquid_parser import HyperliquidParser
from .lighter_parser import LighterParser

__all__ = ['HyperliquidParser', 'LighterParser']
