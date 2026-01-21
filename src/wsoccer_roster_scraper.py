#!/usr/bin/env python3
"""
NCAA Women's Soccer Roster Scraper - Unified Architecture
Adapted from WBB scraper for soccer-specific needs

Usage:
    python src/wsoccer_roster_scraper.py -season 2024-25 -division I
    python src/wsoccer_roster_scraper.py -team 457 -url https://goheels.com/sports/womens-soccer -season 2024-25
"""

import os
import re
import csv
import json
import argparse
import logging
import subprocess
from html import unescape
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import tldextract

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class Player:
    """Player data structure for NCAA women's soccer rosters"""
    team_id: int
    team: str
    season: str
    division: str = ""
    player_id: Optional[str] = None
    name: str = ""
    jersey: str = ""
    position: str = ""
    height: str = ""
    year: str = ""  # Academic year (class)
    major: str = ""  # Soccer rosters often include major (basketball doesn't)
    hometown: str = ""
    high_school: str = ""
    previous_school: str = ""
    url: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV output"""
        d = asdict(self)

        # Clean string fields to remove excessive whitespace/newlines before output
        for k, v in list(d.items()):
            if isinstance(v, str):
                d[k] = FieldExtractors.clean_text(v)

        # Map 'year' field to 'class' for CSV output (matches existing schema)
        d['class'] = d.pop('year', '')
        # Map team_id to ncaa_id (matches existing schema)
        d['ncaa_id'] = d.pop('team_id')
        # Remove player_id from CSV output (internal use only)
        d.pop('player_id', None)
        return d


# ============================================================================
# FIELD EXTRACTORS
# ============================================================================

class FieldExtractors:
    """Common utilities for extracting player fields from text and HTML"""

    @staticmethod
    def extract_jersey_number(text: str) -> str:
        """Extract jersey number from various text patterns"""
        if not text:
            return ''

        patterns = [
            r'Jersey Number[:\s]+(\d+)',
            r'#(\d{1,2})\b',
            r'No\.?[:\s]*(\d{1,2})\b',
            r'\b(\d{1,2})\s+(?=[A-Z])',  # Number followed by capitalized name
            r'^\s*(\d{1,2})\s*$',  # Plain number (1-2 digits, with optional whitespace)
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return ''

    @staticmethod
    def extract_height(text: str) -> str:
        """
        Extract height from various formats (imperial and metric)

        Formats supported:
        - 6'2" or 6-2 (imperial)
        - 6'2" / 1.88m (both)
        - 1.88m (metric only)
        """
        if not text:
            return ''

        patterns = [
            r"(\d+['\′]\s*\d+[\"\″']{1,2}(?:\s*/\s*\d+\.\d+m)?)",  # 6'2" or 6'5'' or 6'2" / 1.88m
            r"(\d+['\′]\s*\d+[\"\″']{1,2})",                        # 6'2" or 6'5''
            r"(\d+-\d+)",                                            # 6-2
            r"(\d+\.\d+m)",                                          # 1.88m
            r"Height:\s*([^\,\n]+)",                                 # Height: label format
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return ''

    @staticmethod
    def extract_position(text: str) -> str:
        """
        Extract position from text - SOCCER VERSION

        Soccer positions: GK, D, M, F (Goalkeeper, Defender, Midfielder, Forward)
        Also handles variations: MF, DF, FW, etc.
        """
        if not text:
            return ''

        # Clean the text
        text = text.strip()

        # Look for abbreviated position patterns (most common first)
        # Expanded to include more variations: ST, A, ATT, W, LW, RW, CB, LB, RB, CM, CDM, CAM, B (Back), etc.
        position_match = re.search(
            r'\b(GK|G|GOALKEEPER|'  # Goalkeeper variations
            r'D|DEF|DF|DEFENDER|B|BACK|CB|LB|RB|LCB|RCB|FB|LWB|RWB|'  # Defender variations (B=Back)
            r'M|MF|MID|MIDFIELDER|CM|CDM|CAM|DM|AM|LM|RM|'  # Midfielder variations
            r'F|FW|FOR|FORWARD|ST|STRIKER|A|ATT|ATTACKER|W|WING|WINGER|LW|RW)\b',
            text,
            re.IGNORECASE
        )
        if position_match:
            pos = position_match.group(1).upper()

            # Normalize variations to standard positions (GK, D, M, F)
            # Goalkeeper
            if pos in ('GK', 'G', 'GOALKEEPER'):
                return 'GK'
            # Defender (including B for Back)
            elif pos in ('DEF', 'DF', 'DEFENDER', 'B', 'BACK', 'CB', 'LB', 'RB', 'LCB', 'RCB', 'FB', 'LWB', 'RWB'):
                return 'D'
            # Midfielder
            elif pos in ('MID', 'MF', 'MIDFIELDER', 'CM', 'CDM', 'CAM', 'DM', 'AM', 'LM', 'RM'):
                return 'M'
            # Forward
            elif pos in ('FOR', 'FW', 'FORWARD', 'ST', 'STRIKER', 'A', 'ATT', 'ATTACKER', 'W', 'WING', 'WINGER', 'LW', 'RW'):
                return 'F'
            # Return as-is if it's one of the standard forms
            return pos

        # Look for full position names (fallback)
        text_upper = text.upper()
        if 'GOALKEEPER' in text_upper or 'GOALIE' in text_upper or 'KEEPER' in text_upper:
            return 'GK'
        elif 'DEFENDER' in text_upper or 'DEFENCE' in text_upper or 'DEFENSE' in text_upper or 'BACK' in text_upper:
            return 'D'
        elif 'MIDFIELDER' in text_upper or 'MIDFIELD' in text_upper:
            return 'M'
        elif 'FORWARD' in text_upper or 'STRIKER' in text_upper or 'ATTACKER' in text_upper or 'ATTACK' in text_upper:
            return 'F'

        return ''

    @staticmethod
    def normalize_academic_year(year_text: str) -> str:
        """Normalize academic year abbreviations to full forms"""
        if not year_text:
            return ''

        year_map = {
            'Fr': 'Freshman', 'Fr.': 'Freshman', 'FR': 'Freshman',
            'So': 'Sophomore', 'So.': 'Sophomore', 'SO': 'Sophomore',
            'Jr': 'Junior', 'Jr.': 'Junior', 'JR': 'Junior',
            'Sr': 'Senior', 'Sr.': 'Senior', 'SR': 'Senior',
            'Gr': 'Graduate', 'Gr.': 'Graduate', 'GR': 'Graduate',
            'R-Fr': 'Redshirt Freshman', 'R-Fr.': 'Redshirt Freshman',
            'R-So': 'Redshirt Sophomore', 'R-So.': 'Redshirt Sophomore',
            'R-Jr': 'Redshirt Junior', 'R-Jr.': 'Redshirt Junior',
            'R-Sr': 'Redshirt Senior', 'R-Sr.': 'Redshirt Senior',
            '1st': 'Freshman', 'First': 'Freshman',
            '2nd': 'Sophomore', 'Second': 'Sophomore',
            '3rd': 'Junior', 'Third': 'Junior',
            '4th': 'Senior', 'Fourth': 'Senior',
        }

        cleaned = year_text.strip()
        return year_map.get(cleaned, year_text)

    @staticmethod
    def parse_hometown_school(text: str) -> Dict[str, str]:
        """
        Parse hometown and school information from combined text

        Handles:
        - "City, State / High School"
        - "City, Country / High School" (international players)
        - "City, State / High School / Previous College"
        """
        result = {'hometown': '', 'high_school': '', 'previous_school': ''}

        if not text:
            return result

        # Clean the text
        text = re.sub(r'\s*(Instagram|Twitter|Opens in a new window).*$', '', text)
        text = re.sub(r'\s+', ' ', text).strip()

        # Pattern 1: City, State/Country followed by school info separated by /
        if '/' in text:
            parts = [p.strip() for p in text.split('/')]
            result['hometown'] = parts[0] if parts else ''

            if len(parts) > 1:
                # Check if second part looks like a college/university
                college_indicators = ['University', 'College', 'State', 'Tech', 'Institute']
                if any(indicator in parts[1] for indicator in college_indicators):
                    result['previous_school'] = parts[1]
                else:
                    result['high_school'] = parts[1]

            if len(parts) > 2:
                result['previous_school'] = parts[2]
        else:
            # No separator, just store as hometown
            result['hometown'] = text

        return result

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""

        # Remove extra whitespace and normalize
        cleaned = re.sub(r'\s+', ' ', text.strip())

        # Remove common unwanted elements
        cleaned = re.sub(r'\s*(Full Bio|Instagram|Twitter|Opens in a new window).*$', '', cleaned)

        # Strip common labelled prefixes
        cleaned = FieldExtractors.clean_field_labels(cleaned)

        return cleaned

    @staticmethod
    def clean_field_labels(text: str) -> str:
        """
        Remove label prefixes like 'Class:', 'Hometown:', 'High school:', 'Ht.:', 'Pos.:'

        This helps with sites that dump labelled content into table cells or bio blocks.
        """
        if not text:
            return text

        # Common label patterns to strip
        patterns = [
            r'\bClass:\s*', r'\bHometown:\s*', r'\bHigh school:\s*',
            r'\bPrevious College:\s*', r'\bPrevious School:\s*',
            r'\bHt\.?:\s*', r'\bPos\.?:\s*', r'\bMajor:\s*',
            r'^High school:\s*', r'^Hometown:\s*', r'^No\.?:\s*',
        ]

        for p in patterns:
            text = re.sub(p, '', text, flags=re.IGNORECASE).strip()

        return text


# ============================================================================
# SEASON VERIFICATION
# ============================================================================

class SeasonVerifier:
    """Centralized season verification logic"""

    @staticmethod
    def verify_season_on_page(html, expected_season: str) -> bool:
        """
        Verify that the roster page is for the expected season

        Args:
            html: BeautifulSoup object or HTML string
            expected_season: Expected season string (e.g., "2024-25")

        Returns:
            bool: True if season matches, False otherwise
        """
        try:
            elements_to_check = []

            # Check h1, h2, and title elements
            for tag in ['h1', 'h2']:
                elements = html.find_all(tag) if hasattr(html, 'find_all') else []
                elements_to_check.extend(elements)

            title = html.find('title') if hasattr(html, 'find') else None
            if title:
                elements_to_check.append(title)

            for element in elements_to_check:
                text = element.get_text(strip=True) if hasattr(element, 'get_text') else str(element)

                # Check for season and 'roster' keyword
                if expected_season in text and 'roster' in text.lower():
                    logger.info(f"✓ Season verification passed: found '{text.strip()}'")
                    return True

            logger.warning(f"✗ Season verification failed: no header found with '{expected_season}' and 'roster'")
            return False

        except Exception as e:
            logger.warning(f"Season verification error: {e}")
            return True  # Default to True if verification fails (don't block scraping)

    @staticmethod
    def is_sidearm_site(html) -> bool:
        """Check if this is a Sidearm-based site"""
        try:
            sidearm_indicators = [
                html.find('li', {'class': 'sidearm-roster-player'}),
                html.find('div', {'class': 'sidearm-roster-list-item'}),
                html.find('span', {'class': 'sidearm-roster-player-name'}),
                html.find_all('div', class_=lambda x: x and 'sidearm' in x.lower())
            ]

            return any(indicator for indicator in sidearm_indicators)
        except Exception as e:
            logger.warning(f"Failed to detect Sidearm site: {e}")
            return False


# ============================================================================
# URL BUILDER
# ============================================================================

class URLBuilder:
    """Build roster URLs for different site patterns"""

    @staticmethod
    def build_roster_url(base_url: str, season: str, url_format: str = 'default') -> str:
        """
        Build roster URL based on site pattern

        Args:
            base_url: Team's URL from teams.csv (e.g., 'https://goheels.com/sports/womens-soccer')
            season: Season string (e.g., '2024-25')
            url_format: URL pattern type
                - 'default': {base_url}/roster/{season}
                - 'wsoc': {base_url}/2024-25/roster (or /roster/2024)
                - 'sports_wsoc': {base_url}/sports/wsoc/{season}/roster

        Returns:
            Full roster URL
        """
        base_url = base_url.rstrip('/')

        # If a full roster URL is already provided, use it as-is
        if '/roster/' in base_url:
            return base_url

        if url_format == 'default':
            # Most common: Sidearm Sports pattern (87.6% of teams - 719 teams)
            # Example: https://ualbanysports.com/sports/womens-soccer/roster/2025
            return f"{base_url}/roster/{season}"

        elif url_format == 'wsoc_index':
            # wsoc/index pattern (11.9% of teams - 98 teams)
            # teams.csv has /sports/wsoc/index, need /sports/wsoc/roster/2025
            # Example: https://aicyellowjackets.com/sports/wsoc/roster/2025
            # These redirect to Sidearm HTML after following redirects
            if '/index' in base_url:
                return base_url.replace('/index', f'/roster/{season}')
            else:
                return f"{base_url}/roster/{season}"

        elif url_format == 'wsoc_plain':
            # /sports/wsoc without /index (e.g., Rollins)
            # Example: https://rollinssports.com/sports/wsoc/roster/2025
            return f"{base_url}/roster/{season}"

        elif url_format == 'ucf_table':
            # UCF uses /roster/season/{season}?view=table
            # Example: https://ucfknights.com/sports/womens-soccer/roster/season/2025?view=table
            return f"{base_url}/roster/season/{season}?view=table"

        elif url_format == 'virginia_season':
            # Virginia uses /roster/season/{season-range}/
            # Example: https://virginiasports.com/sports/wsoc/roster/season/2025-26/
            # Convert single year to range (2025 -> 2025-26)
            try:
                year = int(season)
                next_year = str(year + 1)[-2:]  # Get last 2 digits
                season_range = f"{year}-{next_year}"
                return f"{base_url}/roster/season/{season_range}/"
            except ValueError:
                # If season is already a range, use as-is
                return f"{base_url}/roster/season/{season}/"

        elif url_format == 'clemson_roster':
            # Clemson uses /roster/season/{year}/ with just the year
            # Example: https://clemsontigers.com/sports/womens-soccer/roster/season/2025/
            try:
                # Extract just the year from season string (e.g., '2024-25' -> '2025')
                if '-' in season:
                    year = season.split('-')[1]
                    # If it's a 2-digit year, convert to 4-digit
                    if len(year) == 2:
                        year = f"20{year}"
                else:
                    year = season
                # Request the table/list view to surface full columns (Class, Hometown, High school)
                return f"{base_url}/roster/season/{year}/?view=table"
            except (ValueError, IndexError):
                # Fallback to just using season as-is
                return f"{base_url}/roster/season/{season}/?view=table"

        elif url_format == 'kentucky_season':
            # Kentucky uses /roster/season/{year}/ (just year, no conversion needed)
            # Example: https://ukathletics.com/sports/wsoc/roster/season/2025/
            try:
                # If season is a range like '2024-25', extract the end year
                if '-' in season:
                    year = season.split('-')[1]
                    # If it's a 2-digit year, convert to 4-digit
                    if len(year) == 2:
                        year = f"20{year}"
                else:
                    year = season
                return f"{base_url}/roster/season/{year}/"
            except (ValueError, IndexError):
                # Fallback to just using season as-is
                return f"{base_url}/roster/season/{season}/"

        elif url_format == 'wsoc_season_range':
            # /sports/wsoc/{season-range}/roster format (used by some schools like Emory & Henry)
            # Example: https://gowasps.com/sports/wsoc/2025-26/roster
            # Convert single year to range (2025 -> 2025-26)
            # First, remove /index if present in base_url
            clean_url = base_url.replace('/index', '')
            try:
                year = int(season)
                next_year = str(year + 1)[-2:]  # Get last 2 digits
                season_range = f"{year}-{next_year}"
                return f"{clean_url}/{season_range}/roster"
            except ValueError:
                # If season is already a range, use as-is
                return f"{clean_url}/{season}/roster"

        else:
            # Fallback to default
            logger.warning(f"Unknown url_format '{url_format}', using default")
            return f"{base_url}/roster/{season}"

    @staticmethod
    def extract_base_url(full_url: str) -> str:
        """
        Extract base URL from full team URL

        Example:
            'https://goheels.com/sports/womens-soccer' → 'https://goheels.com'
        """
        extracted = tldextract.extract(full_url)

        # Build domain with subdomain if present
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{extracted.domain}.{extracted.suffix}"
        else:
            domain = f"{extracted.domain}.{extracted.suffix}"

        return f"https://{domain}"


# ============================================================================
# TEAM CONFIGURATION
# ============================================================================

class TeamConfig:
    """Team-specific configuration and categorization"""

    # Team-specific configurations
    # Format: ncaa_id: {'url_format': 'format_type', 'requires_js': bool, 'notes': '...'}
    TEAM_CONFIGS = {
        14: {'url_format': 'default', 'requires_js': False, 'notes': 'Albany - Standard Sidearm'},
        72: {'url_format': 'default', 'requires_js': True, 'notes': 'Bradley - Vue.js rendered roster'},
        74: {'url_format': 'wsoc_season_range', 'requires_js': False, 'notes': 'Bridgeport - /sports/wsoc/YYYY-YY/roster format with data-field attributes'},
        128: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'UCF - Custom URL + JS rendering'},
        147: {'url_format': 'clemson_roster', 'requires_js': False, 'notes': 'Clemson - Custom WordPress roster with person__item structure'},
        216: {'url_format': 'wsoc_season_range', 'requires_js': False, 'notes': 'Emory & Henry - /sports/wsoc/YYYY-YY/roster format'},
        248: {'url_format': 'default', 'requires_js': True, 'notes': 'George Mason - Sidearm list-item + JS rendering'},
        334: {'url_format': 'kentucky_season', 'requires_js': True, 'notes': 'Kentucky - WMT Digital /roster/season/YYYY/ format (use JS-rendered List view / table)'},
        513: {'url_format': 'virginia_season', 'requires_js': True, 'notes': 'Notre Dame - WMT Digital /roster/season/YYYY-YY/ format (JS-rendered List view)'},
        648: {'url_format': 'kentucky_season', 'requires_js': True, 'notes': 'South Carolina - WMT Digital /roster/season/YYYY/ (use JS-rendered List/table view)'},
        1023: {'url_format': 'wsoc_season_range', 'requires_js': False, 'notes': 'Coker - /sports/wsoc/YYYY-YY/roster format'},
        457: {'url_format': 'default', 'requires_js': False, 'notes': 'UNC - Standard Sidearm'},
        523: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'Old Dominion - Custom URL + JS rendering'},
        539: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'Penn State - Custom URL + JS rendering'},
        630: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'San Jose State - Custom URL + JS rendering'},
        674: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'Stanford - Custom URL + JS rendering'},
        742: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'Va Tech - Custom URL + JS rendering'},
        746: {'url_format': 'virginia_season', 'requires_js': False, 'notes': 'Virginia - WMT Digital /roster/season/YYYY-YY/ format'},
        813: {'url_format': 'default', 'requires_js': False, 'notes': 'Yale - Standard Sidearm'},
        1000: {'url_format': 'wsoc_season_range', 'requires_js': False, 'notes': 'Carson-Newman - /sports/w-soccer/YYYY-YY/roster format'},
        1356: {'url_format': 'ucf_table', 'requires_js': True, 'notes': 'Seattle - Custom URL + JS rendering'},
        186: {'url_format': 'wsoc_season_range', 'requires_js': True, 'notes': 'Dist. Columbia - /sports/wsoc/YYYY-YY/roster format with JS-rendered player names'},
        8956: {'url_format': 'wsoc_season_range', 'requires_js': False, 'notes': 'Dominican (NY) - /sports/wsoc/YYYY-YY/roster format'},
        # Teams with 403 Forbidden - require JavaScript rendering (bot protection)
        28: {'url_format': 'default', 'requires_js': True, 'notes': 'Arizona St. - 403 forbidden without JS'},
        37: {'url_format': 'kentucky_season', 'requires_js': True, 'notes': 'Auburn - /roster/season/YYYY/ pattern (JS)'},
        52: {'url_format': 'default', 'requires_js': True, 'notes': 'Bellarmine - 403 forbidden without JS'},
        136: {'url_format': 'default', 'requires_js': True, 'notes': 'Chicago St. - 403 forbidden without JS'},
        140: {'url_format': 'default', 'requires_js': True, 'notes': 'Cincinnati - 403 forbidden without JS'},
        178: {'url_format': 'default', 'requires_js': True, 'notes': 'Delaware St. - 403 forbidden without JS'},
        312: {'url_format': 'default', 'requires_js': True, 'notes': 'Iowa - 403 forbidden without JS'},
        314: {'url_format': 'default', 'requires_js': True, 'notes': 'Jackson St. - 403 forbidden without JS'},
        415: {'url_format': 'virginia_season', 'requires_js': True, 'notes': 'Miami (FL) - /roster/season/YYYY-YY/ pattern (JS)'},
        432: {'url_format': 'default', 'requires_js': True, 'notes': 'Mississippi Val. - 403 forbidden without JS'},
        473: {'url_format': 'virginia_season', 'requires_js': True, 'notes': 'New Mexico - /roster/season/YYYY-YY/ pattern (JS)'},
        559: {'url_format': 'kentucky_season', 'requires_js': True, 'notes': 'Purdue - /roster/season/YYYY/ pattern (JS)'},
        603: {'url_format': 'default', 'requires_js': True, 'notes': 'St. John\'s - 403 forbidden without JS'},
        620: {'url_format': 'default', 'requires_js': True, 'notes': 'St. Thomas (MN) - 403 forbidden without JS'},
        626: {'url_format': 'kentucky_season', 'requires_js': True, 'notes': 'San Diego St. - /roster/season/YYYY/ pattern (JS)'},
        699: {'url_format': 'default', 'requires_js': True, 'notes': 'Texas Southern - 403 forbidden without JS'},
        731: {'url_format': 'default', 'requires_js': True, 'notes': 'Utah St. - 403 forbidden without JS'},
        794: {'url_format': 'default', 'requires_js': True, 'notes': 'Green Bay - 403 forbidden without JS'},
        811: {'url_format': 'default', 'requires_js': True, 'notes': 'Wyoming - 403 forbidden without JS'},
        1104: {'url_format': 'default', 'requires_js': True, 'notes': 'Grand Canyon - SSL error without JS, likely bot protection'},
        1395: {'url_format': 'default', 'requires_js': True, 'notes': 'Tarleton St. - 403 forbidden without JS'},
        2707: {'url_format': 'default', 'requires_js': True, 'notes': 'Kansas City - 403 forbidden without JS'},
        11504: {'url_format': 'default', 'requires_js': True, 'notes': 'Queens (NC) - 403 forbidden without JS'},
        30135: {'url_format': 'default', 'requires_js': True, 'notes': 'California Baptist - SSL error without JS, likely bot protection'},
    }

    @classmethod
    def requires_javascript(cls, team_id: int) -> bool:
        """
        Check if a team requires JavaScript rendering

        Args:
            team_id: NCAA team ID

        Returns:
            True if team requires JSScraper
        """
        if team_id in cls.TEAM_CONFIGS:
            return cls.TEAM_CONFIGS[team_id].get('requires_js', False)
        return False

    @classmethod
    def get_url_format(cls, team_id: int, team_url: str = '') -> str:
        """
        Get URL format for a team

        Can auto-detect from URL pattern if team_url is provided

        Args:
            team_id: NCAA team ID
            team_url: Optional team URL for auto-detection

        Returns:
            URL format string ('default', 'wsoc_index', etc.)
        """
        # Check if explicitly configured
        if team_id in cls.TEAM_CONFIGS:
            return cls.TEAM_CONFIGS[team_id].get('url_format', 'default')

        # Auto-detect from URL if provided
        if team_url:
            if '/sports/wsoc/index' in team_url or '/Sports/wsoc' in team_url:
                return 'wsoc_index'
            elif '/sports/wsoc' in team_url:
                # /sports/wsoc without /index (e.g., Rollins)
                return 'wsoc_plain'
            elif '/sports/womens-soccer' in team_url or '/sports/w-soccer' in team_url:
                return 'default'

        # Default to standard Sidearm pattern
        return 'default'

    @classmethod
    def get_scraper_type(cls, team_id: int) -> str:
        """
        Determine which scraper to use for a team

        Returns:
            'standard' - StandardScraper (Sidearm sites)
            'table' - TableScraper (wsoc and data-label sites)
        """
        if team_id in cls.STANDARD_TEAMS:
            return 'standard'
        elif team_id in cls.MSOC_TEAMS or team_id in cls.TABLE_TEAMS:
            return 'table'
        else:
            # Default to standard
            return 'standard'


# ============================================================================
# SCRAPERS
# ============================================================================

class StandardScraper:
    """Scraper for standard Sidearm Sports sites (majority of teams)"""

    def __init__(self, session: Optional[requests.Session] = None):
        """
        Initialize scraper

        Args:
            session: Optional requests Session for connection pooling
        """
        self.session = session or requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0'
        }

    def scrape_team(self, team_id: int, team_name: str, base_url: str, season: str, division: str = "") -> List[Player]:
        """
        Scrape roster for a single team

        Args:
            team_id: NCAA team ID
            team_name: Team name
            base_url: Base URL for team site
            season: Season string (e.g., '2024-25')
            division: Division ('I', 'II', 'III')

        Returns:
            List of Player objects
        """
        try:
            # Build roster URL (auto-detect format from base_url)
            url_format = TeamConfig.get_url_format(team_id, base_url)
            roster_url = URLBuilder.build_roster_url(base_url, season, url_format)

            logger.info(f"Scraping {team_name} - {roster_url}")

            # Fetch page
            response = self.session.get(roster_url, headers=self.headers, timeout=30)

            # If 404, try season-range URL format (e.g., 2025-26/roster)
            if response.status_code == 404:
                logger.info(f"Got 404, trying season-range URL format for {team_name}")
                alternative_url = self._build_season_range_url(base_url, season)
                if alternative_url != roster_url:
                    logger.info(f"Trying alternative URL: {alternative_url}")
                    response = self.session.get(alternative_url, headers=self.headers, timeout=30)
                    roster_url = alternative_url  # Update for logging

            # If still not 200, try /roster/ without season (some sites don't use season in URL)
            if response.status_code != 200:
                logger.info(f"Trying roster URL without season for {team_name}")
                no_season_url = f"{base_url.rstrip('/')}/roster/"
                if no_season_url != roster_url:
                    logger.info(f"Trying URL: {no_season_url}")
                    response = self.session.get(no_season_url, headers=self.headers, timeout=30)
                    roster_url = no_season_url  # Update for logging

            if response.status_code != 200:
                logger.warning(f"Failed to retrieve {team_name} - Status: {response.status_code}")
                return []

            # Parse HTML
            html = BeautifulSoup(response.content, 'html.parser')

            # Verify season
            if not SeasonVerifier.verify_season_on_page(html, season):
                logger.warning(f"Season mismatch for {team_name}")
                # Continue anyway - warning is enough

            # Extract players
            players = self._extract_players(html, team_id, team_name, season, division, base_url)

            # If no players found and we haven't tried /roster/ yet, try it
            if len(players) == 0 and not roster_url.endswith('/roster/'):
                no_season_url = f"{base_url.rstrip('/')}/roster/"
                if no_season_url != roster_url:
                    logger.info(f"No players found, trying roster URL without season: {no_season_url}")
                    response = self.session.get(no_season_url, headers=self.headers, timeout=30)
                    if response.status_code == 200:
                        html = BeautifulSoup(response.content, 'html.parser')
                        players = self._extract_players(html, team_id, team_name, season, division, base_url)
                        roster_url = no_season_url  # Update for logging

            # Enhance Kentucky players with bio page data (hometown, high school, class, url)
            if team_id == 334:  # Kentucky
                logger.info(f"Enhancing Kentucky players with bio page data...")
                players = self._enhance_kentucky_player_data(players)

            # Enhance positions from individual bio pages if many are missing
            # (Some Sidearm sites don't have positions on roster list page)
            missing_positions = sum(1 for p in players if not p.position and p.url)
            if missing_positions > 0 and len(players) > 0:
                missing_ratio = missing_positions / len(players)
                if missing_ratio > 0.2:  # If >20% of players are missing positions
                    players = self._enhance_sidearm_positions_from_bios(players, team_name)

            logger.info(f"✓ {team_name}: Found {len(players)} players")
            return players

        except requests.RequestException as e:
            logger.error(f"Request error for {team_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error scraping {team_name}: {e}")
            return []

    def _build_season_range_url(self, base_url: str, season: str) -> str:
        """
        Build alternative URL using season-range format (e.g., 2025-26)

        Args:
            base_url: Base URL from teams.csv
            season: Season string (e.g., '2025')

        Returns:
            Alternative roster URL with season range
        """
        base_url = base_url.rstrip('/')

        # Remove /index suffix if present (e.g., /sports/wsoc/index -> /sports/wsoc)
        if base_url.endswith('/index'):
            base_url = base_url[:-6]  # Remove '/index'

        # Convert single year to range (2025 -> 2025-26)
        try:
            year = int(season)
            next_year = str(year + 1)[-2:]  # Get last 2 digits
            season_range = f"{year}-{next_year}"
            return f"{base_url}/{season_range}/roster"
        except ValueError:
            # If season is already a range or invalid format, return as-is
            logger.warning(f"Could not parse season '{season}' for range URL")
            return f"{base_url}/{season}/roster"

    def _extract_players(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from Sidearm Sports HTML

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all player list items (Sidearm pattern)
        roster_items = html.find_all('li', class_='sidearm-roster-player')

        # Also check for alternate Sidearm format used by some schools (e.g., Bradley)
        if not roster_items:
            roster_items = html.find_all('li', class_='sidearm-roster-list-item')
            if roster_items:
                logger.info(f"Found {len(roster_items)} players using sidearm-roster-list-item format for {team_name}")
                # Use the alternate extraction method
                return self._extract_players_from_list_items(html, team_id, team_name, season, division, base_url)

        # Check for sidearm-list-card-item format (used by Elmira and similar)
        if not roster_items:
            card_items = html.find_all('li', class_='sidearm-list-card-item')
            if card_items:
                logger.info(f"Found {len(card_items)} players using sidearm-list-card-item format for {team_name}")
                return self._extract_players_from_card_items(html, team_id, team_name, season, division, base_url)

        if not roster_items:
            logger.warning(f"No roster items found for {team_name} (expected class='sidearm-roster-player')")

            # Check for WMT OAS API roster data (website-api/player-rosters)
            html_text = unescape(str(html))
            if 'website-api/player-rosters' in html_text:
                logger.info(f"Detected WMT OAS API roster data for {team_name}, using API parser")
                api_players = self._extract_players_from_oas_api(html_text, team_id, team_name, season, division, base_url)
                if api_players:
                    return api_players

            # Check if this is a data-field table (Bridgeport style with data-field-label attributes)
            data_field_table = html.find('table', attrs={'class': lambda x: x and 'table' in x})
            if data_field_table and data_field_table.find('th', attrs={'data-field-label': True}):
                logger.info(f"Detected data-field table format for {team_name}, using data-field parser")
                return self._extract_players_from_data_field_table(html, team_id, team_name, season, division, base_url)

            # Check if this is a mod-roster div (Emory & Henry style) with generic table
            mod_roster = html.find('div', class_='mod-roster')
            if mod_roster:
                roster_table = mod_roster.find('table')
                if roster_table:
                    logger.info(f"Detected mod-roster format for {team_name}, using generic table parser")
                    return self._extract_players_from_generic_roster_table(html, team_id, team_name, season, division, base_url)

            # Check if this is a Kentucky-style table (players-table__general) - has all data
            kentucky_table = html.find('table', id='players-table__general')
            if kentucky_table:
                logger.info(f"Detected Kentucky-style data table for {team_name}, using DataTables parser")
                return self._extract_players_from_kentucky_table(html, team_id, team_name, season, division, base_url)

            # Check if this is a PrestoSports site (data-label attributes)
            presto_cells = html.find_all('td', attrs={'data-label': True})
            if presto_cells:
                logger.info(f"Detected PrestoSports format for {team_name}, using data-label parser")
                return self._extract_players_from_presto_table(html, team_id, team_name, season, division, base_url)

            # Check if this is a Sidearm card-based layout (s-person-card)
            person_cards = html.find_all('div', class_='s-person-card')
            if person_cards:
                logger.info(f"Detected Sidearm card-based layout for {team_name}, using s-person-card parser")
                return self._extract_players_from_cards(html, team_id, team_name, season, division, base_url)

            # Check if this is a generic card-based layout (player-card)
            generic_cards = html.find_all('div', class_='player-card')
            if generic_cards:
                logger.info(f"Detected generic card-based layout for {team_name}, using player-card parser")
                return self._extract_players_from_generic_cards(html, team_id, team_name, season, division, base_url)

            # Check if this is a WMT Digital roster (roster__item)
            wmt_items = html.find_all('div', class_='roster__item')
            if wmt_items:
                logger.info(f"Detected WMT Digital format for {team_name}, using roster__item parser")
                return self._extract_players_from_wmt(html, team_id, team_name, season, division, base_url)

            # Check if this is a custom WordPress roster (person__item)
            wordpress_items = html.find_all('li', class_='person__item')
            if wordpress_items:
                logger.info(f"Detected custom WordPress roster format for {team_name}, using person__item parser")
                return self._extract_players_from_wordpress_roster(html, team_id, team_name, season, division, base_url)

            # Check for table--roster class (UCF and similar)
            roster_table = html.find('table', class_='table--roster')
            if roster_table:
                logger.info(f"Detected table--roster format for {team_name}")
                return self._extract_players_from_table(html, team_id, team_name, season, division, base_url)

            # Try Sidearm table-based roster format (s-table-body__row)
            logger.info(f"Attempting to parse Sidearm table-based roster for {team_name}")
            return self._extract_players_from_table(html, team_id, team_name, season, division, base_url)

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for item in roster_items:
            try:
                # Jersey number
                jersey_elem = item.find('span', class_='sidearm-roster-player-jersey-number')
                jersey = FieldExtractors.clean_text(jersey_elem.get_text()) if jersey_elem else ''

                # Name and URL - check both h3 and h2 (some schools use h2, e.g., Yale)
                name_elem = item.find('h3') or item.find('h2')
                if name_elem:
                    name_link = name_elem.find('a', href=True)
                    if name_link:
                        name = FieldExtractors.clean_text(name_link.get_text())
                        # Build absolute URL
                        href = name_link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                    else:
                        name = FieldExtractors.clean_text(name_elem.get_text())
                        profile_url = ''
                else:
                    logger.warning(f"No name found for player in {team_name}")
                    continue

                # Position
                pos_elem = item.find('span', class_='sidearm-roster-player-position-long-short')
                if not pos_elem:
                    # Alternative: position in text-bold span within sidearm-roster-player-position div
                    pos_div = item.find('div', class_='sidearm-roster-player-position')
                    if pos_div:
                        pos_elem = pos_div.find('span', class_='text-bold')

                # Extract position text
                if pos_elem:
                    position = FieldExtractors.extract_position(pos_elem.get_text())
                else:
                    # Fallback: look for any span with position-related class name
                    position = ''
                    all_spans = item.find_all('span')
                    for span in all_spans:
                        span_class = ' '.join(span.get('class', [])).lower()
                        if 'position' in span_class or 'pos' in span_class:
                            position = FieldExtractors.extract_position(span.get_text())
                            if position:  # Found valid position
                                break

                    # If still no position, look for any div with position-related class
                    if not position:
                        all_divs = item.find_all('div')
                        for div in all_divs:
                            div_class = ' '.join(div.get('class', [])).lower()
                            if 'position' in div_class or 'pos' in div_class:
                                position = FieldExtractors.extract_position(div.get_text())
                                if position:  # Found valid position
                                    break

                # Height
                height_elem = item.find('span', class_='sidearm-roster-player-height')
                height = FieldExtractors.extract_height(height_elem.get_text()) if height_elem else ''

                # Academic year
                year_elem = item.find('span', class_='sidearm-roster-player-academic-year')
                year = FieldExtractors.normalize_academic_year(year_elem.get_text()) if year_elem else ''

                # Major (soccer-specific)
                major_elem = item.find('span', class_='sidearm-roster-player-major')
                major = FieldExtractors.clean_text(major_elem.get_text()) if major_elem else ''

                # Hometown (check standard field, then custom2)
                hometown_elem = item.find('span', class_='sidearm-roster-player-hometown')
                if not hometown_elem:
                    hometown_elem = item.find('span', class_='sidearm-roster-player-custom2')
                hometown = FieldExtractors.clean_text(hometown_elem.get_text()) if hometown_elem else ''

                # High school (check standard field, then custom3)
                hs_elem = item.find('span', class_='sidearm-roster-player-highschool')
                if not hs_elem:
                    hs_elem = item.find('span', class_='sidearm-roster-player-custom3')
                high_school = FieldExtractors.clean_text(hs_elem.get_text()) if hs_elem else ''

                # Previous school (check standard field, then custom1)
                prev_elem = item.find('span', class_='sidearm-roster-player-previous-school')
                if not prev_elem:
                    prev_elem = item.find('span', class_='sidearm-roster-player-custom1')
                previous_school = FieldExtractors.clean_text(prev_elem.get_text()) if prev_elem else ''

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major=major,
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing player in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_kentucky_table(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from Kentucky's DataTables table (players-table__general)

        This table contains the full columns (Number, Name, Position, Height, Class, Hometown, Previous School, High school)
        and is rendered when requesting the table/list view. We look specifically for the table with id 'players-table__general'.
        """
        players = []

        # Look for known players table IDs used by WMT/WordPress (players-table, players-table__general)
        table = html.find('table', id=re.compile(r'^players-table'))
        if not table:
            logger.warning(f"Kentucky-style players table not found for {team_name}")
            return []

        # Header mapping
        header_map = {}
        thead = table.find('thead')
        headers = thead.find_all('th') if thead else table.find_all('th')
        for i, th in enumerate(headers):
            htext = th.get_text().strip().lower()
            if 'number' in htext or 'no' in htext or '#' in htext:
                header_map['jersey'] = i
            elif 'name' in htext:
                header_map['name'] = i
            elif 'position' in htext or 'pos' in htext:
                header_map['position'] = i
            elif 'height' in htext or 'ht' in htext:
                header_map['height'] = i
            elif 'class' in htext or 'yr' in htext or 'year' in htext:
                header_map['year'] = i
            elif 'hometown' in htext or 'home' in htext:
                header_map['hometown'] = i
            elif 'high' in htext and 'school' in htext:
                header_map['high_school'] = i
            elif 'previous' in htext:
                header_map['previous_school'] = i

        # Rows are typically in tbody
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')[1:]

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for row in rows:
            try:
                cells = row.find_all(['td', 'th'])
                if not cells or len(cells) < 2:
                    continue

                # Name and profile URL
                name = ''
                profile_url = ''
                if 'name' in header_map and header_map['name'] < len(cells):
                    name_cell = cells[header_map['name']]
                    link = name_cell.find('a', href=True)
                    if link:
                        name = FieldExtractors.clean_text(link.get_text())
                        href = link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                    else:
                        name = FieldExtractors.clean_text(name_cell.get_text())

                if not name:
                    continue

                # Jersey
                jersey = ''
                if 'jersey' in header_map and header_map['jersey'] < len(cells):
                    jersey = FieldExtractors.clean_text(cells[header_map['jersey']].get_text())

                # Position
                position = ''
                if 'position' in header_map and header_map['position'] < len(cells):
                    position = FieldExtractors.extract_position(cells[header_map['position']].get_text())

                # Height
                height = ''
                if 'height' in header_map and header_map['height'] < len(cells):
                    height = FieldExtractors.extract_height(cells[header_map['height']].get_text())

                # Year/Class
                year = ''
                if 'year' in header_map and header_map['year'] < len(cells):
                    year = FieldExtractors.normalize_academic_year(cells[header_map['year']].get_text().strip())

                # Hometown
                hometown = ''
                if 'hometown' in header_map and header_map['hometown'] < len(cells):
                    hometown = FieldExtractors.clean_text(cells[header_map['hometown']].get_text())

                # High school
                high_school = ''
                if 'high_school' in header_map and header_map['high_school'] < len(cells):
                    high_school = FieldExtractors.clean_text(cells[header_map['high_school']].get_text())

                # Previous school
                previous_school = ''
                if 'previous_school' in header_map and header_map['previous_school'] < len(cells):
                    previous_school = FieldExtractors.clean_text(cells[header_map['previous_school']].get_text())

                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing Kentucky table row for {team_name}: {e}")
                continue

        return players

    def _extract_players_from_oas_api(self, html_text: str, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from WMT OAS JSON API embedded in page (website-api/player-rosters)

        This handles Nuxt/WMT sites that render rosters via API calls rather than HTML.
        """
        players: List[Player] = []

        # Find API URL in rendered HTML
        api_match = re.search(r'https?://[^"\s]*?/website-api/player-rosters[^"\s]*', html_text)
        if not api_match:
            api_match = re.search(r'/website-api/player-rosters[^"\s]*', html_text)
        if not api_match:
            api_match = re.search(r'website-api/player-rosters[^"\s]*', html_text)
        if not api_match:
            return players

        api_url = api_match.group(0)

        # Normalize API URL
        base_parsed = urlparse(base_url)
        base_origin = f"{base_parsed.scheme or 'https'}://{base_parsed.netloc}"

        if api_url.startswith('http'):
            parsed = urlparse(api_url)
        else:
            api_url = f"{base_origin}/{api_url.lstrip('/')}"
            parsed = urlparse(api_url)

        # Replace placeholder host like oas-backend with real domain
        if parsed.netloc == 'oas-backend':
            parsed = parsed._replace(netloc=base_parsed.netloc, scheme=base_parsed.scheme or 'https')

        # Ensure required include params (player data)
        qs = parse_qs(parsed.query)
        include_raw = qs.get('include', [''])[0]
        include_parts = [p for p in include_raw.split(',') if p]
        required_includes = [
            'player',
            'photo',
            'classLevel',
            'playerPosition',
            'profileFieldValues.profileField',
            'profileFieldValues',
            'roster.sport',
            'roster.season'
        ]
        for inc in required_includes:
            if inc not in include_parts:
                include_parts.append(inc)
        qs['include'] = [','.join(include_parts)]

        # Ensure pagination defaults
        if 'per_page' not in qs:
            qs['per_page'] = ['200']
        if 'page' not in qs:
            qs['page'] = ['1']

        api_url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

        try:
            response = self.session.get(api_url, headers=self.headers, timeout=30)
            if response.status_code != 200:
                logger.warning(f"WMT OAS API request failed for {team_name}: {response.status_code}")
                return players

            payload = response.json()
            data = payload.get('data', [])
            if not data:
                return players

            for item in data:
                try:
                    player_info = item.get('player', {}) or {}

                    name = player_info.get('full_name') or " ".join([
                        player_info.get('first_name', ''),
                        player_info.get('last_name', '')
                    ]).strip()
                    if not name:
                        continue

                    jersey = (
                        player_info.get('jersey_number_label')
                        or item.get('jersey_number_label')
                        or player_info.get('jersey_number')
                        or item.get('jersey_number')
                        or ''
                    )
                    jersey = str(jersey) if jersey is not None else ''

                    pos_obj = item.get('player_position') or {}
                    pos_text = pos_obj.get('abbreviation') or pos_obj.get('name') or ''
                    position = FieldExtractors.extract_position(str(pos_text)) if pos_text else ''

                    feet = player_info.get('height_feet') or item.get('height_feet')
                    inches = player_info.get('height_inches') or item.get('height_inches')
                    if feet is not None and inches is not None:
                        height = f"{feet}-{inches}"
                    elif feet is not None:
                        height = str(feet)
                    else:
                        height = ''

                    class_obj = item.get('class_level') or {}
                    year_text = class_obj.get('name') or class_obj.get('abbreviation') or ''
                    year = FieldExtractors.normalize_academic_year(year_text)

                    major = player_info.get('major') or ''
                    hometown = player_info.get('hometown') or ''
                    high_school = player_info.get('high_school') or ''
                    previous_school = player_info.get('previous_school') or ''

                    profile_url = ''
                    slug = player_info.get('slug')
                    if slug and base_parsed.netloc:
                        profile_url = f"{base_origin}/sports/womens-soccer/roster/{slug}"

                    players.append(Player(
                        team_id=team_id,
                        team=team_name,
                        season=season,
                        division=division,
                        name=name,
                        jersey=jersey,
                        position=position,
                        height=height,
                        year=year,
                        major=major,
                        hometown=hometown,
                        high_school=high_school,
                        previous_school=previous_school,
                        url=profile_url
                    ))

                except Exception as e:
                    logger.warning(f"Error parsing WMT OAS API player for {team_name}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"WMT OAS API error for {team_name}: {e}")

        return players
    def _extract_players_from_generic_roster_table(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from generic mod-roster HTML table format

        Used by schools like Emory & Henry that have a simple HTML table inside
        a <div class="mod-roster"> container. The table has a header row with buttons
        containing column names (sort buttons with aria-labels).

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find the mod-roster div and its table
        mod_roster = html.find('div', class_='mod-roster')
        if not mod_roster:
            logger.warning(f"No mod-roster div found for {team_name}")
            return []

        table = mod_roster.find('table')
        if not table:
            logger.warning(f"No table found in mod-roster for {team_name}")
            return []

        # Extract headers from the header row
        thead = table.find('thead')
        if not thead:
            logger.warning(f"No thead found in mod-roster table for {team_name}")
            return []

        header_row = thead.find('tr')
        if not header_row:
            logger.warning(f"No header row found in mod-roster table for {team_name}")
            return []

        headers = header_row.find_all('th')
        if not headers:
            logger.warning(f"No th headers found in mod-roster table for {team_name}")
            return []

        # Build header mapping by looking at button aria-labels or button text
        header_map = {}
        for i, header in enumerate(headers):
            # Try to get aria-label from button inside th
            button = header.find('button')
            if button:
                aria_label = button.get('aria-label', '').lower()
                button_text = button.get_text().strip().lower()
                text_to_check = aria_label if aria_label else button_text
            else:
                text_to_check = header.get_text().strip().lower()

            # Map to fields
            if 'number' in text_to_check or 'no.' in text_to_check or 'no:' in text_to_check:
                header_map['jersey'] = i
            elif 'name' in text_to_check or 'last_name' in text_to_check:
                header_map['name'] = i
            elif 'position' in text_to_check or 'pos.' in text_to_check or 'pos:' in text_to_check:
                header_map['position'] = i
            elif 'year' in text_to_check or 'yr.' in text_to_check or 'yr:' in text_to_check:
                header_map['year'] = i
            elif 'height' in text_to_check or 'ht.' in text_to_check or 'ht:' in text_to_check:
                header_map['height'] = i
            elif 'weight' in text_to_check or 'wt.' in text_to_check or 'wt:' in text_to_check:
                header_map['weight'] = i
            elif 'hometown' in text_to_check or 'home' in text_to_check or 'hometown/previous' in text_to_check:
                header_map['hometown'] = i

        logger.info(f"mod-roster headers for {team_name}: {header_map}")

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        # Extract rows from tbody
        tbody = table.find('tbody')
        if not tbody:
            logger.warning(f"No tbody found in mod-roster table for {team_name}")
            return []

        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} rows in mod-roster table for {team_name}")

        for row in rows:
            try:
                cells = row.find_all(['td', 'th'])
                if not cells or len(cells) < 2:
                    continue

                # Extract Name (with URL) - name is typically in a th with class="name"
                name = ''
                profile_url = ''
                if 'name' in header_map and header_map['name'] < len(cells):
                    name_cell = cells[header_map['name']]
                    name_link = name_cell.find('a', href=True)
                    if name_link:
                        name = FieldExtractors.clean_text(name_link.get_text())
                        href = name_link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"

                if not name or name.lower() == 'null':
                    continue

                # Extract Jersey
                jersey = ''
                if 'jersey' in header_map and header_map['jersey'] < len(cells):
                    jersey_cell = cells[header_map['jersey']]
                    # Remove label spans if they exist
                    for label_span in jersey_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    jersey = FieldExtractors.clean_text(jersey_cell.get_text())

                # Extract Position
                position = ''
                if 'position' in header_map and header_map['position'] < len(cells):
                    pos_cell = cells[header_map['position']]
                    # Remove label spans if they exist
                    for label_span in pos_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    position = FieldExtractors.extract_position(pos_cell.get_text())

                # Extract Height
                height = ''
                if 'height' in header_map and header_map['height'] < len(cells):
                    height_cell = cells[header_map['height']]
                    # Remove label spans if they exist
                    for label_span in height_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    height = FieldExtractors.extract_height(height_cell.get_text())

                # Extract Year/Class
                year = ''
                if 'year' in header_map and header_map['year'] < len(cells):
                    year_cell = cells[header_map['year']]
                    # Remove label spans if they exist
                    for label_span in year_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    year = FieldExtractors.normalize_academic_year(year_cell.get_text())

                # Extract Hometown / High School
                hometown = ''
                high_school = ''
                if 'hometown' in header_map and header_map['hometown'] < len(cells):
                    hometown_cell = cells[header_map['hometown']]
                    # Remove label spans if they exist
                    for label_span in hometown_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    hometown_hs = FieldExtractors.clean_text(hometown_cell.get_text())
                    if ' / ' in hometown_hs:
                        hometown, high_school = hometown_hs.split(' / ', 1)
                    else:
                        hometown = hometown_hs

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',
                    hometown=hometown,
                    high_school=high_school,
                    previous_school='',
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing mod-roster row for {team_name}: {e}")
                continue

        return players

    def _extract_players_from_data_field_table(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from data-field table format

        Used by schools like Bridgeport that use data-field-label and data-field attributes
        on th and td elements. Headers have data-field-label attributes, and data cells
        have data-field attributes with field names like "number", "first_name:last_name", etc.

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find any table with data-field-label headers
        table = None
        for t in html.find_all('table'):
            if t.find('th', attrs={'data-field-label': True}):
                table = t
                break

        if not table:
            logger.warning(f"No data-field table found for {team_name}")
            return []

        # Extract headers using data-field-label
        headers = table.find_all('th', attrs={'data-field-label': True})
        if not headers:
            logger.warning(f"No data-field headers found for {team_name}")
            return []

        # Build header mapping
        header_map = {}
        for i, header in enumerate(headers):
            label = header.get('data-field-label', '').strip().lower()
            if 'no.' in label or 'number' in label:
                header_map['jersey'] = i
            elif 'name' in label:
                header_map['name'] = i
            elif 'pos' in label:
                header_map['position'] = i
            elif 'cl.' in label or 'class' in label or 'year' in label or 'yr' in label:
                header_map['year'] = i
            elif 'ht.' in label or 'height' in label:
                header_map['height'] = i
            elif 'wt.' in label or 'weight' in label:
                header_map['weight'] = i
            elif 'hometown' in label or 'home' in label:
                header_map['hometown'] = i
            elif 'high' in label and 'school' in label:
                header_map['high_school'] = i
            elif 'previous' in label and ('college' in label or 'school' in label):
                header_map['previous_school'] = i

        logger.info(f"data-field headers for {team_name}: {header_map}")

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        # Extract rows from tbody
        tbody = table.find('tbody')
        if not tbody:
            logger.warning(f"No tbody found in data-field table for {team_name}")
            return []

        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} rows in data-field table for {team_name}")

        for row in rows:
            try:
                cells = row.find_all(['td', 'th'])
                if not cells or len(cells) < 2:
                    continue

                # Extract Name (with URL)
                name = ''
                profile_url = ''
                if 'name' in header_map and header_map['name'] < len(cells):
                    name_cell = cells[header_map['name']]
                    # Look for links - there may be multiple (image link, name link, etc.)
                    # We want the link with actual name text content
                    name_link = None
                    for link in name_cell.find_all('a', href=True):
                        link_text = link.get_text().strip()
                        # Skip image links and other empty links
                        if len(link_text) > 2:  # Name should be more than 2 chars
                            name_link = link
                            break
                    
                    if name_link:
                        name_text = name_link.get_text()
                        # Handle multi-line names with whitespace
                        name = FieldExtractors.clean_text(name_text)
                        href = name_link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                    else:
                        # Fallback to cell text if no link found
                        name = FieldExtractors.clean_text(name_cell.get_text())

                if not name or name.lower() == 'null':
                    continue

                # Extract Jersey
                jersey = ''
                if 'jersey' in header_map and header_map['jersey'] < len(cells):
                    jersey_cell = cells[header_map['jersey']]
                    # Remove label spans if they exist
                    for label_span in jersey_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    jersey = FieldExtractors.clean_text(jersey_cell.get_text())

                # Extract Position
                position = ''
                if 'position' in header_map and header_map['position'] < len(cells):
                    pos_cell = cells[header_map['position']]
                    # Remove label spans if they exist
                    for label_span in pos_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    position = FieldExtractors.extract_position(pos_cell.get_text())

                # Extract Height
                height = ''
                if 'height' in header_map and header_map['height'] < len(cells):
                    height_cell = cells[header_map['height']]
                    # Remove label spans if they exist
                    for label_span in height_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    height = FieldExtractors.extract_height(height_cell.get_text())

                # Extract Year/Class
                year = ''
                if 'year' in header_map and header_map['year'] < len(cells):
                    year_cell = cells[header_map['year']]
                    # Remove label spans if they exist
                    for label_span in year_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    year = FieldExtractors.normalize_academic_year(year_cell.get_text())

                # Extract Hometown / High School
                hometown = ''
                high_school = ''
                if 'hometown' in header_map and header_map['hometown'] < len(cells):
                    hometown_cell = cells[header_map['hometown']]
                    # Remove label spans if they exist
                    for label_span in hometown_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    hometown_hs = FieldExtractors.clean_text(hometown_cell.get_text())
                    if ' / ' in hometown_hs:
                        hometown, high_school = hometown_hs.split(' / ', 1)
                    else:
                        hometown = hometown_hs

                # Extract High School (if in separate column)
                if not high_school and 'high_school' in header_map and header_map['high_school'] < len(cells):
                    hs_cell = cells[header_map['high_school']]
                    # Remove label spans if they exist
                    for label_span in hs_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    high_school = FieldExtractors.clean_text(hs_cell.get_text())

                # Extract Previous School
                previous_school = ''
                if 'previous_school' in header_map and header_map['previous_school'] < len(cells):
                    prev_cell = cells[header_map['previous_school']]
                    # Remove label spans if they exist
                    for label_span in prev_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    previous_school = FieldExtractors.clean_text(prev_cell.get_text())

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing data-field row for {team_name}: {e}")
                continue

        return players

    def _extract_players_from_list_items(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from Sidearm list-item format (used by Bradley and similar schools)

        This format uses <li class="sidearm-roster-list-item"> with nested divs containing player data

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all roster list items
        roster_items = html.find_all('li', class_='sidearm-roster-list-item')

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for item in roster_items:
            try:
                # Find the link element which contains most of the data
                link_elem = item.find(class_='sidearm-roster-list-item-link')
                if not link_elem:
                    continue

                # Jersey number - in photo div, inside a span
                jersey = ''
                number_box = item.find('div', class_='sidearm-roster-list-item-number')
                if number_box:
                    jersey = FieldExtractors.extract_jersey_number(number_box.get_text())
                photo_number = item.find('div', class_='sidearm-roster-list-item-photo-number')
                if photo_number:
                    number_span = photo_number.find('span')
                    if number_span:
                        jersey = FieldExtractors.clean_text(number_span.get_text())
                    else:
                        jersey = FieldExtractors.clean_text(photo_number.get_text())
                # Fallback: any element with number/jersey class
                if not jersey:
                    number_elem = item.find(class_=lambda x: x and ('number' in x.lower() or 'jersey' in x.lower()) if x else False)
                    if number_elem:
                        jersey = FieldExtractors.extract_jersey_number(number_elem.get_text())

                # Name and URL - in name div
                name = ''
                profile_url = ''
                name_elem = item.find('div', class_='sidearm-roster-list-item-name')
                if name_elem:
                    name_link = name_elem.find('a', href=True)
                    if name_link:
                        name = FieldExtractors.clean_text(name_link.get_text())
                        href = name_link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                    else:
                        # Sometimes name is just in the div without a link
                        name = FieldExtractors.clean_text(name_elem.get_text())

                # Fallback: any anchor with visible text
                if not name:
                    for link in item.find_all('a', href=True):
                        link_text = FieldExtractors.clean_text(link.get_text())
                        if link_text:
                            name = link_text
                            href = link['href']
                            if href.startswith('http'):
                                profile_url = href
                            else:
                                profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                            break

                if not name:
                    logger.warning(f"No name found for player in {team_name}")
                    continue

                # Position and other metadata - use specific classes
                position = ''
                year = ''
                height = ''
                hometown = ''
                high_school = ''
                previous_school = ''

                # Position
                pos_elem = item.find('span', class_='sidearm-roster-list-item-position')
                if pos_elem:
                    position = FieldExtractors.extract_position(pos_elem.get_text())

                # Fallback: look for any span with position-related class name
                if not position:
                    all_spans = item.find_all('span')
                    for span in all_spans:
                        span_class = ' '.join(span.get('class', [])).lower()
                        if 'position' in span_class or 'pos' in span_class:
                            position = FieldExtractors.extract_position(span.get_text())
                            if position:  # Found valid position
                                break

                # If still no position, look for any div with position-related class
                if not position:
                    all_divs = item.find_all('div')
                    for div in all_divs:
                        div_class = ' '.join(div.get('class', [])).lower()
                        if 'position' in div_class or 'pos' in div_class:
                            position = FieldExtractors.extract_position(div.get_text())
                            if position:  # Found valid position
                                break

                # Year/Class
                year_elem = item.find('span', class_='sidearm-roster-list-item-year')
                if year_elem:
                    year = FieldExtractors.normalize_academic_year(year_elem.get_text())

                # Height
                height_elem = item.find('span', class_='sidearm-roster-list-item-height')
                if height_elem:
                    height = FieldExtractors.extract_height(height_elem.get_text())

                # Hometown
                hometown_elem = item.find('div', class_='sidearm-roster-list-item-hometown')
                if hometown_elem:
                    hometown = FieldExtractors.clean_text(hometown_elem.get_text())

                # High school and Previous school
                # Can be in sidearm-roster-list-item-highschool span
                hs_elem = item.find('span', class_='sidearm-roster-list-item-highschool')
                if hs_elem:
                    hs_text = FieldExtractors.clean_text(hs_elem.get_text())
                    # Parse format like "Northern Guilford High School (USC Upstate)" or "(SIUE)"
                    if '(' in hs_text and ')' in hs_text:
                        # Split by parentheses
                        main_part = hs_text[:hs_text.index('(')].strip()
                        paren_part = hs_text[hs_text.index('(') + 1:hs_text.index(')')].strip()
                        
                        # Check if paren part looks like a college/university
                        college_indicators = ['University', 'College', 'State', 'Tech', 'Institute', 'Univ', 'U.', 'SC', 'NJIT', 'SIUE', 'DME', 'DePaul', 'Evansville', 'Bonaventure']
                        if any(indicator.lower() in paren_part.lower() for indicator in college_indicators):
                            # Paren part is previous school, main part is high school
                            if main_part:
                                high_school = main_part
                            previous_school = paren_part
                        else:
                            # Main part is high school, paren part is additional info (ignore)
                            high_school = main_part if main_part else paren_part
                    else:
                        high_school = hs_text

                # Check for explicit previous-school span (some sites have this)
                prev_elem = item.find('span', class_='sidearm-roster-list-item-previous-school')
                if prev_elem:
                    prev_text = FieldExtractors.clean_text(prev_elem.get_text())
                    if prev_text:
                        previous_school = prev_text

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',  # Not typically available in list-item format
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing list-item in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_card_items(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from sidearm-list-card-item format (used by Elmira and similar schools)

        This format uses <li class="sidearm-list-card-item"> with player data in card structure

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all card items
        card_items = html.find_all('li', class_='sidearm-list-card-item')

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for item in card_items:
            try:
                # Jersey number - in sidearm-roster-player-jersey span
                jersey = ''
                jersey_elem = item.find('span', class_='sidearm-roster-player-jersey')
                if jersey_elem:
                    jersey_span = jersey_elem.find('span')
                    if jersey_span:
                        jersey = FieldExtractors.extract_jersey_number(jersey_span.get_text())

                # Name and URL - in sidearm-roster-player-name link
                name = ''
                profile_url = ''
                name_elem = item.find('a', class_='sidearm-roster-player-name')
                if name_elem:
                    name = FieldExtractors.clean_text(name_elem.get_text())
                    if name_elem.get('href'):
                        href = name_elem['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"

                if not name:
                    continue

                # Position - in sidearm-roster-player-position-short div
                position = ''
                pos_elem = item.find('div', class_='sidearm-roster-player-position-short')
                if pos_elem:
                    position = FieldExtractors.extract_position(pos_elem.get_text())

                # Height, weight, year - often in combined div
                height = ''
                year = ''
                hwyr_elem = item.find('div', class_='sidearm-roster-details-height-weight-year-custom')
                if hwyr_elem:
                    text = hwyr_elem.get_text()
                    height = FieldExtractors.extract_height(text)
                    year = FieldExtractors.normalize_academic_year(text)

                # Hometown and schools - in combined div
                hometown = ''
                high_school = ''
                hs_elem = item.find('div', class_='sidearm-roster-details-hometown-schools')
                if hs_elem:
                    text = FieldExtractors.clean_text(hs_elem.get_text())
                    # Usually format like "City, State / High School"
                    if '/' in text:
                        parts = text.split('/', 1)
                        hometown = parts[0].strip()
                        high_school = parts[1].strip()
                    else:
                        hometown = text

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',
                    hometown=hometown,
                    high_school=high_school,
                    previous_school='',
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing card-item in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_table(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from table-based roster format (header-aware)

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find the roster table (not coaching staff or other tables)
        # First, try to find Sidearm-specific rows
        table_rows = html.find_all('tr', class_='s-table-body__row')
        roster_table = None
        is_generic_table = False

        if table_rows:
            # Find the table that contains these rows
            for row in table_rows:
                table = row.find_parent('table')
                if table:
                    roster_table = table
                    break

        # If no Sidearm-specific rows, look for any table with roster-like headers
        if not roster_table:
            all_tables = html.find_all('table')
            for table in all_tables:
                # Check if table has header row with roster-like column names
                # First look in thead, then fall back to first row
                header_row = table.find('thead')
                if header_row:
                    header_row = header_row.find('tr')
                else:
                    header_row = table.find('tr')
                
                if header_row:
                    header_cells = header_row.find_all(['th', 'td'])
                    header_text = ' '.join([cell.get_text().strip().lower() for cell in header_cells])
                    # Look for roster-like headers (handle abbreviations like 'pos.' for 'position')
                    has_name = 'name' in header_text
                    has_position = 'position' in header_text or 'pos' in header_text
                    if has_name and has_position:
                        roster_table = table
                        is_generic_table = True
                        logger.info(f"Found generic roster table for {team_name}")
                        break

        if not roster_table:
            logger.warning(f"Could not find roster table for {team_name}")
            return []

        # Find table headers - different logic for Sidearm vs generic tables
        headers = []
        if is_generic_table:
            # For generic tables, get headers from thead if available, otherwise first row
            thead = roster_table.find('thead')
            if thead:
                first_row = thead.find('tr')
            else:
                first_row = roster_table.find('tr')
            
            if first_row:
                headers = first_row.find_all(['th', 'td'])
            if not headers:
                logger.warning(f"No table headers found in generic roster table for {team_name}")
                return []
        else:
            # For Sidearm tables, look for specific header class
            headers = roster_table.find_all('th', class_='s-table-header__column')
            if not headers:
                logger.warning(f"No table headers found in roster table for {team_name}")
                return []

        # Create header mapping (normalize header text)
        header_map = {}
        for i, header in enumerate(headers):
            header_text = header.get_text().strip().lower()
            # Map various header names to our fields
            if 'name' in header_text and 'first' not in header_text:
                header_map['name'] = i
            elif any(k in header_text for k in ('no', '#', 'jersey', 'number', 'num')):
                header_map['jersey'] = i
            elif 'pos' in header_text:
                header_map['position'] = i
            elif 'ht' in header_text or 'height' in header_text:
                header_map['height'] = i
            elif 'yr' in header_text or 'year' in header_text or 'class' in header_text or 'cl.' in header_text:
                header_map['year'] = i
            elif 'hometown' in header_text or 'home' in header_text:
                header_map['hometown'] = i
            elif 'high school' in header_text or 'highschool' in header_text:
                header_map['high_school'] = i
            elif 'previous' in header_text:
                header_map['previous_school'] = i

        logger.info(f"Table headers for {team_name}: {header_map}")

        # Filter table rows to only those in the roster table
        if is_generic_table:
            # For generic tables, get all rows from tbody if it exists, otherwise all rows except first
            tbody = roster_table.find('tbody')
            if tbody:
                table_rows = tbody.find_all('tr')
            else:
                all_rows = roster_table.find_all('tr')
                table_rows = all_rows[1:] if len(all_rows) > 1 else []
        else:
            # For Sidearm tables, look for specific row class
            table_rows = roster_table.find_all('tr', class_='s-table-body__row')
        logger.info(f"Found {len(table_rows)} table rows in roster table for {team_name}")

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for row in table_rows:
            try:
                # Get all cells (including both td and th for generic tables)
                all_cells = row.find_all(['td', 'th'])

                # Filter out hidden cells used for responsive design (d-md-none, d-none, etc.)
                # These create duplicate cells that break column index mapping
                cells = []
                for cell in all_cells:
                    cell_classes = cell.get('class', [])
                    # Skip cells that are hidden on desktop (d-md-none) or always hidden (d-none)
                    if 'd-md-none' in cell_classes:
                        continue
                    cells.append(cell)

                if len(cells) < 3:  # Need at least some basic data
                    continue

                # Extract Name (with URL)
                name = ''
                profile_url = ''
                if 'name' in header_map:
                    name_cell = cells[header_map['name']]
                    name_link = name_cell.find('a', href=True)
                    if name_link:
                        name = FieldExtractors.clean_text(name_link.get_text())
                        href = name_link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                    else:
                        name = FieldExtractors.clean_text(name_cell.get_text())

                if not name or name.lower() == 'null':
                    continue

                # Extract Jersey
                jersey = ''
                if 'jersey' in header_map:
                    jersey_cell = cells[header_map['jersey']]
                    # Remove label spans if they exist
                    for label_span in jersey_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    jersey = FieldExtractors.clean_text(jersey_cell.get_text())

                # Extract Position
                position = ''
                if 'position' in header_map:
                    pos_cell = cells[header_map['position']]
                    # Remove label spans if they exist
                    for label_span in pos_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    position = FieldExtractors.extract_position(pos_cell.get_text())

                # Extract Height
                height = ''
                if 'height' in header_map:
                    height_cell = cells[header_map['height']]
                    # Remove label spans if they exist
                    for label_span in height_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    height = FieldExtractors.extract_height(height_cell.get_text())

                # Extract Year/Class
                year = ''
                if 'year' in header_map:
                    year_cell = cells[header_map['year']]
                    # Remove label spans if they exist
                    for label_span in year_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    year = FieldExtractors.normalize_academic_year(year_cell.get_text())

                # Extract Hometown / High School
                hometown = ''
                high_school = ''
                if 'hometown' in header_map:
                    hometown_cell = cells[header_map['hometown']]
                    # Remove label spans if they exist
                    for label_span in hometown_cell.find_all('span', class_='label'):
                        label_span.decompose()
                    hometown_hs = FieldExtractors.clean_text(hometown_cell.get_text())
                    if ' / ' in hometown_hs:
                        hometown, high_school = hometown_hs.split(' / ', 1)
                    else:
                        hometown = hometown_hs

                # If high_school has its own column, use that instead
                if 'high_school' in header_map:
                    high_school = FieldExtractors.clean_text(cells[header_map['high_school']].get_text())

                # Extract Previous School
                previous_school = ''
                if 'previous_school' in header_map:
                    previous_school = FieldExtractors.clean_text(cells[header_map['previous_school']].get_text())

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',  # Not available in table format
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing table row in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_presto_table(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from PrestoSports data-label table format

        PrestoSports uses <td data-label="..."> attributes instead of Sidearm's structure.
        Example: <td data-label="No.">5</td>

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all table rows (each row is a player)
        rows = html.find_all('tr')

        for row in rows:
            try:
                # Look for cells with data-label attributes
                cells_with_labels = row.find_all('td', attrs={'data-label': True})

                if not cells_with_labels or len(cells_with_labels) < 3:
                    continue  # Skip header rows or empty rows

                # Extract data by data-label attribute
                data = {}
                for cell in cells_with_labels:
                    label = cell.get('data-label', '').strip().lower()
                    # Get text content, excluding label spans
                    # Look for nested label span and exclude it
                    label_spans = cell.find_all('span', class_='label')
                    if label_spans:
                        # Get all text except from label spans
                        cell_copy = str(cell)
                        from bs4 import BeautifulSoup
                        cell_soup = BeautifulSoup(cell_copy, 'html.parser')
                        for span in cell_soup.find_all('span', class_='label'):
                            span.decompose()
                        value = FieldExtractors.clean_text(cell_soup.get_text())
                    else:
                        value = FieldExtractors.clean_text(cell.get_text())
                    data[label] = value

                # Also check for name in <th> with data-label
                name_header = row.find('th', attrs={'data-label': True})
                if name_header:
                    label = name_header.get('data-label', '').strip().lower()
                    # Get first non-empty anchor tag for name (skip image links)
                    all_links = name_header.find_all('a')
                    name_found = False
                    for link in all_links:
                        link_text = FieldExtractors.clean_text(link.get_text())
                        if link_text:  # Skip empty links (e.g., image links)
                            data[label] = link_text
                            name_found = True
                            break
                    if not name_found:
                        data[label] = FieldExtractors.clean_text(name_header.get_text())

                # Map data-label values to player fields
                name = data.get('name', '')
                jersey = data.get('no.', data.get('no', data.get('#', '')))
                position = data.get('pos.', data.get('pos', data.get('position', '')))
                height = FieldExtractors.extract_height(data.get('ht.', data.get('ht', data.get('height', ''))))
                year = FieldExtractors.normalize_academic_year(data.get('cl.', data.get('class', data.get('year', ''))))

                # Hometown/High School might be combined
                hometown_raw = data.get('hometown/last school', data.get('hometown', data.get('hometown/high school', '')))
                high_school = data.get('high school', data.get('last school', ''))
                hometown = ''

                # If combined field, try to split
                if hometown_raw and '/' in hometown_raw:
                    parts = hometown_raw.split('/')
                    hometown = parts[0].strip()
                    if not high_school:
                        high_school = parts[1].strip() if len(parts) > 1 else ''
                else:
                    hometown = hometown_raw

                # Previous school
                previous_school = data.get('previous school', data.get('last school', ''))

                # Skip if no name
                if not name:
                    continue

                # Extract profile URL from name link
                profile_url = ''
                name_link = None
                if name_header:
                    name_link = name_header.find('a')
                else:
                    # Search in cells for name link
                    for cell in cells_with_labels:
                        if data.get(cell.get('data-label', '').strip().lower()) == name:
                            name_link = cell.find('a')
                            if name_link:
                                break

                if name_link and name_link.get('href'):
                    href = name_link.get('href')
                    if href.startswith('http'):
                        profile_url = href
                    else:
                        profile_url = URLBuilder.extract_base_url(base_url) + href

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',  # Not typically available in PrestoSports
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing PrestoSports row in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_cards(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from Sidearm card-based layout (s-person-card divs)

        Used by some Sidearm sites like Davidson that display rosters as cards
        instead of lists or tables.

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all person cards
        cards = html.find_all('div', class_='s-person-card')

        for card in cards:
            try:
                # Extract name
                name_elem = card.find('div', class_='s-person-details__personal-single-line')
                name = FieldExtractors.clean_text(name_elem.get_text()) if name_elem else ''

                # Extract jersey from thumbnail text (e.g., "Jersey Number 0")
                jersey = ''
                thumbnail = card.find('div', class_='s-person-details__thumbnail')
                if thumbnail:
                    thumbnail_text = thumbnail.get_text()
                    if 'Jersey Number' in thumbnail_text:
                        jersey = thumbnail_text.replace('Jersey Number', '').strip()

                # Extract bio stats (position, year, height, weight)
                bio_stats = card.find_all('span', class_='s-person-details__bio-stats-item')
                position = ''
                year = ''
                height = ''

                for stat in bio_stats:
                    stat_text = stat.get_text().strip()
                    if stat_text.startswith('Position'):
                        position = FieldExtractors.extract_position(stat_text.replace('Position', '').strip())
                    elif stat_text.startswith('Academic Year'):
                        year = FieldExtractors.normalize_academic_year(stat_text.replace('Academic Year', '').strip())
                    elif stat_text.startswith('Height'):
                        height = FieldExtractors.extract_height(stat_text.replace('Height', '').strip())

                # Extract location info (hometown, high school)
                hometown = ''
                high_school = ''
                location_items = card.find_all('span', class_='s-person-card__content__person__location-item')
                for item in location_items:
                    item_text = item.get_text().strip()
                    if item_text.startswith('Hometown'):
                        hometown = item_text.replace('Hometown', '').strip()
                    elif item_text.startswith('Last School'):
                        high_school = item_text.replace('Last School', '').strip()

                # Extract profile URL
                profile_url = ''
                cta_link = card.find('a', href=True)
                if cta_link:
                    href = cta_link['href']
                    if href.startswith('http'):
                        profile_url = href
                    else:
                        extracted = tldextract.extract(base_url)
                        domain = f"{extracted.domain}.{extracted.suffix}"
                        if extracted.subdomain:
                            domain = f"{extracted.subdomain}.{domain}"
                        profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"

                # Skip if no name or no jersey (staff members typically don't have jersey numbers)
                if not name or not jersey:
                    continue

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',  # Not available in card format
                    hometown=hometown,
                    high_school=high_school,
                    previous_school='',  # Not available in card format
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing card in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_generic_cards(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from generic card-based layout (player-card divs)

        Used by schools like Goucher and Carlow that use a simple card structure with
        <div class="player-card"> containing player information

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all player cards - look for exact class 'player-card' to avoid matching wrapper/footer
        cards = html.find_all('div', class_=lambda x: 'player-card' in x.split() if x else False)

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for card in cards:
            try:
                # Find link first (needed for both name and URL)
                name_link = card.find('a', href=True)

                # Extract name - check for firstname/lastname structure first
                name = ''
                firstname_elem = card.find('span', class_='firstname')
                lastname_elem = card.find('span', class_='lastname')
                if firstname_elem and lastname_elem:
                    firstname = FieldExtractors.clean_text(firstname_elem.get_text())
                    lastname = FieldExtractors.clean_text(lastname_elem.get_text())
                    name = f"{firstname} {lastname}"

                # Fallback: look for name in link
                if not name and name_link:
                    name_span = name_link.find('span', class_='name')
                    if name_span:
                        name = FieldExtractors.clean_text(name_span.get_text())
                    else:
                        name = FieldExtractors.clean_text(name_link.get_text())

                # Final fallback: any header element
                if not name:
                    for tag in ['h2', 'h3', 'h4']:
                        name_elem = card.find(tag)
                        if name_elem:
                            name = FieldExtractors.clean_text(name_elem.get_text())
                            if name and len(name) > 3:  # Basic validation
                                break

                if not name:
                    continue

                # Extract jersey number - look for specific classes and patterns
                jersey = ''
                # Try 'number' class first (Goucher uses this)
                number_elem = card.find('span', class_='number')
                if number_elem:
                    jersey = FieldExtractors.extract_jersey_number(number_elem.get_text())
                else:
                    # Try generic jersey class
                    jersey_elem = card.find(class_=lambda x: x and 'jersey' in x.lower() if x else False)
                    if jersey_elem:
                        jersey = FieldExtractors.extract_jersey_number(jersey_elem.get_text())
                    else:
                        # Look for any number in the card
                        card_text = card.get_text()
                        jersey = FieldExtractors.extract_jersey_number(card_text)

                # Check for bio-attr-short structure (Carlow uses this)
                # Contains 3 spans in order: position, year, height
                bio_attr = card.find('div', class_='bio-attr-short')
                position = ''
                year = ''
                height = ''

                if bio_attr:
                    spans = bio_attr.find_all('span', class_='text-muted')
                    if len(spans) >= 1:
                        position = FieldExtractors.extract_position(spans[0].get_text())
                    if len(spans) >= 2:
                        year = FieldExtractors.normalize_academic_year(spans[1].get_text())
                    if len(spans) >= 3:
                        height = FieldExtractors.extract_height(spans[2].get_text())

                # If bio-attr-short didn't provide data, try individual element extraction
                if not position:
                    pos_elem = card.find(class_=lambda x: x and 'position' in x.lower() if x else False)
                    if pos_elem:
                        position = FieldExtractors.extract_position(pos_elem.get_text())
                    else:
                        # Try to find position in text
                        card_text = card.get_text()
                        position = FieldExtractors.extract_position(card_text)

                if not year:
                    year_elem = card.find(class_=lambda x: x and ('year' in x.lower() or 'class' in x.lower()) if x else False)
                    if year_elem:
                        year = FieldExtractors.normalize_academic_year(year_elem.get_text())
                    else:
                        # Try to find year in text
                        card_text = card.get_text()
                        year = FieldExtractors.normalize_academic_year(card_text)

                if not height:
                    height_elem = card.find(class_=lambda x: x and 'height' in x.lower() if x else False)
                    if height_elem:
                        height = FieldExtractors.extract_height(height_elem.get_text())
                    else:
                        # Try to find height in text
                        card_text = card.get_text()
                        height = FieldExtractors.extract_height(card_text)

                # Extract hometown and high school from bio-data section (Goucher uses this)
                hometown = ''
                high_school = ''
                previous_school = ''

                bio_data = card.find('div', class_='bio-data')
                if bio_data:
                    # Parse list items with labels
                    list_items = bio_data.find_all('li')
                    for item in list_items:
                        item_text = item.get_text()
                        if 'Hometown:' in item_text:
                            # Remove the label and get the value
                            label_span = item.find('span', class_='fw-bold')
                            if label_span:
                                label_span.decompose()
                            hometown = FieldExtractors.clean_text(item.get_text())
                        elif 'Highschool:' in item_text or 'High School:' in item_text:
                            label_span = item.find('span', class_='fw-bold')
                            if label_span:
                                label_span.decompose()
                            high_school = FieldExtractors.clean_text(item.get_text())
                        elif 'Previous School:' in item_text or 'Prior School:' in item_text:
                            label_span = item.find('span', class_='fw-bold')
                            if label_span:
                                label_span.decompose()
                            previous_school = FieldExtractors.clean_text(item.get_text())

                # Fallback: try class-based search if bio-data didn't work
                if not hometown:
                    hometown_elem = card.find(class_=lambda x: x and 'hometown' in x.lower() if x else False)
                    if hometown_elem:
                        hometown = FieldExtractors.clean_text(hometown_elem.get_text())

                if not high_school:
                    hs_elem = card.find(class_=lambda x: x and 'school' in x.lower() if x else False)
                    if hs_elem:
                        high_school = FieldExtractors.clean_text(hs_elem.get_text())

                # Extract profile URL
                profile_url = ''
                if name_link:
                    href = name_link['href']
                    if href.startswith('http'):
                        profile_url = href
                    else:
                        profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing generic card in {team_name}: {e}")
                continue

        return players

    def _extract_players_from_wmt(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from WMT Digital roster format (roster__item divs)

        Used by schools like Virginia that use WMT Digital platform with
        <div class="roster__item"> structure and schema.org itemprops

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all roster items
        roster_items = html.find_all('div', class_='roster__item')

        for item in roster_items:
            try:
                # Name - in itemprop="name" span with content attribute
                name = ''
                name_elem = item.find('span', itemprop='name')
                if name_elem and name_elem.get('content'):
                    name = name_elem['content'].strip()

                if not name:
                    continue

                # Jersey number - try two formats:
                # Virginia: in roster__image div > span
                # Kentucky: in roster-item__inner > span.roster-item__number
                jersey = ''
                
                # Try Virginia format first
                image_div = item.find('div', class_='roster__image')
                if image_div:
                    jersey_span = image_div.find('span')
                    if jersey_span:
                        jersey = jersey_span.get_text().strip()
                
                # Try Kentucky format if not found
                if not jersey:
                    number_span = item.find('span', class_='roster-item__number')
                    if number_span:
                        jersey = number_span.get_text().strip()

                # Profile URL - in roster__image div > a (Virginia) or main item > a (Kentucky)
                profile_url = ''
                if image_div:
                    link = image_div.find('a', href=True)
                    if link:
                        href = link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            # Extract base domain
                            extracted = tldextract.extract(base_url)
                            domain = f"{extracted.domain}.{extracted.suffix}"
                            if extracted.subdomain:
                                domain = f"{extracted.subdomain}.{domain}"
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"
                
                # Try Kentucky format if not found
                if not profile_url:
                    link = item.find('a', href=True)
                    if link:
                        href = link['href']
                        if href.startswith('http'):
                            profile_url = href
                        else:
                            # Extract base domain
                            extracted = tldextract.extract(base_url)
                            domain = f"{extracted.domain}.{extracted.suffix}"
                            if extracted.subdomain:
                                domain = f"{extracted.subdomain}.{domain}"
                            profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"

                # Other details are in roster__title and roster__description divs (Virginia)
                # OR in roster-item__info paragraph (Kentucky)
                # Title typically has position
                position = ''
                year = ''
                title_div = item.find('div', class_='roster__title')
                if title_div:
                    title_text = title_div.get_text()
                    position = FieldExtractors.extract_position(title_text)

                # Try Kentucky format first (simpler)
                height = ''
                hometown = ''
                high_school = ''
                previous_school = ''
                
                # Kentucky format: <p class="roster-item__info">
                info_p = item.find('p', class_='roster-item__info')
                if info_p:
                    info_text = info_p.get_text(strip=True)
                    # Format: "Goalkeeper - 6'4" 185 lbs" or similar
                    # Extract position and dimensions
                    if '-' in info_text:
                        parts = info_text.split('-')
                        if not position and len(parts) > 0:
                            position = FieldExtractors.extract_position(parts[0].strip())
                        if len(parts) > 1:
                            # Parse dimensions: "6'4" 185 lbs" or "6'4" 185"
                            dims = parts[1].strip()
                            height = FieldExtractors.extract_height(dims)
                else:
                    # Virginia format: Description div has <p> tags
                    description_div = item.find('div', class_='roster__description')
                    if description_div:
                        p_tags = description_div.find_all('p')
                        
                        # First <p>: height / weight / year
                        if len(p_tags) > 0:
                            first_p = p_tags[0].get_text(strip=True)
                            # Split by / to get components
                            parts = [p.strip() for p in first_p.split('/')]
                            if len(parts) >= 1:
                                # First part should be height
                                height = FieldExtractors.extract_height(parts[0])
                            # Weight is parts[1], we skip it
                            if len(parts) >= 3:
                                # Third part is year
                                year_candidate = FieldExtractors.normalize_academic_year(parts[2])
                                if year_candidate:
                                    year = year_candidate
                        
                        # Second <p>: hometown / high school or club / previous school
                        if len(p_tags) > 1:
                            second_p = p_tags[1].get_text(strip=True)
                            # Remove social media links
                            second_p = re.sub(r'@\w+.*$', '', second_p).strip()
                            # Split by /
                            location_parts = [p.strip() for p in second_p.split('/') if p.strip()]
                            
                            if len(location_parts) >= 1:
                                hometown = location_parts[0]
                            if len(location_parts) >= 2:
                                # Could be high school or club
                                high_school = location_parts[1]
                            if len(location_parts) >= 3:
                                # Previous school/college
                                previous_school = location_parts[2]

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major='',  # Not available in WMT format
                    hometown=hometown,
                    high_school=high_school,
                    previous_school=previous_school,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing WMT roster item in {team_name}: {e}")
                continue

        return players

    def _enhance_kentucky_player_data(self, players: List[Player]) -> List[Player]:
        """
        Enhance Kentucky player data by fetching individual bio pages
        
        Kentucky's roster page doesn't include hometown, high school, class, or URLs.
        This method fetches those details from individual player bio pages.
        
        Args:
            players: List of Player objects to enhance
            
        Returns:
            Enhanced list of Player objects with additional data
        """
        if not players or len(players) == 0:
            return players
        
        # Check if this is Kentucky based on first player's URL pattern
        if not players[0].url or 'ukathletics.com' not in players[0].url:
            return players
        
        enhanced_players = []
        
        for player in players:
            try:
                if not player.url:
                    enhanced_players.append(player)
                    continue
                
                # Fetch individual bio page
                response = requests.get(player.url, timeout=10)
                if response.status_code != 200:
                    enhanced_players.append(player)
                    continue
                
                bio_html = BeautifulSoup(response.text, 'html.parser')
                
                # Extract player info from bio page
                # Looking for: <span class="player-info__label">Hometown</span>
                #              <span class="player-info__value">...</span>
                
                info_list = bio_html.find('ul', class_='player-info__list')
                if info_list:
                    items = info_list.find_all('li')
                    for item in items:
                        label_span = item.find('span', class_='player-info__label')
                        value_span = item.find('span', class_='player-info__value')
                        
                        if label_span and value_span:
                            label = label_span.get_text().strip().lower()
                            value = value_span.get_text().strip()
                            
                            if 'hometown' in label:
                                player.hometown = value
                            elif 'high school' in label:
                                player.high_school = value
                            elif 'class' in label:
                                player.year = FieldExtractors.normalize_academic_year(value)
                
                enhanced_players.append(player)
                
            except Exception as e:
                logger.debug(f"Could not enhance Kentucky player {player.name}: {e}")
                enhanced_players.append(player)
        
        return enhanced_players

    def _enhance_sidearm_positions_from_bios(self, players: List[Player], team_name: str) -> List[Player]:
        """
        Enhance Sidearm player data by fetching positions from individual bio pages

        Some Sidearm sites (e.g., Babson, Brandeis, Bentley) don't include position data
        on the roster list page, but do have it on individual player bio pages.

        Args:
            players: List of Player objects to enhance
            team_name: Team name for logging

        Returns:
            Enhanced list of Player objects with position data from bio pages
        """
        if not players or len(players) == 0:
            return players

        enhanced_players = []
        missing_count = sum(1 for p in players if not p.position)

        if missing_count == 0:
            return players  # No need to enhance if all players have positions

        logger.info(f"Enhancing {missing_count} players with bio page position data for {team_name}...")

        for player in players:
            try:
                # Skip if player already has position or no URL
                if player.position or not player.url:
                    enhanced_players.append(player)
                    continue

                # Fetch individual bio page
                response = self.session.get(player.url, headers=self.headers, timeout=10)
                if response.status_code != 200:
                    enhanced_players.append(player)
                    continue

                bio_html = BeautifulSoup(response.text, 'html.parser')

                # Extract position from bio page
                # Looking for: <span class="sidearm-roster-player-field-label">Position</span>
                #              <span>M</span> (or other position value)
                # Often wrapped in a div structure

                # Find all field labels
                labels = bio_html.find_all('span', class_='sidearm-roster-player-field-label')
                for label in labels:
                    label_text = label.get_text().strip().lower()
                    if 'position' in label_text:
                        # Get the parent div, then find the value span
                        parent = label.find_parent('div')
                        if parent:
                            # Find span that is NOT the label span
                            spans = parent.find_all('span')
                            for span in spans:
                                if 'sidearm-roster-player-field-label' not in span.get('class', []):
                                    position_text = span.get_text().strip()
                                    player.position = FieldExtractors.extract_position(position_text)
                                    if player.position:
                                        logger.debug(f"Found position {player.position} for {player.name}")
                                    break
                        if player.position:
                            break

                enhanced_players.append(player)

            except Exception as e:
                logger.debug(f"Could not enhance player {player.name}: {e}")
                enhanced_players.append(player)

        enhanced_count = sum(1 for p in enhanced_players if p.position) - (len(players) - missing_count)
        logger.info(f"Enhanced {enhanced_count} of {missing_count} missing positions from bio pages")

        return enhanced_players

    def _extract_players_from_wordpress_roster(self, html, team_id: int, team_name: str, season: str, division: str, base_url: str) -> List[Player]:
        """
        Extract players from custom WordPress roster format (person__item divs)

        Used by schools like Clemson that use WordPress with custom CSS classes
        <li class="person__item"> structure containing person__image, person__info, person__meta divs

        Args:
            html: BeautifulSoup parsed HTML
            team_id: NCAA team ID
            team_name: Team name
            season: Season string
            division: Division
            base_url: Base URL for constructing profile URLs

        Returns:
            List of Player objects
        """
        players = []

        # Find all roster items
        roster_items = html.find_all('li', class_='person__item')

        # Extract base domain for URLs
        extracted = tldextract.extract(base_url)
        domain = f"{extracted.domain}.{extracted.suffix}"
        if extracted.subdomain:
            domain = f"{extracted.subdomain}.{domain}"

        for item in roster_items:
            try:
                # Jersey number - in span.person__number > span, text like "#0" or "#1"
                # NOTE: Players have jersey numbers, coaches don't - use this to filter
                jersey = ''
                jersey_span = item.find('span', class_='person__number')
                if jersey_span:
                    jersey_text = jersey_span.get_text().strip()
                    # Extract just the number from "#0" or "#1"
                    jersey = re.sub(r'[^\d]', '', jersey_text)
                else:
                    # Skip if no jersey number (likely a coach)
                    continue

                # Name - in a.custom-value with data-custom-value attribute
                name = ''
                name_elem = item.find('a', class_='custom-value')
                if name_elem and name_elem.get('data-custom-value'):
                    name = name_elem['data-custom-value'].strip()

                if not name:
                    continue

                # Profile URL - in a.custom-value[href]
                profile_url = ''
                if name_elem and name_elem.get('href'):
                    href = name_elem['href']
                    if href.startswith('http'):
                        profile_url = href
                    else:
                        profile_url = f"https://{domain}{href}" if href.startswith('/') else f"https://{domain}/{href}"

                # Height and weight - in div.person__subtitle with two span.person__value
                height = ''
                weight = ''
                subtitle_div = item.find('div', class_='person__subtitle')
                if subtitle_div:
                    value_spans = subtitle_div.find_all('span', class_='person__value')
                    if len(value_spans) > 0:
                        height = FieldExtractors.extract_height(value_spans[0].get_text().strip())
                    if len(value_spans) > 1:
                        weight = value_spans[1].get_text().strip()

                # Extract metadata from person__meta divs with meta__row structure
                position = ''
                year = ''
                hometown = ''
                major = ''
                
                meta_div = item.find('div', class_='person__meta')
                if meta_div:
                    meta_rows = meta_div.find_all('div', class_='meta__row')
                    
                    for row in meta_rows:
                        name_elem = row.find('div', class_='meta__name')
                        value_elem = row.find('div', class_='meta__value')
                        
                        if name_elem and value_elem:
                            field_name = name_elem.get_text().strip().lower().rstrip(':')
                            field_value = value_elem.get_text().strip()
                            
                            # Remove any links from field_value (e.g., Instagram, Twitter links)
                            links = value_elem.find_all('a')
                            if links:
                                # Just get the text content, skipping link text
                                field_value = value_elem.get_text().strip()
                                # For social links, extract just the handle
                                if '@' in field_value:
                                    field_value = field_value  # Keep as is for display
                                else:
                                    # Remove "Opens in a new window" type text
                                    field_value = re.sub(r'\s*Opens in a new window.*$', '', field_value)
                            
                            if field_name == 'position':
                                position = FieldExtractors.extract_position(field_value)
                            elif field_name == 'year':
                                year = FieldExtractors.normalize_academic_year(field_value)
                            elif field_name == 'hometown':
                                hometown = field_value
                            elif field_name == 'major':
                                major = field_value

                # Create Player object
                player = Player(
                    team_id=team_id,
                    team=team_name,
                    season=season,
                    division=division,
                    name=name,
                    jersey=jersey,
                    position=position,
                    height=height,
                    year=year,
                    major=major,
                    hometown=hometown,
                    url=profile_url
                )

                players.append(player)

            except Exception as e:
                logger.warning(f"Error parsing WordPress roster item in {team_name}: {e}")
                continue

        return players


# ============================================================================
# JAVASCRIPT SCRAPER
# ============================================================================

class JSScraper(StandardScraper):
    """Scraper for JavaScript-rendered Sidearm sites using shot-scraper"""

    def __init__(self, session: Optional[requests.Session] = None):
        """
        Initialize JS scraper

        Args:
            session: Optional requests Session (not used, kept for compatibility)
        """
        super().__init__(session)

    def _fetch_html_with_javascript(self, url: str, timeout: int = 45) -> Optional[BeautifulSoup]:
        """
        Fetch HTML with JavaScript rendering using shot-scraper

        Args:
            url: URL to fetch
            timeout: Timeout in seconds (default 45)

        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            # Use shot-scraper via uv to render JavaScript
            result = subprocess.run(
                ['uv', 'run', 'shot-scraper', 'html', url, '--wait', '3000'],
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                return BeautifulSoup(result.stdout, 'html.parser')
            else:
                logger.warning(f"shot-scraper returned code {result.returncode}: {result.stderr}")
                return None

        except subprocess.TimeoutExpired:
            logger.warning(f"shot-scraper timeout after {timeout}s for {url}")
            return None
        except FileNotFoundError:
            logger.error("shot-scraper or uv not found. Install with: uv sync")
            return None
        except Exception as e:
            logger.error(f"Error running shot-scraper: {e}")
            return None

    def _validate_players(self, players: List[Player], team_name: str) -> bool:
        """
        Validate that scraped players have sufficient data completeness

        Args:
            players: List of Player objects
            team_name: Team name for logging

        Returns:
            True if data is valid, False otherwise
        """
        if not players:
            return False

        # Check essential fields (name, jersey, position, year)
        # previous_school and major are optional
        essential_fields = ['name', 'jersey', 'position', 'year']

        field_counts = {field: 0 for field in essential_fields}
        total = len(players)

        for player in players:
            if player.name:
                field_counts['name'] += 1
            if player.jersey:
                field_counts['jersey'] += 1
            if player.position:
                field_counts['position'] += 1
            if player.year:
                field_counts['year'] += 1

        # Calculate coverage percentages
        coverage = {field: (count / total * 100) for field, count in field_counts.items()}

        logger.info(
            f"Validation coverage for {team_name}: name={coverage['name']:.1f}%, jersey={coverage['jersey']:.1f}%, "
            f"pos={coverage['position']:.1f}%, year={coverage['year']:.1f}% (total={total})"
        )

        # Require at least 80% coverage for name and jersey
        # Require at least 70% coverage for position and year
        if coverage['name'] < 80:
            logger.warning(f"Validation failed for {team_name}: name coverage {coverage['name']:.1f}% < 80%")
            return False
        if coverage['jersey'] < 80:
            # Relax jersey threshold for list-item rosters with sufficient size
            if total >= 15 and coverage['jersey'] >= 50:
                logger.info(
                    f"Relaxed jersey validation for {team_name}: jersey={coverage['jersey']:.1f}%"
                )
            else:
                logger.warning(f"Validation failed for {team_name}: jersey coverage {coverage['jersey']:.1f}% < 80%")
                return False
        if coverage['position'] < 70 or coverage['year'] < 70:
            # Relax validation when we still have a substantial roster size
            # Some Sidearm list-item rosters omit year or position on the list view.
            if total >= 15 and (coverage['position'] >= 50 or coverage['year'] >= 50):
                logger.info(
                    f"Relaxed validation for {team_name}: pos={coverage['position']:.1f}%, year={coverage['year']:.1f}%"
                )
            else:
                # Final fallback: accept large rosters with strong name/jersey coverage
                if total >= 15 and coverage['name'] >= 80 and coverage['jersey'] >= 50:
                    logger.info(
                        f"Fallback validation for {team_name}: accepting roster with name/jersey coverage"
                    )
                else:
                    if coverage['position'] < 70:
                        logger.warning(f"Validation failed for {team_name}: position coverage {coverage['position']:.1f}% < 70%")
                    if coverage['year'] < 70:
                        logger.warning(f"Validation failed for {team_name}: year coverage {coverage['year']:.1f}% < 70%")
                    return False

        logger.info(f"✓ Validation passed for {team_name}: name={coverage['name']:.0f}%, jersey={coverage['jersey']:.0f}%, pos={coverage['position']:.0f}%, year={coverage['year']:.0f}%")
        return True

    def _build_season_range_url(self, base_url: str, season: str) -> str:
        """
        Build alternative URL using season-range format (e.g., 2025-26)

        Some schools use this pattern instead of /roster/2025:
        - https://ccsubluedevils.com/sports/wsoc/2025-26/roster

        Args:
            base_url: Base URL from teams.csv
            season: Season string (e.g., '2025')

        Returns:
            Alternative roster URL with season range
        """
        base_url = base_url.rstrip('/')

        # Remove /index suffix if present (e.g., /sports/wsoc/index -> /sports/wsoc)
        if base_url.endswith('/index'):
            base_url = base_url[:-6]  # Remove '/index'

        # Convert single year to range (2025 -> 2025-26)
        try:
            year = int(season)
            next_year = str(year + 1)[-2:]  # Get last 2 digits
            season_range = f"{year}-{next_year}"
            return f"{base_url}/{season_range}/roster"
        except ValueError:
            # If season is already a range or invalid format, return as-is
            logger.warning(f"Could not parse season '{season}' for range URL")
            return f"{base_url}/{season}/roster"

    def scrape_team(self, team_id: int, team_name: str, base_url: str, season: str, division: str = "") -> List[Player]:
        """
        Scrape roster for a single team using shot-scraper for JavaScript rendering

        Args:
            team_id: NCAA team ID
            team_name: Team name
            base_url: Base URL for team site
            season: Season string (e.g., '2025')
            division: Division ('I', 'II', 'III')

        Returns:
            List of Player objects
        """
        try:
            # Build roster URL
            url_format = TeamConfig.get_url_format(team_id, base_url)
            roster_url = URLBuilder.build_roster_url(base_url, season, url_format)

            logger.info(f"Scraping {team_name} (JS) - {roster_url}")

            # Use shot-scraper to render JavaScript
            html = self._fetch_html_with_javascript(roster_url, timeout=45)

            if html is None:
                logger.warning(f"Failed to fetch HTML for {team_name}, trying standard fetch...")
                # Fallback to standard requests
                response = self.session.get(roster_url, headers=self.headers, timeout=30)
                if response.status_code == 200:
                    html = BeautifulSoup(response.text, 'html.parser')
                else:
                    logger.error(f"Standard fetch also failed for {team_name}: {response.status_code}")
                    return []

            # Verify season (optional, less strict for JS sites)
            if not SeasonVerifier.verify_season_on_page(html, season):
                logger.warning(f"Season mismatch for {team_name}")

            # Extract players using parent class method
            players = self._extract_players(html, team_id, team_name, season, division, base_url)

            # If we found players, do not try alternative URL patterns
            if players:
                logger.info(f"✓ {team_name}: Found {len(players)} players")
                return players

            # Validate data completeness
            if not self._validate_players(players, team_name):
                logger.warning(f"✗ {team_name}: Data validation failed, trying alternative URL patterns...")

                # Try alternative URL pattern: season-range format (e.g., 2025-26)
                alternative_url = self._build_season_range_url(base_url, season)
                if alternative_url != roster_url:
                    logger.info(f"Trying alternative URL: {alternative_url}")
                    html_alt = self._fetch_html_with_javascript(alternative_url, timeout=45)

                    if html_alt is not None:
                        players_alt = self._extract_players(html_alt, team_id, team_name, season, division, base_url)
                        if self._validate_players(players_alt, team_name):
                            logger.info(f"✓ {team_name}: Found {len(players_alt)} players (alternative URL)")
                            return players_alt

                logger.warning(f"✗ {team_name}: All URL patterns failed validation")
                return []

            logger.info(f"✓ {team_name}: Found {len(players)} players (validated)")
            return players

        except Exception as e:
            logger.error(f"Error scraping {team_name}: {e}")
            return []


# ============================================================================
# ROSTER MANAGER
# ============================================================================

class RosterManager:
    """Manages batch scraping of rosters with error tracking"""

    def __init__(self, season: str = '2025', output_dir: str = 'data/raw'):
        """
        Initialize RosterManager

        Args:
            season: Season string (e.g., '2025')
            output_dir: Base output directory
        """
        self.season = season
        self.output_dir = Path(output_dir)
        self.scraper = StandardScraper()

        # Error tracking
        self.zero_player_teams = []
        self.failed_teams = []
        self.successful_teams = []

    def load_teams(self, csv_path: str, division: Optional[str] = None) -> List[Dict]:
        """
        Load teams from CSV, optionally filtered by division

        Args:
            csv_path: Path to teams.csv
            division: Optional division filter ('I', 'II', 'III')

        Returns:
            List of team dictionaries
        """
        teams = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Handle missing division column (women's soccer CSV may not have it)
                if division is None or row.get('division', '') == division:
                    teams.append(row)

        logger.info(f"Loaded {len(teams)} teams" + (f" (Division {division})" if division else ""))
        return teams

    def scrape_teams(self, teams: List[Dict], max_teams: Optional[int] = None) -> List[Player]:
        """
        Scrape rosters for multiple teams

        Args:
            teams: List of team dictionaries from CSV
            max_teams: Optional limit on number of teams to scrape

        Returns:
            List of all Player objects
        """
        all_players = []
        teams_to_scrape = teams[:max_teams] if max_teams else teams

        logger.info(f"Starting scrape of {len(teams_to_scrape)} teams")
        logger.info("=" * 80)

        for i, team in enumerate(teams_to_scrape, 1):
            # Handle both 'team_id' and 'ncaa_id' column names
            team_id = int(team.get('team_id') or team.get('ncaa_id'))
            team_name = team['team']
            team_url = team['url']
            # Handle missing division column (women's soccer CSV may not have it)
            division = team.get('division', '')

            logger.info(f"[{i}/{len(teams_to_scrape)}] {team_name}" + (f" (Division {division})" if division else ""))

            try:
                # Choose scraper based on team requirements
                if TeamConfig.requires_javascript(team_id):
                    scraper = JSScraper()
                    logger.info(f"  Using JSScraper (JavaScript rendering)")
                else:
                    scraper = self.scraper  # Use the StandardScraper instance

                players = scraper.scrape_team(
                    team_id=team_id,
                    team_name=team_name,
                    base_url=team_url,
                    season=self.season,
                    division=division
                )

                if len(players) == 0:
                    logger.warning(f"  ⚠️  Zero players found")
                    self.zero_player_teams.append({
                        'team': team_name,
                        'ncaa_id': team_id,
                        'division': division,
                        'url': team_url
                    })
                else:
                    logger.info(f"  ✓ {len(players)} players")
                    all_players.extend(players)
                    self.successful_teams.append({
                        'team': team_name,
                        'ncaa_id': team_id,
                        'division': division,
                        'player_count': len(players)
                    })

            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                self.failed_teams.append({
                    'team': team_name,
                    'ncaa_id': team_id,
                    'division': division,
                    'url': team_url,
                    'error': str(e)
                })

        logger.info("=" * 80)
        logger.info(f"Scraping complete:")
        logger.info(f"  Successful: {len(self.successful_teams)} teams, {len(all_players)} players")
        logger.info(f"  Zero players: {len(self.zero_player_teams)} teams")
        logger.info(f"  Failed: {len(self.failed_teams)} teams")

        return all_players

    def save_results(self, players: List[Player], division: Optional[str] = None):
        """
        Save results to JSON and CSV, plus error reports

        Args:
            players: List of Player objects
            division: Optional division for filename
        """
        # Determine filenames
        div_suffix = f"_{division}" if division else ""
        json_file = self.output_dir / 'json' / f'rosters_wsoc_{self.season}{div_suffix}.json'
        csv_file = self.output_dir / 'csv' / f'rosters_wsoc_{self.season}{div_suffix}.csv'

        # Create directories
        json_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.parent.mkdir(parents=True, exist_ok=True)

        # Save JSON
        players_dicts = [p.to_dict() for p in players]
        with open(json_file, 'w') as f:
            json.dump(players_dicts, f, indent=2)
        logger.info(f"✓ Saved JSON: {json_file} ({len(players)} players)")

        # Save CSV
        if players_dicts:
            fieldnames = players_dicts[0].keys()
            with open(csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(players_dicts)
            logger.info(f"✓ Saved CSV: {csv_file}")

        # Save error reports
        self._save_error_reports(division)

    def _save_error_reports(self, division: Optional[str] = None):
        """Save error reports for zero player and failed teams"""
        div_suffix = f"_{division}" if division else ""

        # Zero player teams
        if self.zero_player_teams:
            zero_file = self.output_dir / 'csv' / f'rosters_wsoc_{self.season}{div_suffix}_zero_players.csv'
            with open(zero_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['team', 'ncaa_id', 'division', 'url'])
                writer.writeheader()
                writer.writerows(self.zero_player_teams)
            logger.warning(f"⚠️  Saved zero-player report: {zero_file} ({len(self.zero_player_teams)} teams)")

        # Failed teams
        if self.failed_teams:
            failed_file = self.output_dir / 'csv' / f'rosters_wsoc_{self.season}{div_suffix}_failed.csv'
            with open(failed_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['team', 'ncaa_id', 'division', 'url', 'error'])
                writer.writeheader()
                writer.writerows(self.failed_teams)
            logger.error(f"✗ Saved failure report: {failed_file} ({len(self.failed_teams)} teams)")


# ============================================================================
# MAIN ENTRY POINT (Placeholder for now)
# ============================================================================

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='NCAA Women\'s Soccer Roster Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all Division I teams
  python src/wsoccer_roster_scraper.py --division I --season 2025

  # Scrape first 10 Division II teams (testing)
  python src/wsoccer_roster_scraper.py --division II --limit 10 --season 2025

  # Scrape all teams
  python src/wsoccer_roster_scraper.py --season 2025

  # Scrape specific team
  python src/wsoccer_roster_scraper.py --team 14 --season 2025
        """
    )

    parser.add_argument(
        '--season',
        default='2025',
        help='Season year (default: 2025)'
    )

    parser.add_argument(
        '--division',
        choices=['I', 'II', 'III'],
        help='Filter by division (I, II, or III)'
    )

    parser.add_argument(
        '--team',
        type=int,
        help='Scrape specific team by NCAA ID'
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of teams to scrape (for testing)'
    )

    parser.add_argument(
        '--teams-csv',
        default='data/input/teams_wsoc.csv',
        help='Path to teams.csv (default: data/input/teams_wsoc.csv)'
    )

    parser.add_argument(
        '--output-dir',
        default='data/raw',
        help='Output directory (default: data/raw)'
    )

    args = parser.parse_args()

    # Initialize manager
    manager = RosterManager(season=args.season, output_dir=args.output_dir)

    # Load teams
    if args.team:
        # Scrape specific team
        teams = manager.load_teams(args.teams_csv)
        teams = [t for t in teams if int(t['team_id']) == args.team]
        if not teams:
            logger.error(f"Team {args.team} not found in {args.teams_csv}")
            return
        logger.info(f"Scraping specific team: {teams[0]['team']}")
    else:
        # Load all teams, optionally filtered by division
        teams = manager.load_teams(args.teams_csv, division=args.division)

    if not teams:
        logger.error("No teams to scrape")
        return

    # Scrape teams
    players = manager.scrape_teams(teams, max_teams=args.limit)

    # Save results
    if players:
        manager.save_results(players, division=args.division)
    else:
        logger.warning("No players scraped - no output files generated")

    # Summary
    print("\n" + "=" * 80)
    print("SCRAPING SUMMARY")
    print("=" * 80)
    print(f"Season: {args.season}")
    print(f"Teams attempted: {len(teams) if not args.limit else min(len(teams), args.limit)}")
    print(f"Successful: {len(manager.successful_teams)} teams")
    print(f"Total players: {len(players)}")
    print(f"Zero players: {len(manager.zero_player_teams)} teams")
    print(f"Failed: {len(manager.failed_teams)} teams")
    print("=" * 80)


if __name__ == '__main__':
    main()
