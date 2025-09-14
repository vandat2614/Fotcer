import re
import unicodedata

from bs4 import Tag, BeautifulSoup
from typing import Dict, Any, Optional, List

from src.constants import (
    EVENT_TYPE_MAPPING, 
    MATCH_EVENT_HEADERS, 
    OFFICIALS_ROLE_MAPPING,
    EVENT_ICON_CLASS, YELLOW_CARD_CLASS, RED_CARD_CLASS,
    TEAM_LOGO_CLASS, SCORE_CLASS, SCORE_PENALTY_CLASS, SCORE_AGGREGATE_CLASS, DATAPOINT_CLASS, SCOREBOX_META_CLASS, VENUETIME_CLASS,
    LINEUP_CLASS, EVENT_HEADER_CLASS, EVENT_A_CLASS, EVENT_B_CLASS
)

def parse_event(div: Tag) -> Dict[str, Optional[Any]]:
    """Parses a single match event div and extracts its details."""
    event_data: Dict[str, Optional[Any]] = {
        'time': None, 'score': None, 'event': None, 
        'in': None, 'out': None, 'scorer': None, 'assist': None, 'player': None
    }

    header_div = div.find('div')    
    if header_div:
        raw_text = header_div.get_text(strip=True)
        if '’' in raw_text:
            event_data['time'] = raw_text.split('’')[0].strip() + '’'
        score_span = header_div.find('span')
        if score_span:
            event_data['score'] = score_span.get_text(strip=True)

    icon_div = div.find('div', class_=EVENT_ICON_CLASS)
    if icon_div:
        # Find the specific event class (e.g., 'substitute_in')
        event_class = next((cls for cls in icon_div.get('class', []) if cls != EVENT_ICON_CLASS), None)
        if event_class:
            event_data['event'] = EVENT_TYPE_MAPPING.get(event_class, event_class.replace('_', ' '))
    
    player_links = div.find_all('a')

    if event_data['event'] == 'substitute':
        event_data['in'] = player_links[0].get_text(strip=True) if len(player_links) >= 1 else None
        event_data['out'] = player_links[1].get_text(strip=True) if len(player_links) >= 2 else None
    elif event_data['event'] in ['goal', 'penalty goal']:
        event_data['scorer'] = player_links[0].get_text(strip=True) if len(player_links) >= 1 else None
        event_data['assist'] = player_links[1].get_text(strip=True) if len(player_links) >= 2 else None
    elif player_links:
        event_data['player'] = player_links[0].get_text(strip=True)

    return {k: v for k, v in event_data.items() if v is not None} # Return only non-None values


def _parse_stat_value(td: Tag, stat_name: str) -> Any:
    """Helper to parse individual stat values from a table cell."""
    if stat_name.lower() == 'cards':
        return {
            'yellow': len(td.select(f'.{YELLOW_CARD_CLASS}')),
            'red': len(td.select(f'.{RED_CARD_CLASS}')),
        }
    else:
        text = td.get_text(' ', strip=True)
        strong = td.find('strong')
        percent = strong.get_text(strip=True) if strong else None

        match = re.search(r'(\d+)\s+of\s+(\d+)', text)
        if match:
            made, total = match.groups()
            return {
                'success': int(made),
                'total': int(total), # Changed 'made' to 'total' for clarity
                'percent': percent
            }
        else:
            return percent or text

def parse_team_basic_stats(div: Tag) -> Dict[str, Dict[str, Any]]:
    """Parses basic team statistics from the main 'team_stats' table."""
    table = div.find('table')
    if not table:
        return {}

    team_cells = table.find('tr').find_all('th')
    # Ensure there are at least two team cells for home/away
    if len(team_cells) < 2: 
        return {}
    teams = [team_cells[0].get_text(strip=True), team_cells[1].get_text(strip=True)]

    result = {team: {} for team in teams}

    rows = table.find_all('tr')[1:] 
    current_stat: Optional[str] = None

    for row in rows:
        th = row.find('th')
        if th:
            current_stat = th.get_text(strip=True)
        else:
            tds = row.find_all('td')
            if not current_stat or len(tds) < 2:
                continue

            # Parse values for home and away teams
            result[teams[0]][current_stat] = _parse_stat_value(tds[0], current_stat)
            result[teams[1]][current_stat] = _parse_stat_value(tds[1], current_stat)

    return result

def parse_team_extra_stats(extra_div: Tag) -> Dict[str, Dict[str, Any]]:
    """Parses extra team statistics from the 'team_stats_extra' section."""
    result: Dict[str, Dict[str, Any]] = {}
    
    blocks = extra_div.find_all('div', recursive=False)
    for block in blocks:
        header_elements = [h.get_text(strip=True) for h in block.select('div.th') if h.get_text(strip=True)]
        if len(header_elements) < 2:
            continue
        team1, team2 = header_elements[0], header_elements[-1]

        result.setdefault(team1, {})
        result.setdefault(team2, {})

        # Find all data cells, excluding the header cells (div.th)
        data_cells = [d.get_text(' ', strip=True) for d in block.find_all('div') if 'th' not in d.get('class', [])]

        # Iterate through the cells in groups of 3 (val1, stat_name, val2)
        for i in range(0, len(data_cells), 3):
            if i + 2 >= len(data_cells):
                break
            val1_str, stat_name, val2_str = data_cells[i:i+3]

            def _convert_to_number(s: str) -> Optional[Any]:
                if not s:
                    return None
                s = s.split()[0]  # Take only the first part if there are units
                s = s.replace(',', '')
                try:
                    return int(s)
                except ValueError:
                    try:
                        return float(s) # Handle float values like percentages
                    except ValueError:
                        return s # Return string if not a number

            result[team1][stat_name] = _convert_to_number(val1_str)
            result[team2][stat_name] = _convert_to_number(val2_str)

    return result

def get_match_lineups(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extracts match lineups (starting XI and bench) for both teams."""
    divs = soup.find_all('div', class_=LINEUP_CLASS)
    result = {}

    for div in divs:
        table = div.find('table')
        if not table: 
            continue

        header = table.find('tr').get_text(strip=True)
        team_name: str
        formation: Optional[str]

        match = re.search(r'^(.*?)\s*\((.*?)\)$', header)
        if match:
            team_name = match.group(1).strip()
            formation = match.group(2).strip()
        else:
            team_name = header.strip()
            formation = None

        lineup = {'formation': formation, 'starting': [], 'bench': []}
        section = 'starting' 

        for row in table.find_all('tr')[1:]:
            th = row.find('th')
            if th and 'bench' in th.get_text(strip=True).lower():
                section = 'bench'
                continue

            cells = row.find_all('td')
            if len(cells) == 2:
                number = cells[0].get_text(strip=True)
                name = cells[1].get_text(strip=True)
                lineup[section].append({'number': number, 'name': name})

        result[team_name] = lineup
    return result

def get_match_stats(soup: BeautifulSoup) -> Dict[str, Any]:
    """Combines basic and extra team statistics for a match."""
    basic_stats_div = soup.find('div', id='team_stats')
    extra_stats_div = soup.find('div', id='team_stats_extra')

    stats = parse_team_basic_stats(basic_stats_div) if basic_stats_div else {}
    extra_stats = parse_team_extra_stats(extra_stats_div) if extra_stats_div else {}

    # Merge stats, prioritizing extra_stats if there are overlaps
    all_stats: Dict[str, Dict[str, Any]] = {}
    for team in set(stats.keys()) | set(extra_stats.keys()):
        all_stats[team] = {
            **stats.get(team, {}), 
            **extra_stats.get(team, {})
        }
    return all_stats

def get_match_events(soup: BeautifulSoup, home_team: str, away_team: str)  -> Dict[str, Any]:
    """Extracts all match events, categorized by half/period."""
    events_divs = soup.find_all('div', class_=[EVENT_HEADER_CLASS, EVENT_A_CLASS, EVENT_B_CLASS])

    result: Dict[str, List[Dict[str, Any]]] = {}
    current_header: Optional[str] = None

    for div in events_divs:
        classes = div.get('class', [])

        if EVENT_HEADER_CLASS in classes:
            header_text = div.get_text(strip=True)
            current_header = MATCH_EVENT_HEADERS.get(header_text, header_text) # Use mapping for headers
            result[current_header] = []
        else:
            if current_header is None:
                continue # Skip events before the first header

            event_data = parse_event(div)
            if event_data:
                event_data['team'] = home_team if EVENT_A_CLASS in classes else away_team

                # Special handling for 'Penalty' events (no time usually)
                if current_header == MATCH_EVENT_HEADERS['Penalty Shootout'] and 'time' in event_data:
                    del event_data['time']
                result[current_header].append(event_data)

    return result

# --- Helper functions for get_match_info ---
def _normalize_text(s: str) -> str:
    """Normalizes Unicode text."""
    return unicodedata.normalize('NFKC', s).strip()

def _parse_teams_and_logos(soup: BeautifulSoup, match_info: Dict[str, Any]) -> None:
    """Parses team names and logos."""
    img_divs = soup.find_all("img", class_=TEAM_LOGO_CLASS)[:2]
    for i, div in enumerate(img_divs):
        team_name = _normalize_text(div.get("alt", "")).rsplit(maxsplit=2)[0]
        team_logo = div.get("src")
        side = "home" if i == 0 else "away"
        match_info["teams"][side] = {"name": team_name, "logo_url": team_logo}

def _parse_scores(soup: BeautifulSoup, match_info: Dict[str, Any]) -> None:
    """Parses match scores."""

    mapping = {SCORE_CLASS : 'scores', SCORE_AGGREGATE_CLASS : 'aggregate', SCORE_PENALTY_CLASS : 'penalties'}
    for cls in mapping.keys():
        score_divs = soup.find_all("div", class_=cls)
        scores = [int(div.get_text()) for div in score_divs if div.get_text().isdigit()]
        if len(scores) == 2:
            name = mapping[cls]
            match_info[name] = {"home": scores[0], "away": scores[1]}

def _parse_managers_and_captains(soup: BeautifulSoup, match_info: Dict[str, Any]) -> None:
    """Parses managers and captains for both teams."""
    datapoints = soup.find_all("div", class_=DATAPOINT_CLASS)
    for i, div in enumerate(datapoints):
        text = _normalize_text(div.get_text())
        if ":" not in text:
            continue
        label, value = [part.strip() for part in text.split(":", 1)]
        role = label.lower()
        side = "home" if i <= 1 else "away"
        match_info["teams"].setdefault(side, {})[role] = value

def _parse_metadata_block(meta_block: Tag, match_info: Dict[str, Any]) -> None:
    """Parses the main metadata block (date, time, competition, attendance, venue, officials)."""
    rows = meta_block.find_all("div")
    
    # Adjust for cases where attendance might be missing (insert a placeholder None)
    if len(rows) == 6: 
        rows.insert(4, None) 

    # Date & time
    venue_time = rows[0].find("span", class_=VENUETIME_CLASS)
    if venue_time:
        match_info["datetime"] = {
            "date": venue_time.get("data-venue-date"),
            "time": venue_time.get("data-venue-time"),
        }
    else: # Fallback if venuetime span is not found
        date_link = rows[0].find('a')
        if date_link and date_link.get('href'):
            date_str = date_link.get('href').split('/')[-1]
            match_info["datetime"] = {'date': date_str}

    # Competition & stage
    if len(rows) > 1 and rows[1]:
        comp_text = _normalize_text(rows[1].get_text())
        if "(" in comp_text:
            comp, stage = comp_text.split("(", 1)
            match_info["competition"]["name"] = comp.strip()
            match_info["competition"]["stage"] = stage.replace(")", "").strip()

    # Attendance
    if len(rows) > 4 and rows[4]:
        attendance_text = _normalize_text(rows[4].get_text()).split(":")[-1].replace(",", "")
        if attendance_text.isdigit():
            match_info["attendance"] = int(attendance_text)

    # Venue (stadium & city)
    if len(rows) > 5 and rows[5]:
        venue_value = _normalize_text(rows[5].get_text()).split(":", 1)[-1]
        if "," in venue_value:
            stadium, city = [part.strip() for part in venue_value.split(",", 1)]
            match_info["venue"] = {"stadium": stadium, "city": city}
        else:
            match_info["venue"] = {"stadium": venue_value, "city": None}

    # Officials
    if len(rows) > 6 and rows[6]:
        spans = rows[6].find_all("span")
        for span in spans:
            info = _normalize_text(span.get_text())
            if "(" not in info:
                continue
            name, role = info.split("(", 1)
            role = role.replace(")", "").strip()
            role = OFFICIALS_ROLE_MAPPING.get(role, role.lower())
            match_info["officials"][role] = name.strip()

def get_match_info(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Aggregates all match information from a BeautifulSoup object,
    including teams, scores, competition details, venue, and officials.
    """
    match_info = {
        "teams": {},
        "scores": {"home" : None, "away" : None},
        "aggregate": {"home" : None, "away" : None},
        "penalties": {"home" : None, "away" : None},
        "competition": {},
        "officials": {},
        "datetime": {},
        "venue": {},
        "attendance": None
    }

    _parse_teams_and_logos(soup, match_info)
    _parse_scores(soup, match_info)
    _parse_managers_and_captains(soup, match_info)

    meta_block = soup.find("div", class_=SCOREBOX_META_CLASS)
    if meta_block:
        _parse_metadata_block(meta_block, match_info)

    return match_info