import re
import unicodedata

# Keep mappings small for now; you can extend these later.
ambiguous_mapping = {
    # e.g. "ascot-1m": "Straight-Right",
}

course_abbr_mapping = {
    # e.g. "MEY": "MEYDAN",
    # "MORPHETTVILLE PARKS": "MORPHETTVILLE",
    "CAULFIELD RACECOURSE": "CAULFIELD",
    "ROSEHILL": "ROSEHILL GARDENS",

}

COURSE_PREFIXES_TO_REMOVE = [
    "BET365 ",
    "LADBROKES",
    "SOUTHSIDE",
    "SPORTSBET",
    "SPORTSBET-",
    "PICKLEBET PARK"
]

# Keys are usually:
# - "course"
# - "course-distance"
# - "course-surface-distance"
direction_mapping = {
    # e.g. "matamata": "Right",
    "trentham": "Left",
    "riccarton park": "Left",
    "ashburton": "Left",
    "ellerslie": "Right",
    "pukehope": "Right",
    "pukehope park": "Right",
    "avondale": "Right",
    "wingatui": "Left",
    "cromwell": "Left",
    "hawera": "Left",
    "awapuni-dirt": "Left",
    "awapuni": "Left",
    "awapuni synthetic": "Left",
    "foxton": "Left",
    "gore": "Left",
    "omoto": "Left",
    "hastings": "Left",
    "flemington-1200": "Straight-Right",
    "flemington-1000": "Straight-Right",
    "flemington": "Right",
    "rosehill gardens": "Left",
    "ascot": "Left",
    "belmont": "Left",
    "canterbury": "Right",
    "caulfield": "Left",
    "doomben": "Right",
    "eagle farm": "Right",
    "kensington": "Right",
    
}


def _normalize_course_key(course: str) -> str:
    text = (course or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text.upper()


def normalize_course(course: str | None) -> str | None:
    if not course:
        return course
    key = _normalize_course_key(course)
    normalized = course_abbr_mapping.get(key, course.strip())
    for prefix in COURSE_PREFIXES_TO_REMOVE:
        normalized = re.sub(rf"^\s*{re.escape(prefix)}[\s-]*", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip(" -")
    return normalized


# --- copied logic pattern from era-scraper ---
def parse_distance(dist):
    dist = dist.replace(" ", "").lower()
    match = re.match(r"(?:(\d+)m)?(?:(\d+)f)?(?:(\d+)y)?", dist)
    miles = int(match.group(1)) if match and match.group(1) else 0
    furlongs = int(match.group(2)) if match and match.group(2) else 0
    yards = int(match.group(3)) if match and match.group(3) else 0
    return miles, furlongs, yards


def distance_to_yards(miles, furlongs, yards):
    return miles * 1760 + furlongs * 220 + yards


def get_direction(course: str, distance: str, surface: str = None) -> str:
    key = (
        f"{course.lower()}-{surface.lower()}-{distance.replace(' ', '').lower()}"
        if surface
        else f"{course.lower()}-{distance.replace(' ', '').lower()}"
    )
    if key in ambiguous_mapping:
        return ambiguous_mapping[key]
    if key in direction_mapping:
        return direction_mapping[key]
    key_no_surface = f"{course.lower()}-{distance.replace(' ', '').lower()}"
    if key_no_surface in direction_mapping:
        return direction_mapping[key_no_surface]

    def fuzzy_search(prefix, max_meter_diff=200):  # Only accept matches within 200 meters
        input_meters = int(distance)
        min_diff = float("inf")
        best_direction = None
        for k in direction_mapping:
            if k.startswith(prefix):
                key_dist_str = k[len(prefix):]
                try:
                    key_meters = int(key_dist_str)
                    diff = abs(key_meters - input_meters)
                    if diff < min_diff:
                        min_diff = diff
                        best_direction = direction_mapping[k]
                except ValueError:
                    continue
        if min_diff <= max_meter_diff:
            return best_direction
        return None

    if surface:
        prefix = f"{course.lower()}-{surface.lower()}-"
        best_direction = fuzzy_search(prefix)
        if best_direction:
            return best_direction
    prefix = f"{course.lower()}-"
    best_direction = fuzzy_search(prefix)
    if best_direction:
        return best_direction
    if course.lower() in direction_mapping:
        return direction_mapping[course.lower()]
    return "Left"
