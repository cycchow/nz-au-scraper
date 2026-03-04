from datetime import date

from loveracing import loveracing


SAMPLE_XML = """<?xml version=\"1.0\" encoding=\"utf-8\"?>
<meeting id=\"54910\" date=\"2026-02-28\" track=\"Matamata\" trackCondition=\"Soft\">
  <races>
    <race id=\"233460\" number=\"1\" type=\"Flat\" distance=\"1400\" class=\"MDN\" stake=\"25000.00\" time=\"12:10:00\">
      <runners>
        <runner id=\"451628\" toteNumber=\"7\" barrier=\"9\" name=\"Silent Spy (AUS)\" finishingposition=\"1\" actualtime=\"1.24.19\" trainer=\"Glenn Old\" startingPriceWin=\"6.70\">
          <jockey name=\"Jasmine Fawcett\" carried=\"58.00\" />
        </runner>
        <runner id=\"442113\" toteNumber=\"12\" barrier=\"6\" name=\"Solid Gold\" finishingposition=\"4\" actualtime=\"1.24.51\" trainer=\"Roger James\" startingPriceWin=\"2.50\">
          <jockey name=\"George Rooke\" carried=\"56.00\" />
        </runner>
      </runners>
    </race>
  </races>
</meeting>
"""


def test_build_result_download_url():
    url = loveracing.build_result_download_url(54910, "Race_54910.xml")
    assert "DayID=54910" in url
    assert "FileName=Race_54910.xml" in url


def test_parse_meeting_xml_race_and_result_mapping():
    fixture_ctx = {
        "raceDate": date(2026, 2, 28),
        "course": "Matamata",
        "meta": {"DayID": 54910},
    }

    def fake_sectionals(meeting_id: int, race_no: int):
        assert meeting_id == 54910
        assert race_no == 1
        return [
            {
                "horse_name": "SILENT SPY (AUS)",
                "cloth_number": 7,
                "sectionals": [
                    {"sector_number": 0, "sector_distance": 400, "sector_time": 24.1, "cumulative_sector_time": 24.1, "sector_position": 1},
                    {"sector_number": 1, "sector_distance": 800, "sector_time": 12.67, "cumulative_sector_time": 49.41, "sector_position": 7},
                    {"sector_number": 2, "sector_distance": 600, "sector_time": 12.75, "cumulative_sector_time": 36.74, "sector_position": 11},
                    {"sector_number": 3, "sector_distance": 400, "sector_time": 12.06, "cumulative_sector_time": 23.99, "sector_position": 3},
                    {"sector_number": 4, "sector_distance": 200, "sector_time": 11.93, "cumulative_sector_time": 11.93, "sector_position": 4},
                ],
            }
        ]

    races, results = loveracing.parse_meeting_xml(SAMPLE_XML, fixture_ctx, sectional_fetcher=fake_sectionals)

    assert len(races) == 1
    assert races[0]["raceId"] == loveracing.RACE_ID_BASE_NZ + 233460
    assert races[0]["country"] == "NZ"
    assert races[0]["currency"] == "NZD"

    assert len(results) == 2
    r1 = next(r for r in results if r["horseNo"] == 7)
    r2 = next(r for r in results if r["horseNo"] == 12)

    assert r1["countryOfOrigin"] == "AUS"
    assert r2["countryOfOrigin"] == "NZ"
    assert r1["first2fSplit"] == 24.1
    assert r1["first2fPos"] == 1
    assert r1["last4fSplit"] == 12.67
    assert r1["last3fSplit"] == 12.75
    assert r1["last1fSplit"] == 11.93
    assert r1["raceId"] == races[0]["raceId"]
