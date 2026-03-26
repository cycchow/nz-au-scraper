from datetime import date

from loveracing.loveracing import (
    build_meeting_overview_url,
    decode_meetings_payload,
    fetch_meeting_result_by_day_id,
    format_calendar_payload,
    generate_month_starts,
    merge_month_meetings,
    parse_meeting_overview_html,
    parse_day_with_context,
    to_fixture_records,
)


def test_decode_d_string_payload():
    payload = {
        "d": '[{"Day":"Sat 28 Feb","DayID":54910,"Club":"Matamata RC","Racecourse":"Matamata"}]'
    }
    meetings = decode_meetings_payload(payload)
    assert isinstance(meetings, list)
    assert meetings[0]["DayID"] == 54910


def test_build_meeting_overview_url():
    assert build_meeting_overview_url(54916) == "https://loveracing.nz/RaceInfo/54916/Meeting-Overview.aspx"


def test_parse_day_with_context():
    parsed = parse_day_with_context("Sat 28 Feb", date(2026, 2, 1))
    assert parsed.isoformat() == "2026-02-28"


def test_fetch_meeting_result_by_day_id_filters_month_results(monkeypatch):
    def fake_fetch_month_meetings(month_start):
        assert month_start == date(2026, 3, 1)
        return [
            {"DayID": 54914, "ResultDownloadXML": "Race_54914.xml"},
            {"DayID": 54915, "ResultDownloadXML": "Race_54915.xml"},
        ]

    monkeypatch.setattr("loveracing.loveracing.fetch_month_meetings", fake_fetch_month_meetings)

    meeting = fetch_meeting_result_by_day_id(54915, date(2026, 3, 6))

    assert meeting == {"DayID": 54915, "ResultDownloadXML": "Race_54915.xml"}


def test_generate_month_starts_desc_inclusive():
    months = generate_month_starts(date(2026, 2, 1), date(2025, 11, 1))
    assert [m.isoformat() for m in months] == ["2026-02-01", "2026-01-01", "2025-12-01", "2025-11-01"]


def test_format_calendar_payload_rolling_6_weeks():
    payload = format_calendar_payload(date(2026, 3, 1), today=date(2026, 3, 1))
    assert payload == {"start": "01-Mar-2026", "end": "12-Apr-2026"}


def test_merge_month_meetings_prefers_results_on_same_dayid():
    results = [{"DayID": 101, "source": "results"}]
    calendar = [{"DayID": 101, "source": "calendar"}]

    merged = merge_month_meetings(results, calendar)

    assert len(merged) == 1
    assert merged[0]["source"] == "results"


def test_merge_month_meetings_adds_missing_calendar_dayid():
    results = [{"DayID": 101, "source": "results"}]
    calendar = [{"DayID": 102, "source": "calendar"}]

    merged = merge_month_meetings(results, calendar)

    assert [item["DayID"] for item in merged] == [101, 102]
    assert merged[1]["source"] == "calendar"


def test_merge_skips_calendar_without_valid_dayid():
    results = [{"DayID": 101, "source": "results"}]
    calendar = [{"DayID": None, "source": "calendar"}]

    merged = merge_month_meetings(results, calendar)

    assert len(merged) == 1
    assert merged[0]["DayID"] == 101


def test_to_fixture_records_mapping():
    meetings = [
        {
            "Day": "Sat 28 Feb",
            "DayID": 54910,
            "Club": "Matamata RC",
            "Racecourse": "Matamata",
            "ResultDownloadXML": "Race_54910.xml",
        }
    ]
    fixtures = to_fixture_records(meetings, date(2026, 2, 1))

    assert len(fixtures) == 1
    assert fixtures[0]["course"] == "Matamata"
    assert fixtures[0]["raceDate"].isoformat() == "2026-02-28"
    assert fixtures[0]["year"] == 2026
    assert fixtures[0]["meta"]["DayID"] == 54910
    assert fixtures[0]["meta"]["requestMonth"] == "2026-02"


def test_to_fixture_records_uses_racedate_fallback_for_calendar_shape():
    meetings = [
        {
            "Day": "3",
            "DayID": 55407,
            "Club": "Racing Taupo",
            "Racecourse": "Taupo",
            "RaceDate": "/Date(1772449200000)/",
        }
    ]

    fixtures = to_fixture_records(meetings, date(2026, 3, 1))

    assert len(fixtures) == 1
    assert fixtures[0]["raceDate"].isoformat() == "2026-03-03"
    assert fixtures[0]["meta"]["DayID"] == 55407


def test_parse_meeting_overview_html_maps_race_and_cards():
    html = """
    <div class="track-conditions">
      <ul class="no-bullets">
        <li>
          <h4>Going</h4>
          <div class="icon"><img alt="Soft" title="Soft" /></div>
          <em>Soft5 6.53 am 22/03/26</em>
        </li>
        <li>
          <h4>Track</h4>
          <div class="icon"><img alt="Left hand" title="Left hand" /></div>
          <em>Left hand<br />1628m</em>
        </li>
      </ul>
    </div>
    <ul>
      <li class="race fields-download">
        <table class="overview-info">
          <tbody>
            <tr>
              <td class="col1">1</td>
              <td class="col2">12:55 pm</td>
              <td class="col3"><a href="#toggle-detail1">ELSDON PARK PLATE</a></td>
              <td class="col4">Rating 75 Benchmark* 1600m - $80,000</td>
            </tr>
          </tbody>
        </table>
        <div class="further-detail">
          <div class="horses">
            <div class="nztr-row row-header"></div>
            <div class="nztr-row">
              <div class="col col-number">1</div>
              <div class="col col-horse">
                <a href="/Common/SystemTemplates/Modal/EntryDetail.aspx?HorseID=435638&RaceID=233545">Peerless</a>
              </div>
            </div>
          </div>
          <div class="horse-details">
            <div id="race1-fields" class="tab-content">
              <div class="nztr-row row-header"></div>
              <div class="nztr-row">
                <div class="col col-draw">1</div>
                <div class="col col-rgt">74</div>
                <div class="col col-wgt">59.5</div>
                <div class="col col-jockey">Joe Doyle</div>
                <div class="col col-trainer">Lance O'Sullivan & Andrew Scott</div>
                <div class="col col-win">6.7</div>
                <div class="col col-place">2.4</div>
              </div>
            </div>
          </div>
        </div>
      </li>
    </ul>
    """
    fixture_ctx = {"raceDate": "2026-03-07", "course": "Ellerslie", "meta": {"DayID": 54916}}

    races, results = parse_meeting_overview_html(html, fixture_ctx)

    assert len(races) == 1
    assert races[0]["raceNo"] == 1
    assert races[0]["raceClass"] == "Rating 75 Benchmark*"
    assert races[0]["distance"] == 1600.0
    assert races[0]["prizeMoney"] == 80000
    assert races[0]["raceId"] == 600233545
    assert races[0]["going"] == "Soft"
    assert races[0]["goingText"] == "Soft5"
    assert races[0]["reading"] == 5.0
    assert races[0]["direction"] == "Left"

    assert len(results) == 1
    assert results[0]["horseNo"] == 1
    assert results[0]["horseId"] == 435638
    assert results[0]["horseName"] == "PEERLESS"
    assert results[0]["draw"] == 1
    assert results[0]["weightCarried"] == 59.5
    assert results[0]["sp"] == 6.7


def test_parse_meeting_overview_html_skips_race_without_race_id():
    html = """
    <li class="race fields-download">
      <table class="overview-info"><tbody><tr>
        <td class="col1">2</td>
        <td class="col2">1:32 pm</td>
        <td class="col3"><a href="#toggle-detail2">RACE NAME</a></td>
        <td class="col4">3YO SW+P 1500m - $100,000</td>
      </tr></tbody></table>
    </li>
    """
    fixture_ctx = {"raceDate": "2026-03-07", "course": "Ellerslie", "meta": {"DayID": 54916}}
    races, results = parse_meeting_overview_html(html, fixture_ctx)
    assert races == []
    assert results == []
