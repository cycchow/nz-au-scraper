from datetime import date

import main


def test_save_fixtures_nz_defaults(monkeypatch):
    captured = []

    def fake_send_merge_mutation(type_name, input_obj):
        captured.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    fixtures = [
        {
            "raceDate": date(2026, 2, 28),
            "course": "Matamata",
            "year": 2026,
            "meta": {"DayID": 54910},
        }
    ]

    main.save_fixtures(fixtures, country="NZ")

    assert len(captured) == 1
    type_name, payload = captured[0]
    assert type_name == "com.superstring.globalracing.uk.models.types.Fixture"
    assert payload["country"] == "NZ"
    assert payload["src"] == "loveracing"
    assert payload["fixtureId"] == 6000000000 + 54910
    assert payload["meetingId"] == 54910


def test_save_fixtures_uses_top_level_meeting_id(monkeypatch):
    captured = []

    def fake_send_merge_mutation(type_name, input_obj):
        captured.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    fixtures = [
        {
            "raceDate": date(2026, 3, 10),
            "course": "Flemington",
            "year": 2026,
            "meetingId": 700123456,
            "meta": {"race_meet_id": 123456},
        }
    ]

    main.save_fixtures(fixtures, country="AUS")

    assert len(captured) == 1
    _, payload = captured[0]
    assert payload["country"] == "AUS"
    assert payload["meetingId"] == 700123456
    assert payload["fixtureId"] == 8000000000 + 700123456
