from unittest.mock import MagicMock, patch


def test_publish(pubsub_client, capsys):
    # create a topic and two subscribers
    r = pubsub_client.post("/topic/prices")
    assert r.status_code == 200
    assert r.json() == {"message": "Topic 'prices' created."}

    r = pubsub_client.post("/subscribe/prices", params={"subscriber": "strategy-1"})
    assert r.status_code == 200
    assert r.json() == {"message": "Subscriber 'strategy-1' subscribed to topic 'prices'."}

    r = pubsub_client.post("/subscribe/prices", params={"subscriber": "strategy-2"})
    assert r.status_code == 200
    assert r.json() == {"message": "Subscriber 'strategy-2' subscribed to topic 'prices'."}

    def mock_post(url, **kwargs):
        print(f"[mock] POST {url} | payload: {kwargs.get('json')}")
        return MagicMock(status_code=200, raise_for_status=lambda: None)

    with patch("edts.pubsub.requests.post", side_effect=mock_post):
        r = pubsub_client.post(
            "/publish/prices",
            json={"generator": "gen-1", "strategy": None, "content": 42.0},
        )

    assert r.status_code == 200
    assert r.json() == {"message": "Message published to topic 'prices'."}

    captured = capsys.readouterr()
    print(captured.out)  # surface mock output in pytest -s
