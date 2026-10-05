"""Subscription management: create, query, enable/disable (#1, #2, #3)."""


def test_create_subscription(svc, session):
    sub = svc.subscriptions.create("http://example/hook", "order.created")
    session.commit()
    assert sub.id
    assert sub.enabled is True
    assert sub.created_at is not None

    fetched = svc.subscriptions.get(sub.id)
    assert fetched is not None
    assert fetched.url == "http://example/hook"
    assert fetched.event_type == "order.created"


def test_list_subscriptions_filters_by_event_type(svc, session):
    svc.subscriptions.create("http://a", "type.a")
    svc.subscriptions.create("http://b", "type.b")
    svc.subscriptions.create("http://c", "type.a")
    session.commit()
    assert len(svc.subscriptions.list("type.a")) == 2
    assert len(svc.subscriptions.list("type.b")) == 1
    assert len(svc.subscriptions.list()) == 3


def test_disabled_subscription_gets_no_new_deliveries(svc, session, clock):
    active = svc.subscriptions.create("http://a", "evt", enabled=True)
    disabled = svc.subscriptions.create("http://b", "evt", enabled=True)
    session.commit()

    svc.subscriptions.set_enabled(disabled.id, False)
    session.commit()
    assert svc.subscriptions.get(disabled.id).enabled is False

    result = svc.events.create("evt", {"x": 1})
    session.commit()
    sub_ids = {d.subscription_id for d in result.deliveries}
    assert sub_ids == {active.id}


def test_disabling_keeps_history_deliveries(svc, session):
    sub = svc.subscriptions.create("http://a", "evt", enabled=True)
    session.commit()
    result = svc.events.create("evt", {"x": 1})
    session.commit()
    delivery_id = result.deliveries[0].id

    svc.subscriptions.set_enabled(sub.id, False)
    session.commit()

    status = svc.deliveries.event_status(result.event.id)
    assert status.total == 1
    assert status.deliveries[0].id == delivery_id


def test_event_fans_out_to_every_matching_enabled_subscription(svc, session):
    s1 = svc.subscriptions.create("http://1", "evt")
    s2 = svc.subscriptions.create("http://2", "evt")
    s3 = svc.subscriptions.create("http://3", "evt")
    svc.subscriptions.create("http://4", "other")
    svc.subscriptions.create("http://5", "evt", enabled=False)
    session.commit()

    result = svc.events.create("evt", {"k": "v"})
    session.commit()
    assert len(result.deliveries) == 3
    assert {d.subscription_id for d in result.deliveries} == {s1.id, s2.id, s3.id}
    # deliveries are independent rows
    assert len({d.id for d in result.deliveries}) == 3
