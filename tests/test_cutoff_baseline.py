"""首批快照带截止序号的同步链路：暂挂/追平、有效基线、废弃重开、发布失败交错。

对应缺陷场景：首批全量清单来源截止序号为 100，随后到达的 101 号迁区变更
被误报为“缺少 1～100”，且首批发布后仍无法接收。
"""
from __future__ import annotations


def open_batch(client, total_shards: int, cutoff: int) -> int:
    r = client.post("/batches", json={"total_shards": total_shards, "source_cutoff_seq": cutoff})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def shard(client, batch_id: int, shard_no: int, items: list[dict]):
    return client.post(
        f"/batches/{batch_id}/shards", json={"shard_no": shard_no, "items": items}
    )


def change(client, seq: int, ctype: str, outlet: str, district=None, active=None):
    return client.post(
        "/changes",
        json={
            "seq": seq,
            "change_type": ctype,
            "outlet_no": outlet,
            "district": district,
            "active": active,
        },
    )


def publish(client, batch_id: int, catch_up_seq: int):
    return client.post(
        f"/batches/{batch_id}/publish", json={"catch_up_seq": catch_up_seq}
    )


# ---------- 1. 缺陷复现路径：截止 100 的首批，101 在发布前后都能接收 ----------

def test_cutoff100_change101_accepted_during_and_after_first_upload(client):
    # 开首批，来源截止序号 100（快照自带 1..100 的效果）
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
    ])

    # 缺陷场景核心断言：101 号变更不得被报为“缺少 1～100”
    r = change(client, 101, "move", "O1", district="B")
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["idempotent"] is False
    assert body["applied"] is False          # 首批尚未发布，无现役清单可套
    assert body["blocked"] is False          # 暂挂不是断点
    assert body["held"] is True
    assert body["effective_baseline"] == 100
    assert body["expected_next_seq"] == 102

    # 待补/下一应收视图不得把快照已覆盖的 1..100 算作缺口
    status = client.get("/batches/current").json()
    assert status["changes_pending"] == []
    assert status["next_expected_seq"] == 102
    assert status["effective_baseline"] == 100
    assert status["held_change_count"] == 1

    listing = client.get("/changes").json()
    assert listing["missing_seqs"] == []
    assert listing["next_expected_seq"] == 102

    # 发布前现役清单仍为空（暂挂不提前生效）
    assert client.get("/outlets").json()["count"] == 0

    # 追平到 101：窗口 (100,101] 重放，暂挂变更纳入现役清单
    r = publish(client, bid, 101)
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 1
    assert r.json()["changes_applied"] == 1
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["O1"]["district"] == "B"
    health = client.get("/health").json()
    assert health["version"] == 1 and health["last_served_seq"] == 101

    # 发布后增量按有效基线接续：102 正常接收并立即套用
    r = change(client, 102, "deactivate", "O1")
    assert r.status_code == 201 and r.json()["applied"] is True
    assert r.json()["expected_next_seq"] == 103
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["O1"]["active"] is False


def test_cutoff100_submitting_history_seq_rejected_not_gap(client):
    # 快照覆盖范围内的序号既不算缺口也不允许作为增量补写
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])

    r = change(client, 50, "deactivate", "O1")
    assert r.status_code == 409 and r.json()["code"] == "SEQ_COVERED_BY_SNAPSHOT"
    assert r.json()["details"]["effective_baseline"] == 100

    r = change(client, 100, "deactivate", "O1")
    assert r.status_code == 409 and r.json()["code"] == "SEQ_COVERED_BY_SNAPSHOT"

    # 视图同样不把 1..100 当缺口
    assert client.get("/changes").json()["missing_seqs"] == []


def test_cutoff100_real_gap_still_detected(client):
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])

    # 101 到达后直接跳 103：真实缺号只有 102，而不是 1..102
    r = change(client, 101, "move", "O1", district="B")
    assert r.status_code == 201
    r = change(client, 103, "deactivate", "O1")
    assert r.status_code == 422 and r.json()["code"] == "SEQ_GAP"
    assert r.json()["details"]["missing_seqs"] == [102]
    assert r.json()["details"]["expected_next_seq"] == 102

    # 补齐 102 后 103 可进
    assert change(client, 102, "deactivate", "O1").status_code == 201
    r = change(client, 103, "move", "O1", district="C")
    assert r.status_code == 201 and r.json()["held"] is True


def test_cutoff101_retry_is_idempotent(client):
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    payload = {"seq": 101, "change_type": "move", "outlet_no": "O1", "district": "B"}

    r1 = client.post("/changes", json=payload)
    assert r1.status_code == 201 and r1.json()["idempotent"] is False
    r2 = client.post("/changes", json=payload)
    assert r2.status_code == 201 and r2.json()["idempotent"] is True
    assert r2.json()["held"] is True

    # 同序号异内容仍是冲突
    r3 = client.post("/changes", json={**payload, "district": "C"})
    assert r3.status_code == 409 and r3.json()["code"] == "CHANGE_CONTENT_CONFLICT"


def test_publish_catches_up_multiple_held_changes(client):
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    # 首批发布前到达一串连续变更，全部暂挂
    change(client, 101, "move", "O1", district="B")
    change(client, 102, "deactivate", "O2")
    change(client, 103, "add", "O3", district="C", active=True)

    r = publish(client, bid, 103)
    assert r.status_code == 200, r.text
    assert r.json()["changes_applied"] == 3
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["O1"]["district"] == "B"
    assert outlets["O2"]["active"] is False
    assert outlets["O3"]["district"] == "C"

    listing = client.get("/changes").json()
    assert all(c["applied"] is True and c["held"] is False for c in listing["items"])
    r = client.get("/batches/current")
    assert r.status_code == 404 and r.json()["code"] == "NO_OPEN_BATCH"


# ---------- 2. 废弃后用相同截止序号重开：已接收变更仍可追平，废弃不推进水位 ----------

def test_abort_and_reopen_same_cutoff_held_changes_reused(client):
    # 首批截止 100，101/102 在上传期间到达并暂挂
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    change(client, 101, "move", "O1", district="B")
    change(client, 102, "deactivate", "O2")

    # 废弃首批：现役水位保持 0（version=0），变更流不丢
    r = client.post(f"/batches/{bid}/abort")
    assert r.status_code == 200 and r.json()["status"] == "aborted"
    health = client.get("/health").json()
    assert health == {"status": "ok", "version": 0, "last_served_seq": 0}
    listing = client.get("/changes").json()
    assert {c["seq"] for c in listing["items"]} == {101, 102}
    assert all(c["held"] is True for c in listing["items"])

    # 用相同截止序号 100 重开：下一应收仍是 103，历史 1..100 不算缺口
    bid2 = open_batch(client, 1, cutoff=100)
    status = client.get("/batches/current").json()
    assert status["batch_id"] == bid2
    assert status["next_expected_seq"] == 103
    assert status["held_change_count"] == 2

    # 重开批次发布追平到 102：此前接收的 101/102 直接用于追平
    shard(client, bid2, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    r = publish(client, bid2, 102)
    assert r.status_code == 200, r.text
    assert r.json()["changes_applied"] == 2
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["O1"]["district"] == "B" and outlets["O2"]["active"] is False
    assert client.get("/health").json()["last_served_seq"] == 102


def test_abort_then_reopen_real_gap_blocks_republish(client):
    # 废弃重开后真实缺号仍由发布门禁拦住
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    change(client, 101, "move", "O1", district="B")
    client.post(f"/batches/{bid}/abort")

    bid2 = open_batch(client, 1, cutoff=100)
    shard(client, bid2, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    # 试图追平到 103，但 102、103 都没到（真实缺号，不含 1..100）
    r = publish(client, bid2, 103)
    assert r.status_code == 422 and r.json()["code"] == "CHANGE_SEQ_GAP"
    assert r.json()["details"]["missing_seqs"] == [102, 103]
    # 废弃/发布失败都不推进水位
    assert client.get("/health").json()["last_served_seq"] == 0


# ---------- 3. 发布失败交错：旧版照常供数，暂挂变更保留，重试可成 ----------

def test_publish_failure_keeps_held_changes_and_old_version(client):
    # 先发布 v1（截止 0、追平 0）
    bid = open_batch(client, 1, cutoff=0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)

    # 第二批截止 100：101 引用快照里不存在的排口，窗口重放必失败
    bid2 = open_batch(client, 1, cutoff=100)
    shard(client, bid2, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    change(client, 101, "move", "GHOST", district="Z")

    r = publish(client, bid2, 101)
    assert r.status_code == 422 and r.json()["code"] == "PUBLISH_CHANGE_FAILED"
    assert r.json()["details"]["seq"] == 101

    # 旧版照常供数、水位不动、批次仍 open；失败的 101 被标记为链上断点（blocked），
    # 事务整体回滚不影响旧版；废弃重开后窗口重放会解除阻断。
    body = client.get("/outlets").json()
    assert body["version"] == 1 and body["count"] == 1
    assert client.get("/health").json()["last_served_seq"] == 0
    ch101 = [c for c in client.get("/changes").json()["items"] if c["seq"] == 101][0]
    assert ch101["applied"] is False and ch101["blocked"] is True
    assert client.get("/batches/current").json()["batch_id"] == bid2

    # 补齐快照后同批次重试发布成功
    client.post(f"/batches/{bid2}/abort")
    bid3 = open_batch(client, 1, cutoff=100)
    shard(client, bid3, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "GHOST", "district": "Q", "active": True},
    ])
    r = publish(client, bid3, 101)
    assert r.status_code == 200, r.text
    assert r.json()["changes_applied"] == 1
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["GHOST"]["district"] == "Z"


# ---------- 4. 已发布后的增量基线：水位与新批次截止取较大值 ----------

def test_incremental_baseline_after_first_publish(client):
    bid = open_batch(client, 1, cutoff=100)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    change(client, 101, "move", "O1", district="B")  # 上传期间暂挂
    publish(client, bid, 101)  # 水位推进到 101，101 经窗口重放生效

    # 水位之内的序号补写：同内容重试幂等，异内容冲突，均不会产生新序号
    r = change(client, 101, "move", "O1", district="B")
    assert r.status_code == 201 and r.json()["idempotent"] is True
    r = change(client, 101, "move", "O1", district="Z")
    assert r.status_code == 409 and r.json()["code"] == "CHANGE_CONTENT_CONFLICT"
    # 水位之内、变更流中不存在的旧序号 -> SEQ_REGRESSION
    r = change(client, 50, "deactivate", "O1")
    assert r.status_code == 409 and r.json()["code"] == "SEQ_REGRESSION"

    # 再开截止序号更高的批次（105）：基线取截止 105，102..105 视为快照已覆盖
    bid2 = open_batch(client, 1, cutoff=105)
    shard(client, bid2, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    r = change(client, 103, "deactivate", "O1")
    assert r.status_code == 409 and r.json()["code"] == "SEQ_COVERED_BY_SNAPSHOT"
    assert client.get("/changes").json()["effective_baseline"] == 105

    # 106 在第二批上传期间到达：现役 v1 仍在供数，立即套到旧版（非暂挂），
    # 发布时再由窗口 (105,106] 在新快照上重放，一致性不丢。
    r = change(client, 106, "deactivate", "O1")
    assert r.status_code == 201 and r.json()["applied"] is True and r.json()["held"] is False
    r = publish(client, bid2, 106)
    assert r.status_code == 200 and r.json()["version"] == 2
    assert r.json()["changes_applied"] == 1
    assert client.get("/health").json()["last_served_seq"] == 106
    assert {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}["O1"]["active"] is False

    # 后续增量从 107 接续
    r = change(client, 107, "move", "O1", district="D")
    assert r.status_code == 201 and r.json()["applied"] is True
    assert r.json()["expected_next_seq"] == 108
