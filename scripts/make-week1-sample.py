"""Tạo lại dữ liệu mẫu tuần 1: kho bài nhỏ và 20 dòng sự kiện minh họa.

Kịch bản viết trực tiếp, tất định (cùng lệnh → cùng byte), có chủ đích gồm:
- 3 người học, 4 lượt làm bài, đủ 5 loại sự kiện;
- 1 lần gửi trùng (client retry, cùng event_id và cùng nội dung);
- 1 sự kiện đến muộn 95 s và đến sai thứ tự (gửi lỗi rồi retry trên mạng di động).

Chạy: uv run python scripts/make-week1-sample.py
Nhãn is_correct / complete / wrong_answer được kiểm lại bằng SymPy trong tests/.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data_sample"
# Salt CÔNG KHAI chỉ dùng cho dữ liệu mẫu. Hệ thống thật đọc salt bí mật từ biến môi trường.
DEMO_SALT = b"int4418-nhom8-demo-salt-not-secret"
EVENT_NAMESPACE = uuid.UUID("6f1d3c52-3c1e-4c55-9a64-1f6b3f0b8e01")
BASE = datetime(2026, 10, 27, 8, 0, 0, tzinfo=timezone.utc)
CLASS_ID = "C-10A1-2026"

PROBLEMS = [
    {"problem_id": "P-LIN-001", "statement_vi": "Giải phương trình 2x + 3 = 11.", "equation": "2*x + 3 = 11",
     "answer": "4", "skill_ids": ["linear_equation.isolate_variable"]},
    {"problem_id": "P-LIN-002", "statement_vi": "Giải phương trình 3(x - 1) = 2x + 5.", "equation": "3*(x - 1) = 2*x + 5",
     "answer": "8", "skill_ids": ["linear_equation.distribute", "linear_equation.isolate_variable"]},
    {"problem_id": "P-QUAD-001", "statement_vi": "Giải phương trình x² - 5x + 6 = 0.", "equation": "x**2 - 5*x + 6 = 0",
     "answer": "{2, 3}", "skill_ids": ["quadratic_equation.factorization"]},
]
SKILLS = {p["problem_id"]: p["skill_ids"] for p in PROBLEMS}

LEARNERS = {
    "A": {"source_id": "hs-001", "client": {"platform": "web", "app_version": "0.1.0", "connection": "wifi"}},
    "B": {"source_id": "hs-002", "client": {"platform": "android", "app_version": "0.1.0", "connection": "cellular"}},
    "C": {"source_id": "hs-003", "client": {"platform": "ios", "app_version": "0.1.0", "connection": "wifi"}},
}

# (người học, giây tính từ BASE, độ trễ gửi giây, problem_id, event_type, payload)
SCRIPT = [
    ("A", 0, 0.2, "P-LIN-001", "open_problem", {}),
    ("A", 25, 0.2, "P-LIN-001", "submit_step", {"step_index": 1, "expression": "2*x = 8", "is_correct": True}),
    ("A", 41, 0.2, "P-LIN-001", "submit_step", {"step_index": 2, "expression": "x = 4", "is_correct": True}),
    ("A", 44, 0.2, "P-LIN-001", "complete", {"answer": "4", "attempts": 1, "hints_used": 0}),
    ("A", 90, 0.2, "P-QUAD-001", "open_problem", {}),
    ("A", 140, 0.2, "P-QUAD-001", "submit_step", {"step_index": 1, "expression": "x**2 - 5*x = -6", "is_correct": True}),
    ("A", 210, 0.2, "P-QUAD-001", "request_hint", {"hint_level": 1, "step_index": 2}),
    ("B", 5, 0.6, "P-LIN-002", "open_problem", {}),
    # Lỗi phân phối: 3(x - 1) viết thành 3x - 1.
    ("B", 50, 0.6, "P-LIN-002", "submit_step", {"step_index": 1, "expression": "3*x - 1 = 2*x + 5", "is_correct": False}),
    # Gửi lỗi trên mạng di động, client retry sau 95 s: đến muộn và sai thứ tự.
    ("B", 70, 95.0, "P-LIN-002", "request_hint", {"hint_level": 1, "step_index": 1}),
    ("B", 100, 0.6, "P-LIN-002", "submit_step", {"step_index": 1, "expression": "3*x - 3 = 2*x + 5", "is_correct": True}),
    ("B", 125, 0.6, "P-LIN-002", "submit_step", {"step_index": 2, "expression": "x = 8", "is_correct": True}),
    ("B", 130, 0.6, "P-LIN-002", "complete", {"answer": "8", "attempts": 1, "hints_used": 1}),
    ("C", 10, 0.3, "P-QUAD-001", "open_problem", {}),
    # Sai dấu khi phân tích nhân tử.
    ("C", 60, 0.3, "P-QUAD-001", "submit_step", {"step_index": 1, "expression": "(x - 2)*(x + 3) = 0", "is_correct": False}),
    ("C", 95, 0.3, "P-QUAD-001", "submit_step", {"step_index": 1, "expression": "(x - 2)*(x - 3) = 0", "is_correct": True}),
    # Thiếu nghiệm: chỉ ghi x = 2.
    ("C", 110, 0.3, "P-QUAD-001", "wrong_answer", {"answer": "2", "attempt": 1}),
    ("C", 135, 0.3, "P-QUAD-001", "request_hint", {"hint_level": 2, "step_index": 2}),
    ("C", 160, 0.3, "P-QUAD-001", "complete", {"answer": "{2, 3}", "attempts": 2, "hints_used": 1}),
]
# Phần tử chỉ số 11 của SCRIPT (B nộp bước 2) được broker nhận hai lần.
DUPLICATED_INDEX = 11


def pseudonym(source_id: str) -> str:
    return "L-" + hmac.new(DEMO_SALT, source_id.encode(), hashlib.sha256).hexdigest()[:16]


def iso(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_events() -> list[dict]:
    seq: dict[str, int] = {}
    events = []
    for who, offset_s, send_delay_s, problem_id, event_type, payload in SCRIPT:
        learner = LEARNERS[who]
        learner_id = pseudonym(learner["source_id"])
        session_id = "S-" + hashlib.sha256(f"{learner_id}-2026-10-27".encode()).hexdigest()[:8]
        seq[session_id] = seq.get(session_id, 0) + 1
        event_time = BASE + timedelta(seconds=offset_s)
        events.append({
            "schema_version": "0.1",
            "event_id": str(uuid.uuid5(EVENT_NAMESPACE, f"{session_id}/{seq[session_id]}")),
            "event_type": event_type,
            "learner_id": learner_id,
            "class_id": CLASS_ID,
            "session_id": session_id,
            "session_seq": seq[session_id],
            "problem_id": problem_id,
            "skill_ids": SKILLS[problem_id],
            "event_time": iso(event_time),
            "sent_time": iso(event_time + timedelta(seconds=send_delay_s)),
            "client": learner["client"],
            "payload": payload,
        })
    duplicate = dict(events[DUPLICATED_INDEX])
    # Thứ tự broker nhận = thứ tự sent_time; bản gửi lại đến ngay sau bản gốc.
    arrival = sorted(events, key=lambda e: (e["sent_time"], e["event_id"]))
    arrival.insert(arrival.index(events[DUPLICATED_INDEX]) + 1, duplicate)
    return arrival


def write_jsonl(rows: list[dict], path: Path) -> None:
    path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tạo lại dữ liệu mẫu tuần 1.")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    out_dir = parser.parse_args().out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(PROBLEMS, out_dir / "problems-week1-sample.jsonl")
    events = build_events()
    write_jsonl(events, out_dir / "events-week1-sample.jsonl")
    print(f"đã ghi {len(PROBLEMS)} bài và {len(events)} dòng sự kiện vào {out_dir}")


if __name__ == "__main__":
    main()
