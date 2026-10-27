"""Ground truth của bộ mô phỏng (`ground_truth.jsonl`), tách hẳn khỏi luồng sự kiện.

Thời gian là **offset giây từ t0 của mô phỏng** (`t_s`, chính xác tới ms); bộ phát lại ghi
`replay_start` để dịch sang thời gian tuyệt đối. Mỗi dòng có trường `record`:

- `run`: dòng đầu — t0, horizon, seed, ngưỡng luật, ngưỡng muộn.
- `learner`: nhóm năng lực, kết nối, nền tảng, tham số BKT ẩn và trạng thái thành thạo ban đầu.
- `mastery`: kỹ năng chuyển sang thành thạo **ẩn** sau cơ hội `event_id` (mastery thật theo thời gian).
- `stuck_episode`: bế tắc thật — onset là sự kiện làm `learner_rules.is_wheel_spinning` chuyển
  sang True khi áp **chuỗi thật** (đủ sự kiện, đúng thứ tự event_time, không trùng) của người học;
  kết thúc là sự kiện làm vị từ trở về False (đạt 3 đúng liên tiếp), `null` nếu chưa kết thúc.
- `delivery`: lỗi mạng được chèn cho một `event_id` — `kind` = `late` (sent − event > ngưỡng)
  hoặc `duplicate` (bản gửi lại nguyên byte; `replay_t_s` = lúc bản trùng tới broker).

Hệ thống dùng cùng hàm `learner_rules` trên chuỗi nó nhận được, nên time-to-detect chỉ phản ánh
độ trễ, mất, đảo thứ tự của hệ thống.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from ai_math_tutor import learner_rules
from ai_math_tutor.simulator.population import LearnerProfile

GROUND_TRUTH_VERSION = "1"


def t_seconds(offset_ms: int) -> float:
    return offset_ms / 1000.0


class GroundTruthWriter:
    def __init__(self, path: Path, header: Mapping[str, Any]):
        self._handle = Path(path).open("w", encoding="utf-8", newline="\n")
        self._profiles: dict[tuple[int, str], learner_rules.SkillProfile] = {}
        self._open: dict[tuple[int, str], dict[str, Any]] = {}
        self._learners: dict[int, LearnerProfile] = {}
        self.counts: Counter[str] = Counter()
        self.stuck_learners: dict[str, set[int]] = {}
        self._write({"record": "run", "ground_truth_version": GROUND_TRUTH_VERSION, **header})

    def _write(self, record: Mapping[str, Any]) -> None:
        self._handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.counts[str(record["record"])] += 1

    def learners(self, profiles: Iterable[LearnerProfile]) -> None:
        for profile in profiles:
            self._learners[profile.index] = profile
            self._write({
                "record": "learner",
                "learner_id": profile.learner_id,
                "class_id": profile.class_id,
                "ability_group": profile.ability_group,
                "connection": profile.connection,
                "platform": profile.platform,
                "skills": {
                    skill: {**params.to_dict(), "mastered_at_start": profile.mastered_at_start[skill]}
                    for skill, params in profile.params.items()
                },
            })

    def observe(
        self,
        learner: LearnerProfile,
        latent: Mapping[str, bool],
        learned: Iterable[str],
        event_id: str,
        t_ms: int,
        event_type: str,
        payload: Mapping[str, Any],
        skill_ids: Iterable[str],
    ) -> None:
        """Áp sự kiện thật lên hồ sơ luật; ghi episode bế tắc khi vị từ đổi giá trị.

        `latent` là trạng thái ẩn sau sự kiện, `learned` là kỹ năng vừa chuyển sang thành thạo
        tại chính sự kiện này; trạng thái ẩn *lúc làm* = latent và không thuộc learned.
        """
        observation = learner_rules.observation_for(event_type, payload)
        if observation is None:
            return
        for skill in skill_ids:
            key = (learner.index, skill)
            before = self._profiles.get(key, learner_rules.EMPTY_PROFILE)
            after = learner_rules.update(before, observation)
            self._profiles[key] = after
            was, now = learner_rules.is_wheel_spinning(before), learner_rules.is_wheel_spinning(after)
            if not was and now:
                self._open[key] = {
                    "record": "stuck_episode",
                    "learner_id": learner.learner_id,
                    "class_id": learner.class_id,
                    "skill_id": skill,
                    "ability_group": learner.ability_group,
                    "connection": learner.connection,
                    "onset_event_id": event_id,
                    "onset_t_s": t_seconds(t_ms),
                    "onset_opportunity": after.opportunities,
                    "latent_mastered_at_onset": bool(latent[skill]) and skill not in learned,
                    "end_event_id": None,
                    "end_t_s": None,
                    "end_opportunity": None,
                }
                self.stuck_learners.setdefault(learner.ability_group, set()).add(learner.index)
            elif was and not now:
                episode = self._open.pop(key)
                episode.update(end_event_id=event_id, end_t_s=t_seconds(t_ms), end_opportunity=after.opportunities)
                self._write(episode)

    def mastery(self, learner: LearnerProfile, skill: str, event_id: str, t_ms: int) -> None:
        profile = self._profiles.get((learner.index, skill), learner_rules.EMPTY_PROFILE)
        self._write({
            "record": "mastery",
            "learner_id": learner.learner_id,
            "skill_id": skill,
            "event_id": event_id,
            "t_s": t_seconds(t_ms),
            "opportunity": profile.opportunities,
        })

    def delivery(self, learner: LearnerProfile, event_id: str, kind: str, event_ms: int, sent_ms: int, replay_ms: int) -> None:
        self._write({
            "record": "delivery",
            "kind": kind,
            "event_id": event_id,
            "learner_id": learner.learner_id,
            "connection": learner.connection,
            "event_t_s": t_seconds(event_ms),
            "sent_t_s": t_seconds(sent_ms),
            "replay_t_s": t_seconds(replay_ms),
        })

    def abort(self) -> None:
        """Đóng tệp khi sinh thất bại (người gọi xóa đầu ra dở)."""
        self._handle.close()

    def close(self) -> dict[str, Any]:
        """Ghi các episode còn mở (theo thứ tự onset) rồi đóng tệp; trả thống kê sanity."""
        open_at_end = len(self._open)
        for episode in self._open.values():
            self._write(episode)
        self._open.clear()
        self._handle.close()
        groups = Counter(p.ability_group for p in self._learners.values())
        return {
            "records": dict(sorted(self.counts.items())),
            "stuck_episodes": self.counts["stuck_episode"],
            "stuck_episodes_open_at_end": open_at_end,
            "mastery_transitions": self.counts["mastery"],
            "learners_by_group": dict(sorted(groups.items())),
            "stuck_learners_by_group": {g: len(self.stuck_learners.get(g, ())) for g in sorted(groups)},
        }
