"""Fold consecutive host values without choosing their wording."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class EventFold[T]:
    """A consecutive run whose endpoints refer to the original input values."""

    first: T
    last: T
    count: int
    mode: Literal["single", "repeat", "steps"]


def fold_events[T](
    events: Sequence[T],
    *,
    repeats: Callable[[T, T], bool],
    continues: Callable[[T, T], bool],
    fold: bool = True,
) -> tuple[EventFold[T], ...]:
    """Fold while every value repeats the first or every value continues its act."""
    if not fold:
        return tuple(EventFold(event, event, 1, "single") for event in events)
    out: list[EventFold[T]] = []
    run: EventFold[T] | None = None
    same = True
    steps = True
    for event in events:
        repeated = run is not None and same and repeats(run.first, event)
        continued = run is not None and steps and continues(run.first, event)
        if run is not None and (repeated or continued):
            same = repeated
            steps = continued
            run = EventFold(run.first, event, run.count + 1, "repeat" if same else "steps")
        else:
            if run is not None:
                out.append(run)
            run = EventFold(event, event, 1, "single")
            same = True
            steps = True
    if run is not None:
        out.append(run)
    return tuple(out)
