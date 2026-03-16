from __future__ import annotations

from collections.abc import Callable, Sequence

from scripts.db import connect
from scripts import (
    create_dashboard_analytics_views,
    create_dashboard_map_view,
    create_dashboard_materialized_views,
    create_dashboard_view,
    create_public_api_views,
    create_target_job_views,
    create_working_student_app_views,
)

RefreshStep = tuple[str, Callable[[], None]]
REFRESH_LOCK_KEY = 804_311_902


def build_refresh_steps() -> list[RefreshStep]:
    return [
        ("dashboard_view", create_dashboard_view.main),
        ("dashboard_map_view", create_dashboard_map_view.main),
        ("dashboard_analytics_views", create_dashboard_analytics_views.main),
        ("dashboard_materialized_views", create_dashboard_materialized_views.main),
        ("target_job_views", create_target_job_views.main),
        ("working_student_app_views", create_working_student_app_views.main),
        ("public_api_views", create_public_api_views.main),
    ]


def refresh_dashboard_read_models(steps: Sequence[RefreshStep] | None = None) -> list[str]:
    executed: list[str] = []
    refresh_steps = list(build_refresh_steps() if steps is None else steps)
    with connect() as lock_conn:
        with lock_conn.cursor() as cur:
            cur.execute("select pg_advisory_lock(%s)", (REFRESH_LOCK_KEY,))
        lock_conn.commit()

        try:
            for name, func in refresh_steps:
                print(f"refreshing_{name}")
                func()
                executed.append(name)
        finally:
            with lock_conn.cursor() as cur:
                cur.execute("select pg_advisory_unlock(%s)", (REFRESH_LOCK_KEY,))
            lock_conn.commit()
    return executed


def main() -> None:
    executed = refresh_dashboard_read_models()
    print(f"dashboard_read_models_refreshed:{','.join(executed)}")


if __name__ == "__main__":
    main()
