from __future__ import annotations

from bgp_harvest.clients import ClickHouseRouteRepository
from bgp_harvest.config.settings import ClickHouseSettings


class LookupProbeRepository(ClickHouseRouteRepository):
    def __init__(self) -> None:
        super().__init__(
            settings=ClickHouseSettings(
                host="localhost",
                port=9000,
                database="bgp",
                user="default",
                password="",
                use_http_driver=False,
                async_inserts=False,
            ),
            batch_size=100_000,
        )
        self.queries: list[str] = []

    def _query_first_column(self, query: str) -> set[int]:
        self.queries.append(query)
        return set()


def test_vrp_object_lookup_uses_small_query_batches() -> None:
    repository = LookupProbeRepository()

    repository._fetch_existing_vrp_object_ids(list(range(5_001)))

    assert len(repository.queries) == 3
