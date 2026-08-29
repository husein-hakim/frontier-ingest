from __future__ import annotations

import re
from datetime import UTC, datetime

from frontier_ingest.core.http import ResilientHttpClient
from frontier_ingest.models import CanonicalRecord, EvidenceMethod, FieldEvidence

REPO_URL = re.compile(r"^https?://github\.com/([^/]+)/([^/#?]+)", re.IGNORECASE)


class GitHubEnricher:
    """Refresh repository metrics in GraphQL batches rather than one REST call per paper."""

    endpoint = "https://api.github.com/graphql"

    def __init__(self, http: ResilientHttpClient, token: str) -> None:
        if not token:
            raise ValueError("GITHUB_TOKEN is required for final star refresh")
        self.http = http
        self.token = token

    async def enrich(self, records: list[CanonicalRecord], batch_size: int = 40) -> None:
        repositories: dict[tuple[str, str], list[CanonicalRecord]] = {}
        for record in records:
            match = REPO_URL.match(str(record.content.get("github_url") or ""))
            if not match:
                continue
            key = (match.group(1), match.group(2).removesuffix(".git"))
            repositories.setdefault(key, []).append(record)
        items = list(repositories.items())
        for offset in range(0, len(items), batch_size):
            batch = items[offset : offset + batch_size]
            query, variables, aliases = self._query(batch)
            response = await self.http.request(
                "POST",
                self.endpoint,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/vnd.github+json",
                },
                json={"query": query, "variables": variables},
                detect_blocks=False,
            )
            payload = __import__("json").loads(response.body)
            if payload.get("errors"):
                raise RuntimeError(f"GitHub GraphQL error: {payload['errors']}")
            collected_at = datetime.now(UTC).isoformat()
            data = payload.get("data", {})
            for alias, key in aliases.items():
                repo = data.get(alias)
                if not repo:
                    continue
                for record in repositories[key]:
                    record.content["github_stars"] = int(repo["stargazerCount"])
                    record.content["github_stars_collected_at"] = collected_at
                    record.content["github_stars_source"] = "github_graphql"
                    record.evidence.extend(
                        [
                            FieldEvidence(
                                "github_stars",
                                EvidenceMethod.API,
                                str(record.content["github_url"]),
                                "GitHub repository.stargazerCount",
                            ),
                            FieldEvidence(
                                "github_stars_collected_at",
                                EvidenceMethod.API,
                                str(record.content["github_url"]),
                                "GitHub GraphQL response time",
                            ),
                            FieldEvidence(
                                "github_stars_source",
                                EvidenceMethod.API,
                                str(record.content["github_url"]),
                                "GitHub GraphQL provider",
                            ),
                        ]
                    )

    @staticmethod
    def _query(
        batch: list[tuple[tuple[str, str], list[CanonicalRecord]]],
    ) -> tuple[str, dict[str, str], dict[str, tuple[str, str]]]:
        declarations: list[str] = []
        selections: list[str] = []
        variables: dict[str, str] = {}
        aliases: dict[str, tuple[str, str]] = {}
        for index, ((owner, name), _) in enumerate(batch):
            owner_var = f"owner{index}"
            name_var = f"name{index}"
            alias = f"repo{index}"
            declarations.extend([f"${owner_var}: String!", f"${name_var}: String!"])
            selections.append(
                f"{alias}: repository(owner: ${owner_var}, name: ${name_var}) "
                "{ stargazerCount url isArchived }"
            )
            variables[owner_var] = owner
            variables[name_var] = name
            aliases[alias] = (owner, name)
        return (
            f"query({', '.join(declarations)}) {{ {' '.join(selections)} }}",
            variables,
            aliases,
        )
