from dataclasses import dataclass


@dataclass(frozen=True)
class HubSpotUpsertResult:
    contact_id: str | None
    gift_id: str | None
    action: str


class HubSpotClient:
    def __init__(self, access_token: str, base_url: str) -> None:
        self.access_token = access_token
        self.base_url = base_url

    def upsert_donation(self, donor_identifier: str, amount: str | None, transaction_key: str) -> HubSpotUpsertResult:
        # Stub only for the first pass.
        return HubSpotUpsertResult(
            contact_id="stub-contact-id",
            gift_id="stub-gift-id",
            action="dry-run",
        )