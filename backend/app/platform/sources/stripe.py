"""Stripe source implementation.

We retrieve data from the Stripe API for the following core resources:
- Balance
- Balance Transactions
- Charges
- Customers
- Events
- Invoices
- Payment Intents
- Payment Methods
- Payouts
- Refunds
- Subscriptions

Then, we yield them as entities using the respective entity schemas defined in entities/stripe.py.
"""

from typing import AsyncGenerator

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import ChunkEntity
from app.platform.entities.stripe import (
    StripeBalanceEntity,
    StripeBalanceTransactionEntity,
    StripeChargeEntity,
    StripeCustomerEntity,
    StripeEventEntity,
    StripeInvoiceEntity,
    StripePaymentIntentEntity,
    StripePaymentMethodEntity,
    StripePayoutEntity,
    StripeRefundEntity,
    StripeSubscriptionEntity,
)
from app.platform.sources._base import BaseSource


@source("Stripe", "stripe", AuthType.config_class, "StripeAuthConfig")
class StripeSource(BaseSource):
    """Stripe source implementation.

    This connector retrieves data from various Stripe objects, yielding them as entities
    through their respective schemas. The following resource endpoints are used:

      - /v1/balance
      - /v1/balance_transactions
      - /v1/charges
      - /v1/customers
      - /v1/events
      - /v1/invoices
      - /v1/payment_intents
      - /v1/payment_methods
      - /v1/payouts
      - /v1/refunds
      - /v1/subscriptions

    Each resource endpoint may use Stripe’s pagination (has_more + starting_after) to
    retrieve all objects. Fields are mapped to the entity schemas defined in entities/stripe.py.
    """

    @classmethod
    async def create(cls, access_token: str) -> "StripeSource":
        """Create a new Stripe source instance."""
        instance = cls()
        instance.access_token = access_token
        return instance

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _get_with_auth(self, client: httpx.AsyncClient, url: str) -> dict:
        """Make an authenticated GET request to the Stripe API.

        The `url` should be a fully qualified endpoint (e.g., 'https://api.stripe.com/v1/customers').
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    async def _generate_balance_entity(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve the current account balance (single object) from.

        GET https://api.stripe.com/v1/balance
        Yields exactly one StripeBalanceEntity if successful.
        """
        url = "https://api.stripe.com/v1/balance"
        data = await self._get_with_auth(client, url)
        yield StripeBalanceEntity(
            entity_id="balance",  # Arbitrary ID since there's only one balance resource
            available=data.get("available", []),
            pending=data.get("pending", []),
            instant_available=data.get("instant_available"),
            connect_reserved=data.get("connect_reserved"),
            livemode=data.get("livemode", False),
        )

    async def _generate_balance_transaction_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve balance transactions in a paginated loop from.

        GET https://api.stripe.com/v1/balance_transactions
        Yields StripeBalanceTransactionEntity objects.
        """
        base_url = "https://api.stripe.com/v1/balance_transactions?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for txn in data.get("data", []):
                yield StripeBalanceTransactionEntity(
                    entity_id=txn["id"],
                    amount=txn.get("amount"),
                    currency=txn.get("currency"),
                    created_at=txn.get("created"),
                    description=txn.get("description"),
                    fee=txn.get("fee"),
                    fee_details=txn.get("fee_details", []),
                    net=txn.get("net"),
                    reporting_category=txn.get("reporting_category"),
                    source=txn.get("source"),
                    status=txn.get("status"),
                    type=txn.get("type"),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_charge_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of charges.

          GET https://api.stripe.com/v1/charges
        Paginated, yields StripeChargeEntity objects.
        """
        base_url = "https://api.stripe.com/v1/charges?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for charge in data.get("data", []):
                yield StripeChargeEntity(
                    entity_id=charge["id"],
                    amount=charge.get("amount"),
                    currency=charge.get("currency"),
                    captured=charge.get("captured", False),
                    paid=charge.get("paid", False),
                    refunded=charge.get("refunded", False),
                    created_at=charge.get("created"),
                    description=charge.get("description"),
                    receipt_url=charge.get("receipt_url"),
                    customer_id=charge.get("customer"),
                    invoice_id=charge.get("invoice"),
                    metadata=charge.get("metadata", {}),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_customer_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of customers.

        GET https://api.stripe.com/v1/customers
        Paginated, yields StripeCustomerEntity objects.
        """
        base_url = "https://api.stripe.com/v1/customers?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for cust in data.get("data", []):
                yield StripeCustomerEntity(
                    entity_id=cust["id"],
                    email=cust.get("email"),
                    phone=cust.get("phone"),
                    name=cust.get("name"),
                    description=cust.get("description"),
                    created_at=cust.get("created"),
                    currency=cust.get("currency"),
                    default_source=cust.get("default_source"),
                    delinquent=cust.get("delinquent", False),
                    invoice_prefix=cust.get("invoice_prefix"),
                    metadata=cust.get("metadata", {}),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_event_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of events.

        GET https://api.stripe.com/v1/events
        Paginated, yields StripeEventEntity objects.
        """
        base_url = "https://api.stripe.com/v1/events?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for evt in data.get("data", []):
                yield StripeEventEntity(
                    entity_id=evt["id"],
                    event_type=evt.get("type"),
                    api_version=evt.get("api_version"),
                    created_at=evt.get("created"),
                    data=evt.get("data", {}),
                    livemode=evt.get("livemode", False),
                    pending_webhooks=evt.get("pending_webhooks"),
                    request=evt.get("request"),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_invoice_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of invoices.

        GET https://api.stripe.com/v1/invoices
        Paginated, yields StripeInvoiceEntity objects.
        """
        base_url = "https://api.stripe.com/v1/invoices?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for inv in data.get("data", []):
                yield StripeInvoiceEntity(
                    entity_id=inv["id"],
                    customer_id=inv.get("customer"),
                    number=inv.get("number"),
                    status=inv.get("status"),
                    amount_due=inv.get("amount_due"),
                    amount_paid=inv.get("amount_paid"),
                    amount_remaining=inv.get("amount_remaining"),
                    created_at=inv.get("created"),
                    due_date=inv.get("due_date"),
                    paid=inv.get("paid", False),
                    currency=inv.get("currency"),
                    metadata=inv.get("metadata", {}),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_payment_intent_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of payment intents.

        GET https://api.stripe.com/v1/payment_intents
        Paginated, yields StripePaymentIntentEntity objects.
        """
        base_url = "https://api.stripe.com/v1/payment_intents?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for pi in data.get("data", []):
                yield StripePaymentIntentEntity(
                    entity_id=pi["id"],
                    amount=pi.get("amount"),
                    currency=pi.get("currency"),
                    status=pi.get("status"),
                    description=pi.get("description"),
                    created_at=pi.get("created"),
                    customer_id=pi.get("customer"),
                    metadata=pi.get("metadata", {}),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_payment_method_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of payment methods for the account or for a specific customer.

        The typical GET is: https://api.stripe.com/v1/payment_methods?customer=<id>&type=<type>
        For demonstration, we'll assume you pass a type of 'card' for all of them.
        Paginated, yields StripePaymentMethodEntity objects.
        """
        # Adjust as needed to retrieve the correct PaymentMethods.
        base_url = "https://api.stripe.com/v1/payment_methods?limit=100&type=card"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for pm in data.get("data", []):
                yield StripePaymentMethodEntity(
                    entity_id=pm["id"],
                    type=pm.get("type"),
                    billing_details=pm.get("billing_details", {}),
                    customer_id=pm.get("customer"),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_payout_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of payouts.

        GET https://api.stripe.com/v1/payouts
        Paginated, yields StripePayoutEntity objects.
        """
        base_url = "https://api.stripe.com/v1/payouts?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for payout in data.get("data", []):
                yield StripePayoutEntity(
                    entity_id=payout["id"],
                    amount=payout.get("amount"),
                    currency=payout.get("currency"),
                    arrival_date=payout.get("arrival_date"),
                    created_at=payout.get("created"),
                    description=payout.get("description"),
                    destination=payout.get("destination"),
                    method=payout.get("method"),
                    status=payout.get("status"),
                    statement_descriptor=payout.get("statement_descriptor"),
                    metadata=payout.get("metadata", {}),
                )
            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_refund_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of refunds.

        GET https://api.stripe.com/v1/refunds
        Paginated, yields StripeRefundEntity objects.
        """
        base_url = "https://api.stripe.com/v1/refunds?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for refund in data.get("data", []):
                yield StripeRefundEntity(
                    entity_id=refund["id"],
                    amount=refund.get("amount"),
                    currency=refund.get("currency"),
                    created_at=refund.get("created"),
                    status=refund.get("status"),
                    reason=refund.get("reason"),
                    receipt_number=refund.get("receipt_number"),
                    charge_id=refund.get("charge"),
                    payment_intent_id=refund.get("payment_intent"),
                    metadata=refund.get("metadata", {}),
                )
            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def _generate_subscription_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Retrieve a list of subscriptions.

        GET https://api.stripe.com/v1/subscriptions
        Paginated, yields StripeSubscriptionEntity objects.
        """
        base_url = "https://api.stripe.com/v1/subscriptions?limit=100"
        url = base_url

        while url:
            data = await self._get_with_auth(client, url)
            for sub in data.get("data", []):
                yield StripeSubscriptionEntity(
                    entity_id=sub["id"],
                    customer_id=sub.get("customer"),
                    status=sub.get("status"),
                    current_period_start=sub.get("current_period_start"),
                    current_period_end=sub.get("current_period_end"),
                    cancel_at_period_end=sub.get("cancel_at_period_end", False),
                    canceled_at=sub.get("canceled_at"),
                    created_at=sub.get("created"),
                    metadata=sub.get("metadata", {}),
                )

            has_more = data.get("has_more")
            if not has_more:
                url = None
            else:
                last_id = data["data"][-1]["id"]
                url = f"{base_url}&starting_after={last_id}"

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:  # noqa: C901
        """Generate all Stripe entities.

        - Balance
        - Balance Transactions
        - Charges
        - Customers
        - Events
        - Invoices
        - Payment Intents
        - Payment Methods
        - Payouts
        - Refunds
        - Subscriptions
        """
        async with httpx.AsyncClient() as client:
            # 1) Single Balance resource
            async for balance_entity in self._generate_balance_entity(client):
                yield balance_entity

            # 2) Balance Transactions
            async for txn_entity in self._generate_balance_transaction_entities(client):
                yield txn_entity

            # 3) Charges
            async for charge_entity in self._generate_charge_entities(client):
                yield charge_entity

            # 4) Customers
            async for customer_entity in self._generate_customer_entities(client):
                yield customer_entity

            # 5) Events
            async for event_entity in self._generate_event_entities(client):
                yield event_entity

            # 6) Invoices
            async for invoice_entity in self._generate_invoice_entities(client):
                yield invoice_entity

            # 7) Payment Intents
            async for pi_entity in self._generate_payment_intent_entities(client):
                yield pi_entity

            # 8) Payment Methods
            async for pm_entity in self._generate_payment_method_entities(client):
                yield pm_entity

            # 9) Payouts
            async for payout_entity in self._generate_payout_entities(client):
                yield payout_entity

            # 10) Refunds
            async for refund_entity in self._generate_refund_entities(client):
                yield refund_entity

            # 11) Subscriptions
            async for sub_entity in self._generate_subscription_entities(client):
                yield sub_entity
