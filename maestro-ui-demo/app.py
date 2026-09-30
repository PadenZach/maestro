"""Synthetic order workflows for exercising the Maestro UI; no external orders."""

import argparse
import os
from uuid import uuid4

from dbos import DBOS, SetWorkflowID


def record_progress(order: dict, stage: str) -> None:
    """Publish the latest status and retain its history in an ordered stream."""
    entry = {"order_id": order["id"], "stage": stage}
    DBOS.set_event("status", entry)
    DBOS.write_stream("progress", entry)


def record_failure(order: dict, error: Exception) -> None:
    DBOS.set_event("error", {"type": type(error).__name__, "message": str(error)})
    record_progress(order, "failed")


def queue_customer_update(order: dict, stage: str) -> None:
    """Leave synthetic messages in this order's inbox for UI inspection."""
    workflow_id = DBOS.workflow_id
    assert workflow_id is not None
    DBOS.send(
        workflow_id,
        {"order_id": order["id"], "stage": stage},
        topic="customer-updates",
    )


@DBOS.step()
def validate_order(order: dict) -> dict:
    DBOS.logger.info("Validating %s: %s", order["id"], order)
    return {**order, "validated": True}


@DBOS.step()
def reserve_inventory(order: dict) -> dict:
    DBOS.logger.info("Reserved %s demo items for %s", order["quantity"], order["id"])
    return {"sku": "DEMO-WIDGET", "quantity": order["quantity"], "reserved": True}


@DBOS.workflow()
def inventory_workflow(order: dict) -> dict:
    DBOS.logger.info("Checking inventory for %s", order["id"])
    record_progress(order, "started")
    try:
        DBOS.sleep(2)
        result = reserve_inventory(order)
        DBOS.set_event("inventory", result)
        record_progress(order, "completed")
        DBOS.logger.info("Inventory ready for %s", order["id"])
        return result
    except Exception as exc:
        record_failure(order, exc)
        raise
    finally:
        DBOS.close_stream("progress")


@DBOS.step()
def charge_payment(order: dict) -> dict:
    if order["scenario"] == "declined":
        DBOS.logger.error("Simulated payment decline for %s", order["id"])
        raise ValueError(f"Demo payment declined for {order['id']}")
    result = {"amount_cents": order["quantity"] * 2500, "currency": "USD"}
    DBOS.logger.info("Demo payment approved for %s: %s", order["id"], result)
    return result


@DBOS.workflow()
def payment_workflow(order: dict) -> dict:
    DBOS.logger.info("Starting payment for %s", order["id"])
    record_progress(order, "started")
    try:
        if order["scenario"] == "review":
            DBOS.logger.warning("%s requires simulated manual review", order["id"])
            record_progress(order, "awaiting_review")
            DBOS.sleep(8)
            approval = DBOS.recv(topic="payment-review", timeout_seconds=30)
            if not isinstance(approval, dict) or approval != {
                "order_id": order["id"], "approved": True,
            }:
                raise ValueError(f"Demo payment review not approved for {order['id']}")
            DBOS.set_event("review", approval)
            record_progress(order, "review_approved")
            DBOS.logger.info("Manual review approved for %s", order["id"])
        else:
            DBOS.sleep(3)
        result = charge_payment(order)
        DBOS.set_event("payment", result)
        record_progress(order, "completed")
        return result
    except Exception as exc:
        record_failure(order, exc)
        raise
    finally:
        DBOS.close_stream("progress")


@DBOS.step()
def create_shipping_label(order: dict) -> dict:
    result = {
        "tracking_number": f"DEMO-{order['id']}",
        "service": "express" if order["scenario"] == "express" else "standard",
    }
    DBOS.logger.info("Created demo shipping label for %s: %s", order["id"], result)
    return result


@DBOS.step()
def notify_customer(order: dict, shipment: dict) -> None:
    DBOS.logger.info("Simulated notification for %s: %s", order["id"], shipment)


@DBOS.workflow()
def shipping_workflow(order: dict) -> dict:
    DBOS.logger.info("Packing order %s", order["id"])
    record_progress(order, "packing")
    try:
        DBOS.sleep(1 if order["scenario"] == "express" else 4)
        shipment = create_shipping_label(order)
        notify_customer(order, shipment)
        DBOS.set_event("shipment", shipment)
        record_progress(order, "completed")
        return shipment
    except Exception as exc:
        record_failure(order, exc)
        raise
    finally:
        DBOS.close_stream("progress")


@DBOS.workflow()
def order_workflow(order: dict) -> dict:
    DBOS.logger.info("Starting order %s (%s)", order["id"], order["scenario"])
    DBOS.set_event("order", order)
    record_progress(order, "started")
    queue_customer_update(order, "started")
    try:
        order = validate_order(order)
        record_progress(order, "validated")
        inventory = DBOS.start_workflow(inventory_workflow, order)
        payment = DBOS.start_workflow(payment_workflow, order)
        inventory_result = inventory.get_result()
        DBOS.set_event("inventory", inventory_result)
        record_progress(order, "inventory_ready")
        if order["scenario"] == "review":
            record_progress(order, "review_requested")
            DBOS.send(
                payment.get_workflow_id(),
                {"order_id": order["id"], "approved": True},
                topic="payment-review",
            )
        payment_result = payment.get_result()
        DBOS.set_event("payment", payment_result)
        record_progress(order, "payment_ready")
        record_progress(order, "shipping")
        shipment = DBOS.start_workflow(shipping_workflow, order).get_result()
        DBOS.set_event("shipment", shipment)
        result = {
            "order_id": order["id"], "inventory": inventory_result,
            "payment": payment_result, "shipment": shipment,
        }
        record_progress(order, "completed")
        queue_customer_update(order, "completed")
        DBOS.logger.info("Order %s completed", order["id"])
        return result
    except Exception as exc:
        record_failure(order, exc)
        queue_customer_update(order, "failed")
        DBOS.logger.error("Order %s failed: %s", order["id"], exc)
        raise
    finally:
        DBOS.close_stream("progress")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-wait", action="store_true", help="Exit after the batch completes")
    args = parser.parse_args()
    DBOS(
        config={
            "name": "my-dbos-demo",
            "system_database_url": os.environ.get("DATABASE_URL", "sqlite:///maestro-demo.sqlite"),
            "log_level": "INFO",
        },
        # Local UI test only; normal application connections require WSS.
        conductor_url=os.environ.get("MAESTRO_DEMO_WS_URL", "ws://127.0.0.1:8090"),
        conductor_key=os.environ.get("MAESTRO_DEMO_KEY", "gateway"),
    )
    DBOS.launch()
    try:
        batch = uuid4().hex[:8]
        handles = []
        for index, scenario in enumerate(("standard", "express", "review", "declined"), 1):
            order = {"id": f"{batch}-{scenario}", "scenario": scenario, "quantity": index}
            with SetWorkflowID(f"demo-order-{order['id']}"):
                handles.append((scenario, DBOS.start_workflow(order_workflow, order)))
        DBOS.logger.info("Started batch %s: four concurrent orders", batch)
        for scenario, handle in handles:
            try:
                result = handle.get_result()
            except Exception as exc:
                if (
                    scenario != "declined"
                    or not isinstance(exc, ValueError)
                    or "Demo payment declined" not in str(exc)
                ):
                    raise
                DBOS.logger.warning("Expected demo failure: %s (%s)", handle.get_workflow_id(), exc)
            else:
                if scenario == "declined":
                    raise RuntimeError("The declined demo unexpectedly succeeded")
                DBOS.logger.info("Completed %s: %s", handle.get_workflow_id(), result)
        DBOS.logger.info("Batch %s finished: 3 successful orders, 1 intentional failure", batch)
        if not args.no_wait:
            input("Connected to Maestro. Press Enter to stop.\n")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    main()
