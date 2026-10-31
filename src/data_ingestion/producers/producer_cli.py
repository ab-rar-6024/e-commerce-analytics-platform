"""Command-line interface for running data producers."""

import argparse
import logging
import sys

from .transaction_producer import TransactionProducer


def setup_logging(level: str = "INFO"):
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


def run_transaction_producer(args: argparse.Namespace):
    """Run the transaction producer."""
    producer_config = {}
    if args.config:
        # Parse additional config from command line
        for config_item in args.config:
            key, value = config_item.split("=", 1)
            producer_config[key] = value

    producer = TransactionProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        producer_config=producer_config,
        generation_rate=args.rate,
    )

    try:
        producer.run_continuous(args.duration)
    except Exception as e:
        logging.error(f"Producer failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="E-commerce Data Producer CLI")

    # Global arguments
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Transaction producer
    transaction_parser = subparsers.add_parser(
        "transaction",
        help="Run transaction data producer",
    )
    transaction_parser.add_argument(
        "--topic",
        default="transactions",
        help="Kafka topic name (default: transactions)",
    )
    transaction_parser.add_argument(
        "--rate",
        type=float,
        default=1000.0,
        help="Generation rate in transactions per hour (default: 1000)",
    )
    transaction_parser.add_argument(
        "--duration",
        type=int,
        help="Duration to run in seconds (default: infinite)",
    )
    transaction_parser.add_argument(
        "--config",
        action="append",
        help="Additional producer config in key=value format",
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging(level=args.log_level)

    # Run the appropriate command
    if args.command == "transaction":
        run_transaction_producer(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
