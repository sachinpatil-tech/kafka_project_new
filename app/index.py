"""
================================================================
 index.py — CLI entrypoint
================================================================
 Routes between:
   --mode kafka       Kafka (Debezium CDC) → Bronze
   --mode transform   Bronze → Silver → Gold
   --mode trino       Run Trino analytics on Lakehouse

 Usage:
   python -m app.index --mode kafka
   python -m app.index --mode kafka --topic pgb.public.users
   python -m app.index --mode kafka --consume-mode streaming
   python -m app.index --mode transform --layer silver
   python -m app.index --mode transform --layer gold
   python -m app.index --mode transform --layer all
   python -m app.index --mode trino
   python -m app.index --mode trino --section silver
================================================================
"""
import argparse
import sys

from app import config
from app.utils import get_logger

log = get_logger("index")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse",
        description="Lakehouse — Debezium CDC → Bronze/Silver/Gold + Trino",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  --mode kafka                                     # CDC → Bronze (batch)
  --mode kafka --topic pgb.public.users            # specific topic
  --mode kafka --consume-mode streaming            # continuous stream
  --mode transform --layer silver                  # Bronze → Silver
  --mode transform --layer gold                    # Silver → Gold
  --mode transform --layer all                     # both
  --mode trino                                     # all Trino sections
  --mode trino --section silver                    # one section
        """
    )

    parser.add_argument("--mode", required=True,
                        choices=["kafka", "transform", "trino"],
                        help="Pipeline mode")

    # Kafka options
    parser.add_argument("--topic", default=None,
                        help="[kafka] Specific Kafka topic")
    parser.add_argument("--consume-mode", default="batch",
                        choices=["batch", "streaming"],
                        help="[kafka] batch = read-all-stop; streaming = continuous")
    parser.add_argument("--checkpoint-dir",
                        default="/tmp/kafka-iceberg-checkpoints",
                        help="[kafka] Checkpoint dir for streaming")

    # Transform options
    parser.add_argument("--layer", default="all",
                        choices=["silver", "gold", "all"],
                        help="[transform] Which lakehouse layer to build")

    # Trino options
    parser.add_argument("--section", default=None,
                        choices=["counts", "bronze", "silver", "gold"],
                        help="[trino] Single section to run")

    parser.add_argument("--show-config", action="store_true",
                        help="Print resolved configuration and exit")

    args = parser.parse_args()

    if args.show_config:
        config.print_config()
        return 0

    config.print_config()

    if args.mode == "kafka":
        from app import kafka_consumer
        return kafka_consumer.run(
            mode=args.consume_mode,
            topic=args.topic,
            checkpoint_dir=args.checkpoint_dir,
        )

    elif args.mode == "transform":
        from app import transformations
        return transformations.run(layer=args.layer)

    elif args.mode == "trino":
        from app import trino_client
        return trino_client.run(section=args.section)

    else:
        log.error("Unknown mode: %s", args.mode)
        return 1


if __name__ == "__main__":
    sys.exit(main())
