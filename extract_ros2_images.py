#!/usr/bin/env python3
"""Extract Image/CompressedImage topics from a ROS2 bag into JPEG files."""
from __future__ import annotations

import argparse
import os
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Dict, Iterable, Sequence

import yaml

try:
    import cv2
    import numpy as np
    from cv_bridge import CvBridge
    import rosbag2_py
    from rclpy.serialization import deserialize_message
    from rosidl_runtime_py.utilities import get_message
except ImportError as exc:  # pragma: no cover - handled at runtime
    raise SystemExit(
        "Missing ROS2 dependencies. Make sure cv_bridge, rosbag2_py, and rclpy are installed."
    ) from exc

IMAGE_TYPES = {
    "sensor_msgs/msg/image",
    "sensor_msgs/msg/compressedimage",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract sensor_msgs Image/CompressedImage messages from a ROS2 bag",
    )
    parser.add_argument("bag_db3", type=Path, help="Path to the .db3 file inside the ROS2 bag folder")
    parser.add_argument("output", type=Path, help="Directory where extracted JPEGs will be written")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=os.cpu_count() or 4,
        help="Number of parallel worker threads (defaults to CPU count)",
    )
    parser.add_argument(
        "--quality",
        type=int,
        default=95,
        help="JPEG quality (0-100)",
    )
    parser.add_argument(
        "--topics",
        metavar="TOPIC",
        nargs="*",
        help="Optional subset of topics to extract (default: all image/compressed image topics)",
    )
    return parser.parse_args(argv)


def load_metadata(bag_dir: Path) -> Dict:
    metadata_path = bag_dir / "metadata.yaml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Could not find metadata.yaml in {bag_dir}")
    with metadata_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not data or "rosbag2_bagfile_information" not in data:
        raise RuntimeError(f"metadata.yaml in {bag_dir} is missing rosbag2_bagfile_information")
    return data["rosbag2_bagfile_information"]


def select_image_topics(metadata: Dict, requested_topics: Iterable[str] | None = None) -> Dict[str, Dict]:
    requested_set = {t for t in requested_topics} if requested_topics else None
    result: Dict[str, Dict] = {}
    for entry in metadata.get("topics_with_message_count", []):
        topic_meta = entry.get("topic_metadata", {})
        name = topic_meta.get("name")
        msg_type = topic_meta.get("type", "").lower()
        if not name or msg_type not in IMAGE_TYPES:
            continue
        if requested_set and name not in requested_set:
            continue
        result[name] = {
            "type": topic_meta.get("type"),
            "count": entry.get("message_count", 0),
        }
    if requested_set and not result:
        missing = ", ".join(sorted(requested_set))
        raise ValueError(f"Requested topics were not found or are not image topics: {missing}")
    return result


def ensure_topic_dirs(topics: Iterable[str], output_dir: Path) -> Dict[str, Path]:
    topic_dirs: Dict[str, Path] = {}
    for topic in topics:
        relative = topic.lstrip("/") or "root"
        topic_path = output_dir / relative
        topic_path.mkdir(parents=True, exist_ok=True)
        topic_dirs[topic] = topic_path
    return topic_dirs


def create_reader(bag_dir: Path) -> rosbag2_py.SequentialReader:
    storage_options = rosbag2_py.StorageOptions(uri=str(bag_dir), storage_id="sqlite3")
    converter_options = rosbag2_py.ConverterOptions("", "")
    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)
    return reader


def topic_type_lookup(topics: Dict[str, Dict]) -> Dict[str, str]:
    return {topic: info["type"] for topic, info in topics.items()}


def encode_and_write_image(
    topic: str,
    msg_type: str,
    msg: object,
    destination: Path,
    quality: int,
    bridge: CvBridge,
) -> None:
    if msg_type.lower() == "sensor_msgs/msg/compressedimage":
        # Decode compressed payload (typically JPEG) then re-encode for consistent naming/quality.
        np_buffer = np.frombuffer(msg.data, dtype=np.uint8)
        cv_image = cv2.imdecode(np_buffer, cv2.IMREAD_COLOR)
        if cv_image is None:
            raise RuntimeError(f"Failed to decode compressed image from topic {topic}")
    else:
        cv_image = bridge.imgmsg_to_cv2(msg, desired_encoding="passthrough")
    success, encoded = cv2.imencode(".jpg", cv_image, [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)])
    if not success:
        raise RuntimeError(f"OpenCV failed to encode JPEG for topic {topic}")
    destination.write_bytes(encoded.tobytes())


def extract_images(
    reader: rosbag2_py.SequentialReader,
    topics: Dict[str, Dict],
    output_dirs: Dict[str, Path],
    quality: int,
    max_workers: int,
) -> None:
    bridge = CvBridge()
    counters = defaultdict(int)
    type_lookup = topic_type_lookup(topics)
    msg_class_cache: Dict[str, type] = {}

    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while reader.has_next():
            topic, data, timestamp = reader.read_next()
            if topic not in topics:
                continue
            msg_type = type_lookup[topic]
            msg_cls_key = msg_type
            if msg_cls_key not in msg_class_cache:
                msg_class_cache[msg_cls_key] = get_message(msg_type)
            msg = deserialize_message(data, msg_class_cache[msg_cls_key])

            relative_idx = counters[topic]
            counters[topic] += 1
            timestamp_ms = timestamp // 1_000_000
            filename = f"{relative_idx:06d}_{timestamp_ms:013d}.jpg"
            destination = output_dirs[topic] / filename

            future = executor.submit(
                encode_and_write_image,
                topic,
                msg_type,
                msg,
                destination,
                quality,
                bridge,
            )
            pending.add(future)
            if len(pending) >= max_workers * 4:
                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                for job in completed:
                    job.result()
        if pending:
            completed, _ = wait(pending)
            for job in completed:
                job.result()


def list_topics(topics: Dict[str, Dict]) -> None:
    print("Found image topics:")
    for name in sorted(topics):
        info = topics[name]
        print(f"  - {name} ({info['type']}, {info['count']} messages)")


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.quality < 0 or args.quality > 100:
        raise SystemExit("--quality must be between 0 and 100")

    bag_db3 = args.bag_db3.resolve()
    if not bag_db3.exists():
        raise SystemExit(f"Bag file {bag_db3} does not exist")
    bag_dir = bag_db3.parent

    metadata = load_metadata(bag_dir)
    topics = select_image_topics(metadata, args.topics)
    if not topics:
        raise SystemExit("No sensor_msgs Image or CompressedImage topics found in the bag")

    output_root = args.output.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    output_dirs = ensure_topic_dirs(topics.keys(), output_root)

    list_topics(topics)

    reader = create_reader(bag_dir)
    reader.set_filter(
        rosbag2_py.StorageFilter(topics=list(topics.keys())),
    )

    extract_images(reader, topics, output_dirs, args.quality, args.max_workers)
    print(f"Extraction complete. Images written under {output_root}")


if __name__ == "__main__":
    main()
