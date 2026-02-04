# TODO: README OVERHAUL


# ROS2_quick_image_extractor
Extract `sensor_msgs/msg/Image` and `sensor_msgs/msg/CompressedImage` topics from a ROS 2 bag into JPEG files while mirroring the topic hierarchy in the output directory.

## Requirements
- ROS 2 environment (rclpy, rosbag2_py, rosidl_runtime_py)
- `cv_bridge`
- OpenCV Python bindings (`cv2`)
- PyYAML

Install the Python dependencies via your ROS 2 distribution or with `pip` inside an activated ROS 2 workspace:
```bash
pip install -r requirements.txt
```

## Usage
```bash
python3 extract_ros2_images.py <path/to/bag_directory_or_db3> <output/directory> [--quality 95] [--max-workers 8] [--topics /camera/image_raw ...]
```

- Pass either the bag directory or one of its `.db3` files as the first argument. The script automatically finds `metadata.yaml`.
- All extracted frames are written as JPEGs under the provided output directory, preserving the ROS topic hierarchy (e.g., `/camera/left/image_raw` → `<output>/camera/left/image_raw`).
- Each file is named after the message's nanosecond timestamp (e.g., `1714398730123456789.jpg`) to keep ordering deterministic across topics.
- Use `--topics` to restrict extraction to a subset of image topics.
- `--max-workers` controls how many parallel encoder threads run (defaults to CPU count). Each image topic spins up its own rosbag reader so topics stream concurrently while still preserving per-topic ordering.
- `--quality` sets the JPEG quality factor (0–100).

The script prints the detected image topics before extraction starts and reports completion when all frames are written.

> **Note:** `requirements.txt` pins `numpy<2` to stay compatible with current `cv_bridge` binaries. If you see `_ARRAY_API not found`, reinstall the requirements so NumPy is downgraded, or rebuild `cv_bridge` against NumPy 2.x.
