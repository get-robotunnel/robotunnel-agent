#!/usr/bin/env python3
"""Session-local ROS2 projection worker for visual_debug."""

import argparse
import copy
import io
import math
import struct
import subprocess
import sys
import time
from typing import List, Optional, Tuple

import rclpy
from rclpy.node import Node
from rosidl_runtime_py.utilities import get_message
from sensor_msgs.msg import CompressedImage, Image, LaserScan, PointCloud2, PointField

try:
    from PIL import Image as PILImage
except Exception:  # pylint: disable=broad-except
    PILImage = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RoboTunnel projection worker")
    parser.add_argument("--source-topic", required=True)
    parser.add_argument("--projected-topic", required=True)
    parser.add_argument("--hz-limit", type=float, default=0.0)
    parser.add_argument("--point-stride", type=int, default=1)
    parser.add_argument("--voxel-leaf-m", type=float, default=0.0)
    parser.add_argument("--image-scale", type=float, default=1.0)
    parser.add_argument("--encode", default="")
    parser.add_argument("--quality", type=int, default=75)
    return parser.parse_args()


def read_topic_type(topic: str) -> str:
    cmd = ["ros2", "topic", "type", topic]
    out = subprocess.check_output(cmd, text=True, timeout=5)
    value = out.strip()
    if not value:
        raise RuntimeError(f"empty topic type for {topic}")
    return value


def image_channels(encoding: str) -> int:
    encoding = (encoding or "").strip().lower()
    mapping = {
        "mono8": 1,
        "8uc1": 1,
        "rgb8": 3,
        "bgr8": 3,
        "8uc3": 3,
        "rgba8": 4,
        "bgra8": 4,
        "8uc4": 4,
    }
    return mapping.get(encoding, 0)


def xyz_offsets(fields: List[PointField]) -> Optional[Tuple[int, int, int]]:
    offsets = {}
    for field in fields:
        name = field.name.strip().lower()
        if name not in ("x", "y", "z"):
            continue
        if field.datatype != PointField.FLOAT32:
            return None
        offsets[name] = int(field.offset)
    if "x" in offsets and "y" in offsets and "z" in offsets:
        return (offsets["x"], offsets["y"], offsets["z"])
    return None


class ProjectionWorker(Node):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__("rt_projection_worker")
        self.args = args
        self.source_topic = args.source_topic
        self.projected_topic = args.projected_topic
        self.hz_limit = max(0.0, float(args.hz_limit))
        self.point_stride = max(1, int(args.point_stride))
        self.voxel_leaf_m = max(0.0, float(args.voxel_leaf_m))
        self.image_scale = min(1.0, max(0.05, float(args.image_scale)))
        self.encode = (args.encode or "").strip().lower()
        self.quality = max(1, min(100, int(args.quality)))
        self.min_publish_gap = (1.0 / self.hz_limit) if self.hz_limit > 0.0 else 0.0
        self.last_publish_mono = 0.0

        self.topic_type = read_topic_type(self.source_topic)
        self.msg_cls = get_message(self.topic_type)
        self.can_process_compressed_image = (
            self.topic_type == "sensor_msgs/msg/CompressedImage"
            and PILImage is not None
            and (self.image_scale < 0.999 or self.encode in ("jpeg", "jpg"))
        )
        self.publish_compressed_jpeg = (
            self.topic_type == "sensor_msgs/msg/Image"
            and self.encode == "jpeg"
            and PILImage is not None
        )
        if self.publish_compressed_jpeg or self.can_process_compressed_image:
            self.publisher = self.create_publisher(CompressedImage, self.projected_topic, 10)
            self.output_topic_type = "sensor_msgs/msg/CompressedImage"
        else:
            self.publisher = self.create_publisher(self.msg_cls, self.projected_topic, 10)
            self.output_topic_type = self.topic_type
        self.subscription = self.create_subscription(self.msg_cls, self.source_topic, self._on_msg, 10)

        self.get_logger().info(
            "projection worker started: type=%s output=%s source=%s projected=%s hz=%.3f stride=%d voxel=%.3f scale=%.3f encode=%s quality=%d"
            % (
                self.topic_type,
                self.output_topic_type,
                self.source_topic,
                self.projected_topic,
                self.hz_limit,
                self.point_stride,
                self.voxel_leaf_m,
                self.image_scale,
                self.encode or "none",
                self.quality,
            )
        )

    def _rate_limited(self) -> bool:
        if self.min_publish_gap <= 0.0:
            return False
        now = time.monotonic()
        if now - self.last_publish_mono < self.min_publish_gap:
            return True
        self.last_publish_mono = now
        return False

    def _on_msg(self, msg) -> None:
        if self._rate_limited():
            return

        out = msg
        try:
            if self.topic_type == "sensor_msgs/msg/PointCloud2":
                out = self._transform_point_cloud(msg)
            elif self.topic_type == "sensor_msgs/msg/LaserScan":
                out = self._transform_laserscan(msg)
            elif self.topic_type == "sensor_msgs/msg/Image":
                image_out = self._transform_image(msg)
                if self.publish_compressed_jpeg:
                    encoded = self._encode_image_jpeg(image_out)
                    if encoded is None:
                        return
                    out = encoded
                else:
                    out = image_out
            elif self.can_process_compressed_image:
                encoded = self._transform_compressed_image(msg)
                if encoded is not None:
                    out = encoded
        except Exception as exc:  # pylint: disable=broad-except
            self.get_logger().warn(f"transform failed, publishing original message: {exc}")
            out = msg

        self.publisher.publish(out)

    def _transform_point_cloud(self, msg: PointCloud2) -> PointCloud2:
        if self.point_stride <= 1 and self.voxel_leaf_m <= 0.0:
            return msg
        if msg.point_step <= 0:
            return msg

        total_points = int(msg.width) * int(msg.height)
        if total_points <= 0:
            return msg

        indices = list(range(0, total_points, self.point_stride))
        raw = bytes(msg.data)
        point_step = int(msg.point_step)

        if self.voxel_leaf_m > 0.0:
            xyz = xyz_offsets(list(msg.fields))
            if xyz is not None:
                xoff, yoff, zoff = xyz
                keep = []
                seen = set()
                inv = 1.0 / self.voxel_leaf_m
                for idx in indices:
                    base = idx * point_step
                    if base + point_step > len(raw):
                        break
                    x = struct.unpack_from("<f", raw, base + xoff)[0]
                    y = struct.unpack_from("<f", raw, base + yoff)[0]
                    z = struct.unpack_from("<f", raw, base + zoff)[0]
                    if not (math.isfinite(x) and math.isfinite(y) and math.isfinite(z)):
                        continue
                    key = (math.floor(x * inv), math.floor(y * inv), math.floor(z * inv))
                    if key in seen:
                        continue
                    seen.add(key)
                    keep.append(idx)
                indices = keep

        if not indices:
            return msg

        out_data = bytearray()
        for idx in indices:
            base = idx * point_step
            end = base + point_step
            if end > len(raw):
                break
            out_data.extend(raw[base:end])

        out = PointCloud2()
        out.header = msg.header
        out.height = 1
        out.width = int(len(out_data) / point_step)
        out.fields = msg.fields
        out.is_bigendian = msg.is_bigendian
        out.point_step = msg.point_step
        out.row_step = out.width * out.point_step
        out.is_dense = msg.is_dense
        out.data = bytes(out_data)
        return out

    def _transform_image(self, msg: Image) -> Image:
        if self.image_scale >= 0.999:
            return msg
        if msg.width <= 0 or msg.height <= 0:
            return msg

        channels = image_channels(msg.encoding)
        if channels <= 0:
            return msg

        src_w = int(msg.width)
        src_h = int(msg.height)
        src_step = int(msg.step) if int(msg.step) > 0 else src_w * channels
        raw = bytes(msg.data)
        if len(raw) < src_h * src_step:
            return msg

        dst_w = max(1, int(round(src_w * self.image_scale)))
        dst_h = max(1, int(round(src_h * self.image_scale)))
        dst_step = dst_w * channels

        x_map = [min(src_w - 1, int(x / self.image_scale)) for x in range(dst_w)]
        y_map = [min(src_h - 1, int(y / self.image_scale)) for y in range(dst_h)]

        out_data = bytearray(dst_h * dst_step)
        write = 0
        for src_y in y_map:
            row_start = src_y * src_step
            row = raw[row_start : row_start + src_step]
            for src_x in x_map:
                px = src_x * channels
                out_data[write : write + channels] = row[px : px + channels]
                write += channels

        out = copy.copy(msg)
        out.width = dst_w
        out.height = dst_h
        out.step = dst_step
        out.data = bytes(out_data)
        return out

    def _transform_laserscan(self, msg: LaserScan) -> LaserScan:
        if self.point_stride <= 1:
            return msg

        step = max(1, self.point_stride)
        ranges = list(msg.ranges)[::step]
        intensities = list(msg.intensities)[::step] if msg.intensities else []
        if not ranges:
            return msg

        out = LaserScan()
        out.header = msg.header
        out.angle_min = msg.angle_min
        out.angle_max = msg.angle_max
        out.angle_increment = msg.angle_increment * float(step)
        out.time_increment = msg.time_increment * float(step)
        out.scan_time = msg.scan_time
        out.range_min = msg.range_min
        out.range_max = msg.range_max
        out.ranges = ranges
        out.intensities = intensities
        return out

    def _encode_image_jpeg(self, msg: Image) -> Optional[CompressedImage]:
        if PILImage is None:
            return None
        channels = image_channels(msg.encoding)
        if channels <= 0:
            return None

        src_w = int(msg.width)
        src_h = int(msg.height)
        if src_w <= 0 or src_h <= 0:
            return None
        src_step = int(msg.step) if int(msg.step) > 0 else src_w * channels
        raw = bytes(msg.data)
        if len(raw) < src_h * src_step:
            return None

        row_bytes = src_w * channels
        if src_step == row_bytes:
            contiguous = raw[: src_h * src_step]
        else:
            contiguous = bytearray(src_h * row_bytes)
            write = 0
            for y in range(src_h):
                start = y * src_step
                contiguous[write : write + row_bytes] = raw[start : start + row_bytes]
                write += row_bytes
            contiguous = bytes(contiguous)

        encoding = (msg.encoding or "").strip().lower()
        if encoding in ("mono8", "8uc1"):
            pil_img = PILImage.frombytes("L", (src_w, src_h), contiguous)
        elif encoding in ("rgb8", "8uc3"):
            pil_img = PILImage.frombytes("RGB", (src_w, src_h), contiguous)
        elif encoding == "bgr8":
            pil_img = PILImage.frombytes("RGB", (src_w, src_h), contiguous, "raw", "BGR")
        elif encoding in ("rgba8", "8uc4"):
            pil_img = PILImage.frombytes("RGBA", (src_w, src_h), contiguous)
        elif encoding == "bgra8":
            pil_img = PILImage.frombytes("RGBA", (src_w, src_h), contiguous, "raw", "BGRA")
        else:
            return None

        if pil_img.mode != "RGB":
            pil_img = pil_img.convert("RGB")

        buffer = io.BytesIO()
        pil_img.save(buffer, format="JPEG", quality=self.quality, optimize=True)

        out = CompressedImage()
        out.header = msg.header
        out.format = "jpeg"
        out.data = buffer.getvalue()
        return out

    def _transform_compressed_image(self, msg: CompressedImage) -> Optional[CompressedImage]:
        if PILImage is None:
            return None

        try:
            pil_img = PILImage.open(io.BytesIO(bytes(msg.data)))
            pil_img.load()
        except Exception:  # pylint: disable=broad-except
            return None

        if self.image_scale < 0.999:
            src_w, src_h = pil_img.size
            dst_w = max(1, int(round(src_w * self.image_scale)))
            dst_h = max(1, int(round(src_h * self.image_scale)))
            resample = getattr(getattr(PILImage, "Resampling", PILImage), "BILINEAR")
            pil_img = pil_img.resize((dst_w, dst_h), resample)

        if pil_img.mode != "RGB":
            pil_img = pil_img.convert("RGB")

        buffer = io.BytesIO()
        pil_img.save(buffer, format="JPEG", quality=self.quality, optimize=True)

        out = CompressedImage()
        out.header = msg.header
        out.format = "jpeg"
        out.data = buffer.getvalue()
        return out


def main() -> int:
    args = parse_args()
    rclpy.init()
    node = None
    try:
        node = ProjectionWorker(args)
        rclpy.spin(node)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        print(f"projection worker failed: {exc}", file=sys.stderr)
        return 2
    finally:
        if node is not None:
            node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    sys.exit(main())
