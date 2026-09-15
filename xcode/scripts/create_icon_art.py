#!/usr/bin/env python3
"""Generate the app icon assets from the existing swifty Rust artwork."""

from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_IMAGE = ROOT / "assets/swift-rust.jpg"
LIGHT_SOURCE_IMAGE = ROOT / "assets/swift-rust-light.jpg"
APP_ICON_DIR = ROOT / "swiftyapp" / "swiftyapp" / "Assets.xcassets" / "AppIcon.appiconset"
RUST_ORB_PATH = ROOT / "swiftyapp" / "swiftyapp" / "Assets.xcassets" / "RustOrb.imageset" / "RustOrb.png"
RUST_ORB_LIGHT_PATH = ROOT / "swiftyapp" / "swiftyapp" / "Assets.xcassets" / "RustOrbLight.imageset" / "RustOrbLight.png"

CONTENTS_JSON = """{
  "images" : [
    { "idiom" : "iphone", "size" : "20x20", "scale" : "2x", "filename" : "AppIcon-20@2x.png" },
    { "idiom" : "iphone", "size" : "20x20", "scale" : "3x", "filename" : "AppIcon-20@3x.png" },
    { "idiom" : "ipad", "size" : "20x20", "scale" : "1x", "filename" : "AppIcon-20-ipad.png" },
    { "idiom" : "ipad", "size" : "20x20", "scale" : "2x", "filename" : "AppIcon-20@2x-ipad.png" },
    { "idiom" : "iphone", "size" : "29x29", "scale" : "2x", "filename" : "AppIcon-29@2x.png" },
    { "idiom" : "iphone", "size" : "29x29", "scale" : "3x", "filename" : "AppIcon-29@3x.png" },
    { "idiom" : "ipad", "size" : "29x29", "scale" : "1x", "filename" : "AppIcon-29-ipad.png" },
    { "idiom" : "ipad", "size" : "29x29", "scale" : "2x", "filename" : "AppIcon-29@2x-ipad.png" },
    { "idiom" : "iphone", "size" : "40x40", "scale" : "2x", "filename" : "AppIcon-40@2x.png" },
    { "idiom" : "iphone", "size" : "40x40", "scale" : "3x", "filename" : "AppIcon-40@3x.png" },
    { "idiom" : "ipad", "size" : "40x40", "scale" : "1x", "filename" : "AppIcon-40-ipad.png" },
    { "idiom" : "ipad", "size" : "40x40", "scale" : "2x", "filename" : "AppIcon-40@2x-ipad.png" },
    { "idiom" : "ipad", "size" : "76x76", "scale" : "1x", "filename" : "AppIcon-76-ipad.png" },
    { "idiom" : "ipad", "size" : "76x76", "scale" : "2x", "filename" : "AppIcon-76@2x-ipad.png" },
    { "idiom" : "ipad", "size" : "83.5x83.5", "scale" : "2x", "filename" : "AppIcon-83.5@2x-ipad.png" },
    { "idiom" : "iphone", "size" : "60x60", "scale" : "2x", "filename" : "AppIcon-60@2x.png" },
    { "idiom" : "iphone", "size" : "60x60", "scale" : "3x", "filename" : "AppIcon-60@3x.png" },
    { "idiom" : "ios-marketing", "size" : "1024x1024", "scale" : "1x", "filename" : "AppIcon-1024.png" }
  ],
  "info" : {
    "author" : "xcode",
    "version" : 1
  }
}
"""

SIZES = {
    "AppIcon-20@2x.png": 40,
    "AppIcon-20@3x.png": 60,
    "AppIcon-20-ipad.png": 20,
    "AppIcon-20@2x-ipad.png": 40,
    "AppIcon-29@2x.png": 58,
    "AppIcon-29@3x.png": 87,
    "AppIcon-29-ipad.png": 29,
    "AppIcon-29@2x-ipad.png": 58,
    "AppIcon-40@2x.png": 80,
    "AppIcon-40@3x.png": 120,
    "AppIcon-40-ipad.png": 40,
    "AppIcon-40@2x-ipad.png": 80,
    "AppIcon-76-ipad.png": 76,
    "AppIcon-76@2x-ipad.png": 152,
    "AppIcon-83.5@2x-ipad.png": 167,
    "AppIcon-60@2x.png": 120,
    "AppIcon-60@3x.png": 180,
    "AppIcon-1024.png": 1024,
}


def ensure_image(source: Path, output: Path, size: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    command = ["sips", "-s", "format", "png"]
    if size != 1024:
        command += ["-z", str(size), str(size)]
    command += [str(source), "--out", str(output)]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main() -> None:
    if not SOURCE_IMAGE.exists():
        raise FileNotFoundError(f"Missing source artwork: {SOURCE_IMAGE}")
    if not LIGHT_SOURCE_IMAGE.exists():
        raise FileNotFoundError(f"Missing source artwork: {LIGHT_SOURCE_IMAGE}")

    APP_ICON_DIR.mkdir(parents=True, exist_ok=True)
    (APP_ICON_DIR / "Contents.json").write_text(CONTENTS_JSON, encoding="utf-8")

    for filename, size in SIZES.items():
        ensure_image(SOURCE_IMAGE, APP_ICON_DIR / filename, size)

    ensure_image(SOURCE_IMAGE, RUST_ORB_PATH, 1024)
    ensure_image(LIGHT_SOURCE_IMAGE, RUST_ORB_LIGHT_PATH, 1024)


if __name__ == "__main__":
    main()
