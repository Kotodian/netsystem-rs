#!/usr/bin/env bash
# Build an xcframework + Swift bindings for the hammer crate.
#
# Output layout:
#   build/Hammer.xcframework
#   build/Sources/Hammer/hammer.swift
#
# Required toolchain:
#   - Xcode 15+
#   - Rust targets: aarch64-apple-ios, aarch64-apple-ios-sim, x86_64-apple-ios,
#                   aarch64-apple-darwin, x86_64-apple-darwin
#     Install with `rustup target add ...`.

set -euo pipefail

cd "$(dirname "$0")/.."

CRATE_NAME="hammer"
LIB_NAME="libhammer.a"
BUILD_DIR="ios-demo/build"
GENERATED_DIR="${BUILD_DIR}/generated"
XCFRAMEWORK="${BUILD_DIR}/Hammer.xcframework"

rm -rf "${BUILD_DIR}"
mkdir -p "${GENERATED_DIR}"

echo "==> cargo build (release) for all Apple targets"
cargo build --release --target aarch64-apple-ios
cargo build --release --target aarch64-apple-ios-sim
cargo build --release --target x86_64-apple-ios
cargo build --release --target aarch64-apple-darwin
cargo build --release --target x86_64-apple-darwin

echo "==> lipo simulator + macOS static libs"
mkdir -p "${BUILD_DIR}/sim" "${BUILD_DIR}/macos"
lipo -create \
  "target/aarch64-apple-ios-sim/release/${LIB_NAME}" \
  "target/x86_64-apple-ios/release/${LIB_NAME}" \
  -output "${BUILD_DIR}/sim/${LIB_NAME}"
lipo -create \
  "target/aarch64-apple-darwin/release/${LIB_NAME}" \
  "target/x86_64-apple-darwin/release/${LIB_NAME}" \
  -output "${BUILD_DIR}/macos/${LIB_NAME}"

echo "==> generate Swift bindings via uniffi-bindgen"
cargo run --release --bin uniffi-bindgen -- generate src/hammer.udl \
  --language swift --out-dir "${GENERATED_DIR}"

# uniffi emits hammer.swift, hammerFFI.h and a module map; rename to satisfy XCFramework requirements
HEADERS_DIR="${BUILD_DIR}/headers"
mkdir -p "${HEADERS_DIR}"
mv "${GENERATED_DIR}/hammerFFI.h" "${HEADERS_DIR}/hammerFFI.h"
mv "${GENERATED_DIR}/hammerFFI.modulemap" "${HEADERS_DIR}/module.modulemap"

echo "==> assemble xcframework"
xcodebuild -create-xcframework \
  -library "target/aarch64-apple-ios/release/${LIB_NAME}" -headers "${HEADERS_DIR}" \
  -library "${BUILD_DIR}/sim/${LIB_NAME}" -headers "${HEADERS_DIR}" \
  -library "${BUILD_DIR}/macos/${LIB_NAME}" -headers "${HEADERS_DIR}" \
  -output "${XCFRAMEWORK}"

echo "==> publish Swift glue"
mkdir -p "${BUILD_DIR}/Sources/Hammer"
mv "${GENERATED_DIR}/hammer.swift" "${BUILD_DIR}/Sources/Hammer/hammer.swift"

echo
echo "Done."
echo "  XCFramework: ${XCFRAMEWORK}"
echo "  Swift glue:  ${BUILD_DIR}/Sources/Hammer/hammer.swift"
